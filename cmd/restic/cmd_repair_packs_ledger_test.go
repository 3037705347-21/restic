package main

import (
	"context"
	"strings"
	"testing"

	"github.com/restic/restic/internal/checker"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/ledger"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
)

// TestRepairPacksFromLedgerValidation covers TR-7.1: flag gating and mutual
// exclusion fail before touching the repository.
func TestRepairPacksFromLedgerValidation(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)

	// feature flag disabled
	_, _, err := testRunRepairPacksOpts(t, env.gopts,
		RepairPacksOptions{FromLedger: true, LedgerDir: t.TempDir()}, nil)
	rtest.Assert(t, err != nil, "feature flag must be required")

	enableLedgerFlag(t)

	// mutually exclusive with explicit IDs
	_, _, err = testRunRepairPacksOpts(t, env.gopts,
		RepairPacksOptions{FromLedger: true}, []string{"0123abcd"})
	rtest.Assert(t, err != nil, "from-ledger must reject explicit IDs")

	// --ledger-dir without --from-ledger
	_, _, err = testRunRepairPacksOpts(t, env.gopts,
		RepairPacksOptions{LedgerDir: t.TempDir()}, nil)
	rtest.Assert(t, err != nil, "ledger-dir without from-ledger must be rejected")

	// no persistent cache and no explicit dir
	noCache := env.gopts
	noCache.NoCache = true
	_, _, err = testRunRepairPacksOpts(t, noCache, RepairPacksOptions{FromLedger: true}, nil)
	rtest.Assert(t, err != nil, "--no-cache without --ledger-dir must be rejected")
}

// TestRepairPacksFromLedgerEmpty covers TR-7.2: an empty ledger is a no-op.
func TestRepairPacksFromLedgerEmpty(t *testing.T) {
	enableLedgerFlag(t)
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, env.testdata+"/0", []string{"0/9"}, BackupOptions{}, env.gopts)

	out, outErr, err := testRunRepairPacksOpts(t, env.gopts,
		RepairPacksOptions{FromLedger: true, LedgerDir: t.TempDir()}, nil)
	rtest.OK(t, err)
	rtest.Assert(t, strings.Contains(out+outErr, "no quarantined packs"), "expected empty-ledger message, got stdout %q stderr %q", out, outErr)
}

// TestRepairPacksFromLedgerOnePack covers TR-7.3: a quarantined pack is
// repaired through the command and the ledger entry is consumed.
func TestRepairPacksFromLedgerOnePack(t *testing.T) {
	enableLedgerFlag(t)
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, env.testdata, []string{"0"}, BackupOptions{}, env.gopts)

	victim, handles := findDataPackWithBlobs(t, env.gopts)

	ledgerDir := t.TempDir()
	store, err := ledger.Open(ledgerDir)
	rtest.OK(t, err)

	// seed the record exactly the way check would: current fingerprint plus the
	// blob handles in the damaged pack
	rtest.OK(t, withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(false, gopts.Verbosity, gopts.Term)
		_, repo, unlock, err := openWithReadLock(ctx, gopts, false, printer)
		rtest.OK(t, err)
		defer unlock()

		fps, err := checker.New(repo, false).PackFingerprints(ctx)
		rtest.OK(t, err)
		fp := fps[victim]
		_, err = store.RegisterQuarantine(victim,
			ledger.Fingerprint{BlobHash: fp.BlobHash, Size: fp.Size},
			ledger.ReasonDataVerification, handles)
		return err
	}))
	rtest.OK(t, store.Close())

	out, outErr, err := testRunRepairPacksOpts(t, env.gopts,
		RepairPacksOptions{FromLedger: true, LedgerDir: ledgerDir}, nil)
	rtest.OK(t, err)
	rtest.Assert(t, strings.Contains(out+outErr, "1 pack(s) repaired, 0 remaining"),
		"expected completion message, got stdout %q stderr %q", out, outErr)

	// second invocation finds an empty ledger and does nothing
	out2, out2Err, err := testRunRepairPacksOpts(t, env.gopts,
		RepairPacksOptions{FromLedger: true, LedgerDir: ledgerDir}, nil)
	rtest.OK(t, err)
	rtest.Assert(t, strings.Contains(out2+out2Err, "no quarantined packs"), "expected empty ledger, got stdout %q stderr %q", out2, out2Err)

	// the structural check still passes afterwards
	testRunCheck(t, env.gopts)
}
