package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/ledger"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
)

func enableLedgerFlag(t *testing.T) {
	t.Helper()
	undo := feature.TestSetFlag(t, feature.Flag, feature.VerificationLedger, true)
	t.Cleanup(undo)
}

func runCheckResume(t testing.TB, gopts global.Options, ledgerDir string) (checkSummary, error) {
	t.Helper()
	var summary checkSummary
	_, err := withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		opts := CheckOptions{ReadData: true, Resume: true, LedgerDir: ledgerDir}
		var e error
		summary, e = runCheck(ctx, opts, gopts, nil, gopts.Term)
		return e
	})
	return summary, err
}

// findDataPack returns a pack file that contains at least one data blob.
func findDataPack(t testing.TB, gopts global.Options) restic.ID {
	id, _ := findDataPackWithBlobs(t, gopts)
	return id
}

// findDataPackWithBlobs returns a data pack and the handles of all its blobs.
func findDataPackWithBlobs(t testing.TB, gopts global.Options) (restic.ID, []restic.BlobHandle) {
	t.Helper()
	var packID restic.ID
	var handles restic.BlobSet
	rtest.OK(t, withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(false, gopts.Verbosity, gopts.Term)
		_, repo, unlock, err := openWithReadLock(ctx, gopts, false, printer)
		rtest.OK(t, err)
		defer unlock()
		rtest.OK(t, repo.LoadIndex(ctx, printer))
		return repo.ListBlobs(ctx, func(blob restic.PackBlob) {
			if blob.Handle().Type == restic.DataBlob {
				if packID == (restic.ID{}) {
					packID = blob.PackID()
				}
				if blob.PackID().Equal(packID) {
					if handles == nil {
						handles = restic.NewBlobSet()
					}
					handles.Insert(blob.Handle())
				}
			}
		})
	}))
	rtest.Assert(t, !packID.IsNull(), "expected a data pack")
	return packID, handles.List()
}

func packPath(gopts global.Options, id restic.ID) string {
	name := id.String()
	return filepath.Join(gopts.Repo, "data", name[0:2], name)
}

// damagePackByte flips a byte in the middle of a pack, corrupting one of its
// data blobs while the pack header remains intact.
func damagePackByte(t testing.TB, gopts global.Options, id restic.ID) {
	t.Helper()
	filename := packPath(gopts, id)
	data, err := os.ReadFile(filename)
	rtest.OK(t, err)
	rtest.Assert(t, len(data) > 1024, "pack too small to damage safely: %d", len(data))
	data[len(data)/2] ^= 0xff
	// the local backend stores packs read-only, make it writable for the edit
	rtest.OK(t, os.Chmod(filename, 0o600))
	rtest.OK(t, os.WriteFile(filename, data, 0o444))
}

func openTestLedger(t testing.TB, dir string) *ledger.Store {
	t.Helper()
	store, err := ledger.Open(dir)
	rtest.OK(t, err)
	return store
}

// TR-5.1: the second resume run reads zero packs.
func TestCheckResumeSkipsVerifiedPacks(t *testing.T) {
	enableLedgerFlag(t)
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, env.testdata+"/0", []string{"0/9"}, BackupOptions{}, env.gopts)

	ledgerDir := t.TempDir()
	first, err := runCheckResume(t, env.gopts, ledgerDir)
	rtest.OK(t, err)
	rtest.Assert(t, first.PacksRead > 0, "first run must read packs, got summary %+v", first)
	rtest.Equals(t, int64(0), first.PacksSkipped)

	second, err := runCheckResume(t, env.gopts, ledgerDir)
	rtest.OK(t, err)
	rtest.Equals(t, int64(0), second.PacksRead)
	rtest.Equals(t, first.PacksRead, second.PacksSkipped)
	rtest.Equals(t, 0, second.QuarantinedPacks)
	rtest.Equals(t, ledgerDir, second.LedgerPath)
}

// TR-5.2: a damaged pack encountered during a resume run is registered exactly
// once with its salvageable blobs, and later runs skip it as quarantined.
func TestCheckResumeRegistersDamagedPack(t *testing.T) {
	enableLedgerFlag(t)
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, env.testdata, []string{"0"}, BackupOptions{}, env.gopts)

	// damage a pack before the first ever resume verification, so the pack is
	// pending and actually read (an already verified pack with unchanged index
	// entries is intentionally skipped, like every cached verification result)
	victim := findDataPack(t, env.gopts)
	damagePackByte(t, env.gopts, victim)

	ledgerDir := t.TempDir()
	first, err := runCheckResume(t, env.gopts, ledgerDir)
	rtest.Assert(t, err != nil, "check must fail on a damaged pack")
	rtest.Equals(t, 1, first.QuarantinedPacks)

	store := openTestLedger(t, ledgerDir)
	rec, ok := store.Get(victim)
	rtest.Assert(t, ok, "ledger must contain the damaged pack")
	rtest.Equals(t, ledger.StageRegistered, rec.Stage)
	rtest.Assert(t, len(rec.Salvageable) > 0, "expected salvageable blobs, got %d", len(rec.Salvageable))
	rtest.Assert(t, rec.BlobHash != restic.ID{}, "fingerprint must be recorded")
	recordsAfterFirst := store.Len()
	// release the lock so the second check run can open the ledger
	rtest.OK(t, store.Close())

	// a further run must not duplicate the record: the pack is now skipped as
	// quarantined and check keeps failing with a single quarantine entry
	second, err := runCheckResume(t, env.gopts, ledgerDir)
	rtest.Assert(t, err != nil, "check keeps failing while the pack is quarantined")
	rtest.Equals(t, 1, second.QuarantinedPacks)
	rtest.Equals(t, int64(0), second.PacksRead)

	reopened := openTestLedger(t, ledgerDir)
	rtest.Equals(t, recordsAfterFirst, reopened.Len(), "quarantine record count must stay stable")
}

// TR-5.3/TR-5.4-style: a missing pack is registered with the missing reason.
func TestCheckResumeRegistersMissingPack(t *testing.T) {
	enableLedgerFlag(t)
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, env.testdata+"/0", []string{"0/9"}, BackupOptions{}, env.gopts)

	ledgerDir := t.TempDir()
	_, err := runCheckResume(t, env.gopts, ledgerDir)
	rtest.OK(t, err)

	victim := findDataPack(t, env.gopts)
	rtest.OK(t, os.Remove(packPath(env.gopts, victim)))

	_, err = runCheckResume(t, env.gopts, ledgerDir)
	rtest.Assert(t, err != nil, "check must fail when a pack is missing")

	store := openTestLedger(t, ledgerDir)
	rec, ok := store.Get(victim)
	rtest.Assert(t, ok, "missing pack must be registered")
	rtest.Equals(t, ledger.StageRegistered, rec.Stage)
	rtest.Equals(t, ledger.ReasonMissingPack, rec.Reason)
	rtest.OK(t, store.Close())
}

// TR-5.4: a ledger that cannot be written fails the run and never advances
// beyond what was previously committed.
func TestCheckResumeWriteFailure(t *testing.T) {
	enableLedgerFlag(t)
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, env.testdata+"/0", []string{"0/9"}, BackupOptions{}, env.gopts)

	ledgerDir := t.TempDir()
	first, err := runCheckResume(t, env.gopts, ledgerDir)
	rtest.OK(t, err)
	committed := first.PacksRead

	// make the directory tree and the existing segment files read-only so the
	// next append cannot succeed
	rtest.OK(t, filepath.Walk(ledgerDir, func(path string, info os.FileInfo, err error) error {
		rtest.OK(t, err)
		if info.IsDir() {
			return os.Chmod(path, 0o500)
		}
		return os.Chmod(path, 0o400)
	}))
	t.Cleanup(func() {
		_ = filepath.Walk(ledgerDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if info.IsDir() {
				_ = os.Chmod(path, 0o700)
			} else {
				_ = os.Chmod(path, 0o600)
			}
			return nil
		})
	})

	_, err = runCheckResume(t, env.gopts, ledgerDir)
	rtest.Assert(t, err != nil, "read-only ledger must fail the run")

	rtest.OK(t, filepath.Walk(ledgerDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			_ = os.Chmod(path, 0o700)
		} else {
			_ = os.Chmod(path, 0o600)
		}
		return nil
	}))
	store := openTestLedger(t, ledgerDir)
	// the failed run must not have verified additional packs
	rtest.Assert(t, int64(store.Len()) <= committed,
		"no records beyond the committed batch expected, got %d > %d", store.Len(), committed)
	rtest.OK(t, store.Close())
}

// TR-5.5: flag and path validation.
func TestCheckResumeFlagValidation(t *testing.T) {
	undo := feature.TestSetFlag(t, feature.Flag, feature.VerificationLedger, false)
	defer undo()

	err := checkFlags(CheckOptions{Resume: true, ReadData: true})
	rtest.Assert(t, err != nil, "flag disabled must be rejected")

	enableLedgerFlag(t)
	err = checkFlags(CheckOptions{Resume: true})
	rtest.Assert(t, err != nil, "--resume without --read-data must be rejected")

	err = checkFlags(CheckOptions{Resume: true, ReadDataSubset: "1/2"})
	rtest.OK(t, err)

	_, err = resolveLedgerDir("", "", "repo-id")
	rtest.Assert(t, err != nil, "missing cache and --ledger-dir must be rejected")
	dir, err := resolveLedgerDir("/tmp/x", "/ignored", "repo-id")
	rtest.OK(t, err)
	rtest.Equals(t, "/tmp/x", dir)
}

// TR-5.6: JSON summary fields and no ledger artifacts without --resume.
func TestCheckResumeSummaryAndOptOut(t *testing.T) {
	enableLedgerFlag(t)
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, env.testdata+"/0", []string{"0/9"}, BackupOptions{}, env.gopts)

	wouldBeDir := filepath.Join(t.TempDir(), "verify-ledger")
	gopts := env.gopts
	gopts.JSON = true
	var summary checkSummary
	_, err := withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		// plain run without --resume must not touch any ledger directory
		opts := CheckOptions{ReadData: true, LedgerDir: wouldBeDir}
		var e error
		summary, e = runCheck(ctx, opts, gopts, nil, gopts.Term)
		return e
	})
	rtest.OK(t, err)
	rtest.Equals(t, int64(0), summary.PacksRead)
	rtest.Equals(t, "", summary.LedgerPath)
	_, statErr := os.Stat(wouldBeDir)
	rtest.Assert(t, os.IsNotExist(statErr), "ledger directory must not be created without --resume")
}
