package main

import (
	stderrors "errors"
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/restic/restic/internal/backend/cache"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/ledger"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
)

const ledgerDirName = "verify-ledger"

// persistentCacheBase returns the persistent cache base directory, ignoring the
// temporary-cache override that check installs by default. It returns an
// empty string when caching is disabled.
func persistentCacheBase(gopts global.Options) string {
	if gopts.NoCache {
		return ""
	}
	if gopts.CacheDir != "" {
		return gopts.CacheDir
	}
	if base := cache.EnvDir(); base != "" {
		return base
	}
	base, err := cache.DefaultDir()
	if err != nil {
		return ""
	}
	return base
}

// resolveLedgerDir returns the ledger directory: an explicit --ledger-dir wins,
// otherwise it is derived from the persistent cache base and the repository ID.
func resolveLedgerDir(explicit, cacheBase, repoID string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if cacheBase == "" {
		return "", errors.Fatal("the verification ledger needs a persistent location: enable the cache or specify --ledger-dir")
	}
	return filepath.Join(cacheBase, repoID, ledgerDirName), nil
}

// openLedgerStore opens the ledger, turning structural corruption into a
// fail-closed error that names the directory and suggests recovery.
func openLedgerStore(dir string) (*ledger.Store, error) {
	store, err := ledger.Open(dir)
	if err == nil {
		return store, nil
	}
	if stderrors.Is(err, ledger.ErrStructural) {
		return nil, fmt.Errorf("verification ledger in %s is structurally corrupted and will not be reset automatically; "+
			"move the directory aside to fall back to a full data verification: %w", dir, err)
	}
	return nil, err
}

// checkLedger wires the persistent ledger into the check run: it decides which
// packs to read, persists results in lockstep with verification and counts
// what happened for the summary.
type checkLedger struct {
	store     *ledger.Store
	committer *ledger.Committer

	fps map[restic.ID]ledger.Fingerprint
	// pending contains exactly the packs that must be read this run.
	pending map[restic.ID]ledger.Fingerprint

	// verifiedSkipped and quarantinedSkipped are populated once by the
	// filter callback, which runs before the workers start.
	verifiedSkipped    int
	quarantinedSkipped int

	// read and newlyQuarantined are updated by the concurrent workers.
	read             int64
	newlyQuarantined int64
}

func newCheckLedger(store *ledger.Store, fingerprints map[restic.ID]repository.PackFingerprint) (*checkLedger, error) {
	l := &checkLedger{
		store:     store,
		committer: ledger.NewCommitter(store),
		fps:       make(map[restic.ID]ledger.Fingerprint, len(fingerprints)),
	}
	for id, fp := range fingerprints {
		l.fps[id] = ledger.Fingerprint{BlobHash: fp.BlobHash, Size: fp.Size}
	}

	sel := ledger.SelectPending(store.Records(), l.fps)
	if err := store.RemoveStale(sel.Stale); err != nil {
		return nil, err
	}
	l.pending = sel.Pending
	return l, nil
}

// pendingFilter wraps the user's subset/filter: only packs selected for
// reading by the ledger are returned, everything else is counted as skipped.
func (l *checkLedger) pendingFilter(printer restic.Printer, user func(map[restic.ID]int64) map[restic.ID]int64) func(map[restic.ID]int64) map[restic.ID]int64 {
	return func(allPacks map[restic.ID]int64) map[restic.ID]int64 {
		chosen := user(allPacks)
		toRead := make(map[restic.ID]int64)
		for id, size := range chosen {
			if _, ok := l.pending[id]; ok {
				toRead[id] = size
				continue
			}
			// a pack not pending is either already verified or handed off to
			// repair; distinguish the two for the progress summary
			if rec, known := l.store.Get(id); known && rec.Stage.Quarantine() {
				l.quarantinedSkipped++
			} else {
				l.verifiedSkipped++
			}
		}
		printer.P("resuming with verification ledger %v: %d packs to read, %d already verified skipped, %d damaged skipped (awaiting repair)",
			l.store.Dir(), len(toRead), l.verifiedSkipped, l.quarantinedSkipped)
		return toRead
	}
}

// hook is invoked concurrently after every verified pack.
func (l *checkLedger) hook(res repository.PackCheckResult) error {
	fp := l.fps[res.ID]
	if res.Err == nil {
		atomic.AddInt64(&l.read, 1)
		return l.committer.SubmitVerified(res.ID, fp)
	}

	var dataErr *repository.ErrPackData
	if !stderrors.As(res.Err, &dataErr) {
		// a complete download failure does not prove damage; the pack is not
		// registered and will simply be read again on the next run.
		atomic.AddInt64(&l.read, 1)
		return nil
	}

	atomic.AddInt64(&l.read, 1)
	reg, err := l.store.RegisterQuarantine(res.ID, fp, ledger.ReasonDataVerification, res.GoodBlobs)
	if err != nil {
		return err
	}
	if reg == ledger.RegistrationCreated {
		atomic.AddInt64(&l.newlyQuarantined, 1)
	}
	return nil
}

// registerMetadata records a missing/truncated pack discovered by the cheap
// metadata comparison phase.
func (l *checkLedger) registerMetadata(id restic.ID, reason string) error {
	fp, ok := l.fps[id]
	if !ok {
		return nil
	}
	reg, err := l.store.RegisterQuarantine(id, fp, reason, nil)
	if err != nil {
		return err
	}
	if reg == ledger.RegistrationCreated {
		atomic.AddInt64(&l.newlyQuarantined, 1)
	}
	return nil
}

// counts returns the counters for the run summary.
func (l *checkLedger) counts() (read, skipped, newlyQ int64, quarantineTotal int, dir string) {
	read = atomic.LoadInt64(&l.read)
	skipped = int64(l.verifiedSkipped + l.quarantinedSkipped)
	newlyQ = atomic.LoadInt64(&l.newlyQuarantined)
	quarantineTotal = len(l.store.Quarantine())
	dir = l.store.Dir()
	return
}

// quarantineIDs returns all pack IDs currently recorded as quarantined.
func (l *checkLedger) quarantineIDs() restic.IDSet {
	ids := restic.NewIDSet()
	for _, rec := range l.store.Quarantine() {
		ids.Insert(rec.PackID)
	}
	return ids
}

// finish flushes pending batches, compacts when the segment log grew large and
// closes the ledger.
func (l *checkLedger) finish() error {
	if err := l.committer.Close(); err != nil {
		_ = l.store.Close()
		return err
	}
	if segBytes, err := l.store.SegmentBytes(); err == nil && segBytes > ledger.CompactThreshold {
		if err := l.store.Compact(); err != nil {
			_ = l.store.Close()
			return err
		}
	}
	return l.store.Close()
}
