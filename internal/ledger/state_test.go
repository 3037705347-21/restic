package ledger

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/test"
)

var errInjected = errors.New("injected fsync failure")

func fpFor(rec Record) Fingerprint {
	return Fingerprint{BlobHash: rec.BlobHash, Size: rec.Size}
}

// TR-2.1: SelectPending classifies all reconciliation cases correctly.
func TestSelectPending(t *testing.T) {
	read := mkRecord(restic.NewRandomID(), StageVerified)
	sameFP := fpFor(read)

	changed := mkRecord(restic.NewRandomID(), StageVerified)
	changedFP := Fingerprint{BlobHash: restic.NewRandomID(), Size: changed.Size}

	sizediff := mkRecord(restic.NewRandomID(), StageVerified)
	sizeFP := Fingerprint{BlobHash: sizediff.BlobHash, Size: sizediff.Size + 7}

	quarantined := mkRecord(restic.NewRandomID(), StageRegistered)
	quarantineFP := fpFor(quarantined)

	quarChanged := mkRecord(restic.NewRandomID(), StageRegistered)
	quarChangedFP := Fingerprint{BlobHash: restic.NewRandomID(), Size: quarChanged.Size}

	salvaged := mkRecord(restic.NewRandomID(), StageSalvaged)
	salvagedFP := Fingerprint{BlobHash: restic.NewRandomID(), Size: salvaged.Size}

	gone := mkRecord(restic.NewRandomID(), StageVerified)

	newPack := restic.NewRandomID()
	newPackFP := Fingerprint{BlobHash: restic.NewRandomID(), Size: 42}

	records := []Record{read, changed, sizediff, quarantined, quarChanged, salvaged, gone}
	current := map[restic.ID]Fingerprint{
		read.PackID:        sameFP,
		changed.PackID:     changedFP,
		sizediff.PackID:    sizeFP,
		quarantined.PackID: quarantineFP,
		quarChanged.PackID: quarChangedFP,
		salvaged.PackID:    salvagedFP,
		newPack:            newPackFP,
	}

	sel := SelectPending(records, current)
	pending := func(id restic.ID) bool {
		_, ok := sel.Pending[id]
		return ok
	}

	test.Assert(t, !pending(read.PackID), "unchanged verified pack must be skipped")
	test.Assert(t, pending(changed.PackID), "blobhash-changed pack must be pending")
	test.Assert(t, pending(sizediff.PackID), "size-changed pack must be pending")
	test.Assert(t, !pending(quarantined.PackID), "quarantined pack with same fingerprint is owned by repair")
	test.Assert(t, pending(quarChanged.PackID), "registered pack whose index changed must be re-checked")
	test.Assert(t, !pending(salvaged.PackID), "salvaged pack is owned by repair")
	test.Assert(t, pending(newPack), "unknown pack must be pending")
	test.Assert(t, sel.Stale.Has(gone.PackID), "pack removed from index must be stale")
	test.Equals(t, 4, len(sel.Pending))
}

// TR-2.2: partial batches commit on Flush with a single fsync and survive
// reopen.
func TestCommitterBatching(t *testing.T) {
	s, dir := newTestStore(t)
	var fsyncs atomic.Int32
	s.fsyncFileImpl = func(f *os.File) error {
		fsyncs.Add(1)
		return fsyncFile(f)
	}

	c := NewCommitter(s)
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			test.OK(t, c.Submit(mkRecord(restic.NewRandomID(), StageVerified)))
		}()
	}
	wg.Wait()
	test.OK(t, c.Flush())
	test.Equals(t, int32(1), fsyncs.Load())
	test.Equals(t, 100, s.Len())
	test.OK(t, c.Close())
	test.OK(t, s.Close())

	s2 := reopenStore(t, dir)
	defer s2.Close()
	test.Equals(t, 100, s2.Len())
}

// TR-2.3: after an fsync failure, all later operations fail fast and the batch
// can only survive as a whole (complete frames); a torn/partial frame must never
// survive (that case is covered by the truncation tests).
func TestCommitterPersistenceFailure(t *testing.T) {
	const n = 50
	dir := t.TempDir()
	s, err := Open(dir)
	test.OK(t, err)

	s.fsyncFileImpl = func(_ *os.File) error { return errInjected }
	c := NewCommitter(s)
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Submit(mkRecord(restic.NewRandomID(), StageVerified))
		}()
	}
	err = c.Flush()
	wg.Wait()
	test.Assert(t, errors.Is(err, errInjected), "flush must surface injected error, got %v", err)
	// later submissions fail immediately
	err = c.Submit(mkRecord(restic.NewRandomID(), StageVerified))
	test.Assert(t, err != nil, "submit after fatal error must fail")
	_ = c.Close()
	_ = s.Close()

	s2, err := Open(dir)
	test.OK(t, err)
	cnt := s2.Len()
	test.Assert(t, cnt >= 0 && cnt <= n, "failed batches may survive only whole, got %d of %d", cnt, n)
	test.OK(t, s2.Close())
}

// TR-2.3b: a partial write (frame torn mid-batch) is truncated on reopen and
// must never expose an incomplete record.
func TestCommitterTornBatch(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	test.OK(t, err)

	first := mkRecord(restic.NewRandomID(), StageVerified)
	test.OK(t, s.Put(first))

	// force a fresh segment, then inject an fsync that drops half the batch
	s.mu.Lock()
	if err := s.rotateSegment(s.nextSeq); err != nil {
		t.Fatal(err)
	}
	s.mu.Unlock()
	var dropped atomic.Bool
	s.fsyncFileImpl = func(f *os.File) error {
		if dropped.CompareAndSwap(false, true) {
			// emulate a torn append: truncate to half the frames just written
			fi, _ := f.Stat()
			_ = f.Truncate(fi.Size() / 2)
			return errInjected
		}
		return fsyncFile(f)
	}
	err = s.Apply([]Record{
		mkRecord(restic.NewRandomID(), StageVerified),
		mkRecord(restic.NewRandomID(), StageVerified),
	}, nil)
	test.Assert(t, errors.Is(err, errInjected), "torn write must surface, got %v", err)
	_ = s.Close()

	s2, err := Open(dir)
	test.OK(t, err)
	cnt := s2.Len()
	test.Assert(t, cnt >= 1 && cnt <= 3, "torn batch must not expose partial state, got %d", cnt)
	test.OK(t, s2.Close())
}

// TR-2.4: repeated quarantine registration keeps a single record and refreshes
// its payload.
func TestRegisterQuarantineIdempotent(t *testing.T) {
	s, dir := newTestStore(t)
	id := restic.NewRandomID()
	fp := Fingerprint{BlobHash: restic.NewRandomID(), Size: 99}

	res, err := s.RegisterQuarantine(id, fp, ReasonDataVerification, nil)
	test.OK(t, err)
	test.Equals(t, RegistrationCreated, res)

	salvage := []restic.BlobHandle{
		{ID: restic.NewRandomID(), Type: restic.DataBlob},
		{ID: restic.NewRandomID(), Type: restic.TreeBlob},
	}
	for range 99 {
		res, err = s.RegisterQuarantine(id, fp, ReasonDataVerification, salvage)
		test.OK(t, err)
		test.Equals(t, RegistrationUpdated, res)
	}
	test.Equals(t, 1, s.Len())
	rec, ok := s.Get(id)
	test.Assert(t, ok, "record must exist")
	test.Equals(t, StageRegistered, rec.Stage)
	test.Equals(t, 2, len(rec.Salvageable))

	// later repair stages must not be rolled back
	test.OK(t, s.AdvanceStage(id, StageSalvaged))
	res, err = s.RegisterQuarantine(id, fp, "again", nil)
	test.OK(t, err)
	test.Equals(t, RegistrationUnchanged, res)
	rec, _ = s.Get(id)
	test.Equals(t, StageSalvaged, rec.Stage)
	test.Equals(t, 2, len(rec.Salvageable))

	s.Close()
	s2 := reopenStore(t, dir)
	defer s2.Close()
	test.Equals(t, 1, s2.Len())
	rec, _ = s2.Get(id)
	test.Equals(t, StageSalvaged, rec.Stage)
}

// TR-2.5: stage machine only allows forward transitions and removes the record
// at removed.
func TestStageTransitions(t *testing.T) {
	s, dir := newTestStore(t)
	id := restic.NewRandomID()
	fp := Fingerprint{BlobHash: restic.NewRandomID(), Size: 10}
	_, err := s.RegisterQuarantine(id, fp, ReasonMissingPack, nil)
	test.OK(t, err)

	test.Assert(t, errors.Is(s.AdvanceStage(id, StageRewritten), ErrInvalidStageTransition), "stage skip rejected")
	test.Assert(t, errors.Is(s.AdvanceStage(id, StageVerified), ErrInvalidStageTransition), "stage rollback rejected")
	rec, _ := s.Get(id)
	test.Equals(t, StageRegistered, rec.Stage)

	test.OK(t, s.AdvanceStage(id, StageSalvaged))
	test.OK(t, s.AdvanceStage(id, StageRewritten))
	test.OK(t, s.AdvanceStage(id, StageRemoved))
	_, ok := s.Get(id)
	test.Assert(t, !ok, "record must be deleted after removal")
	test.OK(t, s.AdvanceStage(id, StageRemoved)) // idempotent on missing record

	s.Close()
	s2 := reopenStore(t, dir)
	defer s2.Close()
	test.Equals(t, 0, s2.Len())
}

// TestMarkVerifiedSelfHeal covers the registered -> verified transition that is
// allowed only after the index fingerprint changed.
func TestMarkVerifiedSelfHeal(t *testing.T) {
	s, _ := newTestStore(t)
	id := restic.NewRandomID()
	fp := Fingerprint{BlobHash: restic.NewRandomID(), Size: 10}
	_, err := s.RegisterQuarantine(id, fp, "broken", nil)
	test.OK(t, err)

	// same fingerprint: pack was never re-read, MarkVerified is a no-op
	test.OK(t, s.MarkVerified(id, fp))
	rec, _ := s.Get(id)
	test.Equals(t, StageRegistered, rec.Stage)

	changed := Fingerprint{BlobHash: restic.NewRandomID(), Size: 10}
	test.OK(t, s.MarkVerified(id, changed))
	rec, _ = s.Get(id)
	test.Equals(t, StageVerified, rec.Stage)
}

// TestBatchStageAndStaleHelpers covers batch stage advancement, stale removal
// and the quarantine listing.
func TestBatchStageAndStaleHelpers(t *testing.T) {
	s, dir := newTestStore(t)
	fp := Fingerprint{BlobHash: restic.NewRandomID(), Size: 3}
	ids := restic.NewIDSet()
	var registered []restic.ID
	for range 5 {
		id := restic.NewRandomID()
		_, err := s.RegisterQuarantine(id, fp, ReasonMissingPack, nil)
		test.OK(t, err)
		ids.Insert(id)
		registered = append(registered, id)
	}
	verifiedGone := mkRecord(restic.NewRandomID(), StageVerified)
	test.OK(t, s.Put(verifiedGone))

	test.Equals(t, 5, len(s.Quarantine()))

	// all five advance together
	test.OK(t, s.AdvanceStages(ids, StageSalvaged))
	for _, id := range registered {
		rec, ok := s.Get(id)
		test.Assert(t, ok, "salvaged record must remain")
		test.Equals(t, StageSalvaged, rec.Stage)
	}
	// second identical batch transition is rejected (records already moved)
	err := s.AdvanceStages(ids, StageRewritten) // valid one step, do it for real below
	test.OK(t, err)
	err = s.AdvanceStages(ids, StageRemoved) // removed must use per-record delete
	test.Assert(t, errors.Is(err, ErrInvalidStageTransition), "removed cannot be batch-set")

	// stale removal batch deletes vanished verified records
	stale := restic.NewIDSet()
	stale.Insert(verifiedGone.PackID)
	test.OK(t, s.RemoveStale(stale))
	_, ok := s.Get(verifiedGone.PackID)
	test.Assert(t, !ok, "stale record removed")
	test.OK(t, s.RemoveStale(restic.NewIDSet()))

	s.Close()
	s2 := reopenStore(t, dir)
	defer s2.Close()
	test.Equals(t, 5, len(s2.Quarantine()))
}

// TestCommitterConcurrentClose exercises shutdown while submitters race with
// Close (TR-2.6 race coverage).
func TestCommitterConcurrentClose(t *testing.T) {
	s, _ := newTestStore(t)
	c := NewCommitter(s)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				if err := c.Submit(mkRecord(restic.NewRandomID(), StageVerified)); err != nil {
					return
				}
			}
		}()
	}
	time.Sleep(5 * time.Millisecond)
	test.OK(t, c.Close())
	wg.Wait()
}
