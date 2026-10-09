package repository

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/repository/pack"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

var errFakeRewrite = fakeErr("rewrite failed")

type fakeErr string

func (e fakeErr) Error() string { return string(e) }

type capturePrinter struct {
	mu  sync.Mutex
	buf []byte
}

func (c *capturePrinter) PT(s string, args ...any) { c.add(fmt.Sprintf("PT "+s+"\n", args...)) }
func (c *capturePrinter) P(s string, args ...any)  { c.add(fmt.Sprintf("P "+s+"\n", args...)) }
func (c *capturePrinter) V(s string, args ...any)  { c.add(fmt.Sprintf("V "+s+"\n", args...)) }
func (c *capturePrinter) VV(s string, args ...any) { c.add(fmt.Sprintf("VV "+s+"\n", args...)) }
func (c *capturePrinter) S(s string, args ...any)  { c.add(fmt.Sprintf("S "+s+"\n", args...)) }
func (c *capturePrinter) E(s string, args ...any)  { c.add(fmt.Sprintf("E "+s+"\n", args...)) }
func (c *capturePrinter) add(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf = append(c.buf, s...)
}
func (c *capturePrinter) all() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return string(c.buf)
}
func (c *capturePrinter) NewCounter(string) restic.Counter { return restic.NoopCounter }
func (c *capturePrinter) NewCounterTerminalOnly(string) restic.Counter {
	return restic.NoopCounter
}

// fakeQuarantineStore is an in-memory QuarantineStore that records stage
// transitions.
type fakeQuarantineStore struct {
	mu          sync.Mutex
	records     map[restic.ID]QuarantineEntry
	salvaged    []restic.ID
	rewritten   []restic.IDSet
	removed     []restic.ID
	failRewrite bool
}

func newFakeStore(entries ...QuarantineEntry) *fakeQuarantineStore {
	f := &fakeQuarantineStore{records: make(map[restic.ID]QuarantineEntry, len(entries))}
	for _, e := range entries {
		f.records[e.PackID] = e
	}
	return f
}

func (f *fakeQuarantineStore) QuarantineRecords() []QuarantineEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]QuarantineEntry, 0, len(f.records))
	for _, e := range f.records {
		out = append(out, e)
	}
	return out
}

func (f *fakeQuarantineStore) MarkSalvaged(id restic.ID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.records[id]
	e.Stage = QuarantineSalvaged
	f.records[id] = e
	f.salvaged = append(f.salvaged, id)
	return nil
}

func (f *fakeQuarantineStore) MarkRewritten(ids restic.IDSet) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failRewrite {
		return errFakeRewrite
	}
	cp := restic.NewIDSet()
	for id := range ids {
		cp.Insert(id)
		e := f.records[id]
		e.Stage = QuarantineRewritten
		f.records[id] = e
	}
	f.rewritten = append(f.rewritten, cp)
	return nil
}

func (f *fakeQuarantineStore) Remove(id restic.ID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.records, id)
	f.removed = append(f.removed, id)
	return nil
}

// repairTestRepo creates a repo with one snapshot and returns the first data
// pack together with its index blob entries.
func repairTestRepo(t *testing.T) (*Repository, restic.ID, pack.Blobs) {
	t.Helper()
	repo, _ := TestRepositoryWithBackend(t, nil, 0, Options{})
	data.TestCreateSnapshot(t, repo, time.Unix(1470492820, 207401672), 2)
	rtest.OK(t, repo.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory))

	sizes, err := pack.Size(context.TODO(), repo, false)
	rtest.OK(t, err)
	ids := restic.NewIDSet()
	for id := range sizes {
		ids.Insert(id)
	}
	var packID restic.ID
	var found pack.Blobs
	// drain the channel fully: ListPacks keeps a background goroutine running
	// and returning early would race the index rewrite done by the repair
	for pbs := range repo.listPacksFromIndex(context.TODO(), ids) {
		if found != nil {
			continue
		}
		var dataBlobs pack.Blobs
		for _, b := range pbs.Blobs {
			if b.Type == restic.DataBlob {
				dataBlobs = append(dataBlobs, b)
			}
		}
		if len(dataBlobs) > 0 {
			packID = pbs.PackID
			found = dataBlobs
		}
	}
	if found == nil {
		t.Fatal("no data pack created")
	}
	return repo, packID, found
}

func removePackFile(t *testing.T, repo *Repository, id restic.ID) {
	t.Helper()
	rtest.OK(t, repo.be.Remove(context.TODO(), backend.Handle{Type: backend.PackFile, Name: id.String()}))
}

// TR-6.1 + TR-6.4: registered pack is salvaged, rewritten, deleted and
// removed; only salvageable blobs survive; a second run is a no-op.
func TestRepairPacksFromLedgerFullFlow(t *testing.T) {
	repo, packID, dataBlobs := repairTestRepo(t)
	wanted := restic.NewBlobSet(dataBlobs[0].BlobHandle) // salvage exactly one blob

	// the damaged pack must still be present for salvage; salvage reads the
	// good blobs out of it before the rewritten index and the delete drop it
	store := newFakeStore(QuarantineEntry{
		PackID:      packID,
		Stage:       QuarantineRegistered,
		Salvageable: wanted.List(),
	})

	printer := &capturePrinter{}
	rtest.OK(t, RepairPacksFromLedger(context.TODO(), repo, store, printer))
	t.Logf("repair output: %s", printer.all())

	rtest.Equals(t, 1, len(store.salvaged), "salvage stage once")
	rtest.Equals(t, packID, store.salvaged[0])
	rtest.Equals(t, 1, len(store.rewritten), "one rewrite batch")
	rtest.Assert(t, store.rewritten[0].Has(packID), "rewritten set contains the pack")
	rtest.Equals(t, 1, len(store.removed))
	rtest.Equals(t, packID, store.removed[0])
	rtest.Equals(t, 0, len(store.records))

	// the index was rebuilt on the backend: reopen as a new command would and
	// verify the salvaged blob is readable from the re-uploaded copy
	reopened := TestOpenBackend(t, repo.be)
	rtest.OK(t, reopened.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory))
	buf, err := reopened.LoadBlob(context.TODO(), dataBlobs[0].BlobHandle, nil)
	rtest.OK(t, err)
	rtest.Assert(t, len(buf) > 0, "salvaged blob must be readable")

	// other blobs of the deleted pack are not available
	if len(dataBlobs) > 1 {
		_, err := reopened.LoadBlob(context.TODO(), dataBlobs[1].BlobHandle, nil)
		rtest.Assert(t, err != nil, "non-salvaged blob must not be recoverable")
	}

	// a second run has nothing to do and must not touch any stage
	rtest.OK(t, RepairPacksFromLedger(context.TODO(), repo, store, restic.NewNoopPrinter()))
	rtest.Equals(t, 1, len(store.salvaged))
	rtest.Equals(t, 1, len(store.rewritten))
}

// TR-6.2: a pack already in salvaged stage skips salvage entirely.
func TestRepairPacksFromLedgerResumeAtRewrite(t *testing.T) {
	repo, packID, _ := repairTestRepo(t)

	store := newFakeStore(QuarantineEntry{PackID: packID, Stage: QuarantineSalvaged})
	rtest.OK(t, RepairPacksFromLedger(context.TODO(), repo, store, restic.NewNoopPrinter()))

	rtest.Equals(t, 0, len(store.salvaged), "salvage must not run again")
	rtest.Equals(t, 1, len(store.rewritten))
	rtest.Assert(t, store.rewritten[0].Has(packID), "rewritten set must contain the pack")
	rtest.Equals(t, 1, len(store.removed))
	rtest.Equals(t, 0, len(store.records))
}

// TR-6.3: a pack missing from backend and index is forgotten directly.
func TestRepairPacksFromLedgerMissingPack(t *testing.T) {
	repo, packID, _ := repairTestRepo(t)
	removePackFile(t, repo, packID)

	store := newFakeStore(QuarantineEntry{PackID: packID, Stage: QuarantineRegistered})
	rtest.OK(t, RepairPacksFromLedger(context.TODO(), repo, store, restic.NewNoopPrinter()))

	rtest.Equals(t, 0, len(store.salvaged))
	rtest.Equals(t, 0, len(store.rewritten))
	rtest.Equals(t, 1, len(store.removed))
	rtest.Equals(t, packID, store.removed[0])
}

// TR-6.2b: failure while marking the rewritten stage is surfaced and a retry
// completes without redoing salvage.
func TestRepairPacksFromLedgerRewriteFailureRetries(t *testing.T) {
	repo, packID, _ := repairTestRepo(t)
	store := newFakeStore(QuarantineEntry{PackID: packID, Stage: QuarantineSalvaged})
	store.failRewrite = true

	err := RepairPacksFromLedger(context.TODO(), repo, store, restic.NewNoopPrinter())
	rtest.Assert(t, err != nil, "injected rewrite failure must surface")
	rec, ok := store.records[packID]
	rtest.Assert(t, ok, "record must survive the failure: %v", packID)
	rtest.Equals(t, QuarantineSalvaged, rec.Stage)
	rtest.Equals(t, 0, len(store.salvaged))

	store.failRewrite = false
	rtest.OK(t, RepairPacksFromLedger(context.TODO(), repo, store, restic.NewNoopPrinter()))
	rtest.Equals(t, 1, len(store.rewritten))
	rtest.Equals(t, 1, len(store.removed))
}
