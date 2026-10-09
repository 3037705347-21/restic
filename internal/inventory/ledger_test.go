package inventory_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/restic/restic/internal/inventory"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

type fakeLister struct {
	objects  map[restic.FileType]map[string]int64
	failType restic.FileType
}

func (f *fakeLister) List(_ context.Context, t restic.FileType, fn func(restic.ID, int64) error) error {
	if t == f.failType {
		return errors.New("listing failed")
	}
	for name, size := range f.objects[t] {
		id, err := restic.ParseID(name)
		if err != nil {
			return err
		}
		if err := fn(id, size); err != nil {
			return err
		}
	}
	return nil
}

func testID(t *testing.T, byte_ byte) string {
	t.Helper()
	var id restic.ID
	for i := range id {
		id[i] = byte_
	}
	return id.String()
}

func TestEnumerate(t *testing.T) {
	idA := testID(t, 0x01)
	idB := testID(t, 0x02)
	idC := testID(t, 0x03)

	lister := &fakeLister{
		objects: map[restic.FileType]map[string]int64{
			restic.PackFile:     {idB: 20, idA: 10},
			restic.KeyFile:      {idC: 30},
			restic.SnapshotFile: {},
			restic.IndexFile:    {idA: 40},
			// locks must never be recorded, even if the backend lists them
			restic.LockFile: {idB: 50},
		},
	}

	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	ledger, err := inventory.Enumerate(context.Background(), lister, "repo-id", now)
	rtest.OK(t, err)

	rtest.Equals(t, inventory.CurrentVersion, ledger.Version)
	rtest.Equals(t, "repo-id", ledger.RepoID)
	rtest.Equals(t, now, ledger.GeneratedAt)
	rtest.Equals(t, 4, ledger.Count())

	rtest.Equals(t, []inventory.Record{{ID: idA, Size: 10}, {ID: idB, Size: 20}}, ledger.Objects["data"])
	rtest.Equals(t, []inventory.Record{{ID: idC, Size: 30}}, ledger.Objects["key"])
	rtest.Equals(t, []inventory.Record{}, ledger.Objects["snapshot"])
	rtest.Equals(t, []inventory.Record{{ID: idA, Size: 40}}, ledger.Objects["index"])
	_, locksTracked := ledger.Objects["lock"]
	rtest.Assert(t, !locksTracked, "locks must not be recorded")
}

func TestEnumerateFailure(t *testing.T) {
	lister := &fakeLister{
		objects:  map[restic.FileType]map[string]int64{restic.PackFile: {testID(t, 0x01): 10}},
		failType: restic.KeyFile,
	}

	ledger, err := inventory.Enumerate(context.Background(), lister, "repo-id", time.Now())
	rtest.Assert(t, err != nil, "expected enumeration to fail")
	rtest.Assert(t, ledger == nil, "failed enumeration must not return a ledger")
}

func TestEnumerateContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := inventory.Enumerate(ctx, &fakeLister{}, "repo-id", time.Now())
	rtest.Assert(t, errors.Is(err, context.Canceled), "expected context cancellation error, got %v", err)
}

func ledgerWith(records map[string][]inventory.Record) *inventory.Ledger {
	return &inventory.Ledger{
		Version:     inventory.CurrentVersion,
		RepoID:      "repo-id",
		GeneratedAt: time.Now().UTC(),
		Objects:     records,
	}
}

func TestCompare(t *testing.T) {
	id1 := testID(t, 0x01)
	id2 := testID(t, 0x02)
	id3 := testID(t, 0x03)
	id4 := testID(t, 0x04)

	old := ledgerWith(map[string][]inventory.Record{
		"data":     {{ID: id1, Size: 10}, {ID: id2, Size: 20}},
		"snapshot": {{ID: id4, Size: 40}},
	})
	new := ledgerWith(map[string][]inventory.Record{
		// id1 unchanged, id2 replaced with a different size, id3 added,
		// id4 removed
		"data":     {{ID: id1, Size: 10}, {ID: id2, Size: 22}, {ID: id3, Size: 30}},
		"snapshot": {},
	})

	diff := inventory.Compare(old, new)
	rtest.Equals(t, []inventory.Ref{{Type: "snapshot", ID: id4}}, diff.Removed)
	rtest.Equals(t, []inventory.Ref{{Type: "data", ID: id3}}, diff.Added)
	rtest.Equals(t, []inventory.ReplacedObject{
		{Ref: inventory.Ref{Type: "data", ID: id2}, OldSize: 20, NewSize: 22},
	}, diff.Replaced)
	rtest.Assert(t, diff.Changed(), "removed/replaced objects must count as a change")

	// additions alone must not count as a change
	addOnly := inventory.Compare(
		ledgerWith(map[string][]inventory.Record{"data": {{ID: id1, Size: 10}}}),
		ledgerWith(map[string][]inventory.Record{"data": {{ID: id1, Size: 10}, {ID: id2, Size: 20}}}),
	)
	rtest.Assert(t, !addOnly.Changed(), "added objects must not be reported as a change")
	rtest.Equals(t, 1, len(addOnly.Added))
}

func TestCompareIdentical(t *testing.T) {
	l := ledgerWith(map[string][]inventory.Record{
		"data":  {{ID: testID(t, 0xaa), Size: 1}},
		"index": {{ID: testID(t, 0xbb), Size: 2}},
	})
	diff := inventory.Compare(l, l)
	rtest.Assert(t, !diff.Changed(), "identical inventories must not differ")
	rtest.Equals(t, 0, len(diff.Added))
}

func TestSaveLoadRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "inventory.json")

	l := ledgerWith(map[string][]inventory.Record{
		"data": {{ID: testID(t, 0x07), Size: 77}},
	})

	// Save creates the parent directory if necessary.
	rtest.OK(t, l.Save(path))

	loaded, err := inventory.Load(path)
	rtest.OK(t, err)
	rtest.Equals(t, l.Version, loaded.Version)
	rtest.Equals(t, l.RepoID, loaded.RepoID)
	rtest.Equals(t, l.Objects, loaded.Objects)
}

func TestSaveFailureKeepsExistingLedger(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "inventory.json")

	old := ledgerWith(map[string][]inventory.Record{
		"data": {{ID: testID(t, 0x01), Size: 1}},
	})
	rtest.OK(t, old.Save(path))
	original, err := os.ReadFile(path)
	rtest.OK(t, err)

	// saving to an invalid path (a regular file is used as a directory) fails
	// and must not touch the existing ledger
	bad := &inventory.Ledger{Version: inventory.CurrentVersion, Objects: map[string][]inventory.Record{}}
	err = bad.Save(filepath.Join(dir, "inventory.json", "nested"))
	rtest.Assert(t, err != nil, "expected save to fail")

	after, err := os.ReadFile(path)
	rtest.OK(t, err)
	rtest.Equals(t, original, after)

	// no leftover temporary files
	entries, err := os.ReadDir(dir)
	rtest.OK(t, err)
	for _, entry := range entries {
		rtest.Assert(t, entry.Name() == "inventory.json", "unexpected leftover file %q", entry.Name())
	}
}

func TestLoadMissing(t *testing.T) {
	_, err := inventory.Load(filepath.Join(t.TempDir(), "does-not-exist.json"))
	rtest.Assert(t, errors.Is(err, os.ErrNotExist), "expected os.ErrNotExist, got %v", err)
}

func TestLoadInvalid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inventory.json")
	rtest.OK(t, os.WriteFile(path, []byte("{not json"), 0o600))
	_, err := inventory.Load(path)
	rtest.Assert(t, err != nil, "expected parse error")
}

func TestLoadUnsupportedVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inventory.json")
	buf, err := json.Marshal(map[string]any{
		"version":   inventory.CurrentVersion + 1,
		"repo_id":   "repo-id",
		"objects":   map[string][]inventory.Record{},
		"generated": time.Now(),
	})
	rtest.OK(t, err)
	rtest.OK(t, os.WriteFile(path, buf, 0o600))

	_, err = inventory.Load(path)
	rtest.Assert(t, err != nil, "expected unsupported version error")
}
