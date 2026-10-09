package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/test"
)

func newTestStore(t testing.TB) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	test.OK(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func mkRecord(id restic.ID, stage Stage, salvage ...restic.BlobHandle) Record {
	return Record{
		PackID:      id,
		BlobHash:    restic.NewRandomID(),
		Size:        1234,
		Stage:       stage,
		Reason:      "test",
		Salvageable: salvage,
		UpdatedAt:   time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC),
	}
}

func reopenStore(t testing.TB, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	test.OK(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TR-1.1: upsert/delete replay with last-write-wins semantics.
func TestStoreApplyReplay(t *testing.T) {
	s, dir := newTestStore(t)
	id1, id2, id3 := restic.NewRandomID(), restic.NewRandomID(), restic.NewRandomID()

	test.OK(t, s.Put(mkRecord(id1, StageVerified)))
	test.OK(t, s.Apply([]Record{mkRecord(id2, StageRegistered), mkRecord(id3, StageRegistered)}, nil))

	// overwrite id1 multiple times within one batch
	updated := mkRecord(id1, StageRegistered)
	updated.Reason = "damaged"
	test.OK(t, s.Put(updated))
	test.OK(t, s.Delete(id3))

	test.Equals(t, 2, s.Len())
	rec, ok := s.Get(id1)
	test.Assert(t, ok, "record id1 must exist")
	test.Equals(t, StageRegistered, rec.Stage)
	test.Equals(t, "damaged", rec.Reason)
	_, ok = s.Get(id3)
	test.Assert(t, !ok, "deleted record must be gone")

	s.Close()
	s = reopenStore(t, dir)
	test.Equals(t, 2, s.Len())
	rec, ok = s.Get(id1)
	test.Assert(t, ok, "record id1 must survive replay")
	test.Equals(t, "damaged", rec.Reason)
	_, ok = s.Get(id2)
	test.Assert(t, ok, "record id2 must survive replay")
	_, ok = s.Get(id3)
	test.Assert(t, !ok, "delete must survive replay")
}

// buildSegment encodes frames with explicit sequences for format tests.
func buildSegment(t testing.TB, seq uint64, recs []Record, garbage []byte) []byte {
	t.Helper()
	var out []byte
	for i, rec := range recs {
		wr := encodeRecord(rec)
		payload, err := json.Marshal(wireFrame{Seq: seq + uint64(i), Op: opUpsert, Rec: &wr})
		test.OK(t, err)
		frame, err := encodeFrame(seq+uint64(i), opUpsert, payload)
		test.OK(t, err)
		out = append(out, frame...)
	}
	return append(out, garbage...)
}

func writeSegmentFile(t testing.TB, dir, name string, data []byte) {
	t.Helper()
	test.OK(t, os.MkdirAll(filepath.Join(dir, segDirName), 0o700))
	test.OK(t, os.WriteFile(filepath.Join(dir, segDirName, name), data, 0o600))
}

func segName(seq uint64) string {
	return fmt.Sprintf("%020d", seq)
}

// TR-1.2: a torn tail of the last segment is truncated, complete records
// survive and the segment stays writable.
func TestStoreTornTailRecovery(t *testing.T) {
	dir := t.TempDir()
	id1, id2, id3 := restic.NewRandomID(), restic.NewRandomID(), restic.NewRandomID()
	data := buildSegment(t, 1, []Record{
		mkRecord(id1, StageVerified),
		mkRecord(id2, StageVerified),
		mkRecord(id3, StageVerified),
	}, []byte{0x52, 0x53, 0x54, 0x4c, 0x01, 0x01, 0xff, 0xff})
	writeSegmentFile(t, dir, segName(1), data)

	s := reopenStore(t, dir)
	test.Equals(t, 3, s.Len())

	// torn bytes must have been truncated away
	info, err := os.Stat(filepath.Join(dir, segDirName, segName(1)))
	test.OK(t, err)
	test.Assert(t, info.Size() < int64(len(data)), "torn tail should be truncated")
	validLen := info.Size()

	// store remains writable and new data survives another reopen
	test.OK(t, s.Put(mkRecord(restic.NewRandomID(), StageRegistered)))
	test.Equals(t, 4, s.Len())
	s.Close()

	s = reopenStore(t, dir)
	defer s.Close()
	test.Equals(t, 4, s.Len())
	info, err = os.Stat(filepath.Join(dir, segDirName, segName(1)))
	test.OK(t, err)
	test.Assert(t, info.Size() > validLen, "new frame should be appended")
}

// TR-1.2b: a torn tail shorter than the frame header is truncated as well.
func TestStoreTornTailShortGarbage(t *testing.T) {
	dir := t.TempDir()
	id := restic.NewRandomID()
	data := buildSegment(t, 1, []Record{mkRecord(id, StageVerified)}, []byte{0xaa, 0xbb})
	writeSegmentFile(t, dir, segName(1), data)

	s := reopenStore(t, dir)
	defer s.Close()
	test.Equals(t, 1, s.Len())
	info, err := os.Stat(filepath.Join(dir, segDirName, segName(1)))
	test.OK(t, err)
	test.Equals(t, int64(len(data)-2), info.Size())
}

// TR-1.3: corruption in a non-last segment and invalid snapshots are
// structural errors that must not modify the directory.
func TestStoreStructuralCorruption(t *testing.T) {
	// initialize a ledger first so that Open does not legitimately create any
	// missing directories during the fail-closed attempt
	s0, dir := newTestStore(t)
	test.OK(t, s0.Close())

	id1, id2, id3, id4 := restic.NewRandomID(), restic.NewRandomID(), restic.NewRandomID(), restic.NewRandomID()
	first := buildSegment(t, 1, []Record{
		mkRecord(id1, StageVerified),
		mkRecord(id2, StageVerified),
	}, []byte{0x00, 0x11, 0x22, 0x33, 0x44})
	second := buildSegment(t, 4, []Record{
		mkRecord(id3, StageVerified),
		mkRecord(id4, StageVerified),
	}, nil)
	writeSegmentFile(t, dir, segName(1), first)
	writeSegmentFile(t, dir, segName(4), second)

	before := hashTree(t, dir)
	_, err := Open(dir)
	test.Assert(t, errors.Is(err, ErrStructural), "expected structural error, got %v", err)
	test.Equals(t, before, hashTree(t, dir))

	// invalid snapshot version must also fail closed
	s1, dir2 := newTestStore(t)
	test.OK(t, s1.Close())
	snap, err := json.Marshal(wireSnapshot{Version: 99, NextSeq: 1})
	test.OK(t, err)
	test.OK(t, os.WriteFile(filepath.Join(dir2, snapDirName, snapName), snap, 0o600))
	before2 := hashTree(t, dir2)
	_, err = Open(dir2)
	test.Assert(t, errors.Is(err, ErrStructural), "expected structural error, got %v", err)
	test.Equals(t, before2, hashTree(t, dir2))
}

func hashTree(t testing.TB, dir string) string {
	t.Helper()
	h := sha256.New()
	err := filepath.Walk(dir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		test.OK(t, err)
		// Open always (re)creates the lock file, it is not ledger state.
		if rel == lockName {
			return nil
		}
		// directory sizes change as entries are added; hash only names for dirs
		if fi.IsDir() {
			fmt.Fprintf(h, "%s:dir\n", rel)
		} else {
			fmt.Fprintf(h, "%s:%d\n", rel, fi.Size())
		}
		if !fi.IsDir() {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = io.Copy(h, f)
			if err != nil {
				return err
			}
		}
		return nil
	})
	test.OK(t, err)
	return hex.EncodeToString(h.Sum(nil))
}

// TR-1.4: compaction at every simulated crash point keeps all records.
func TestStoreCompactionCrashPoints(t *testing.T) {
	s, dir := newTestStore(t)
	var ids []restic.ID
	for range 50 {
		id := restic.NewRandomID()
		ids = append(ids, id)
		test.OK(t, s.Put(mkRecord(id, StageVerified)))
	}

	// point: snapshot written, segments still present (normal post-crash state)
	test.OK(t, s.Compact())
	s.Close()
	s = reopenStore(t, dir)
	test.Equals(t, 50, s.Len())

	// more writes after compaction land in a fresh segment
	for range 10 {
		id := restic.NewRandomID()
		ids = append(ids, id)
		test.OK(t, s.Put(mkRecord(id, StageRegistered)))
	}
	test.Equals(t, 60, s.Len())
	s.Close()
	s = reopenStore(t, dir)
	test.Equals(t, 60, s.Len())

	// point: obsolete segment left behind because its removal failed; frames
	// below the snapshot sequence must be ignored
	s.Compact()
	s.Close()
	leftover := buildSegment(t, 1, []Record{
		mkRecord(restic.NewRandomID(), StageRegistered), // must NOT reappear
	}, nil)
	writeSegmentFile(t, dir, segName(1), leftover)
	s = reopenStore(t, dir)
	defer s.Close()
	test.Equals(t, 60, s.Len())
	for _, id := range ids {
		_, ok := s.Get(id)
		test.Assert(t, ok, "record %v missing after compaction replay", id)
	}

	// repeated compaction and new writes still work
	test.OK(t, s.Compact())
	test.OK(t, s.Put(mkRecord(restic.NewRandomID(), StageVerified)))
	test.OK(t, s.Compact())
}

// TR-1.5: concurrent opens of the same directory conflict.
func TestStoreExclusiveLock(t *testing.T) {
	s, dir := newTestStore(t)
	_, err := Open(dir)
	test.Assert(t, err != nil, "second open must fail")
	test.Equals(t, errLockBusy, err)
	test.OK(t, s.Close())

	s2, err := Open(dir)
	test.OK(t, err)
	test.OK(t, s2.Close())
}

// TR-1.7: memory and replay cost at repository scale (1M verified records).
func TestStoreScaleOneMillion(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test skipped in short mode")
	}
	const n = 1_000_000
	dir := t.TempDir()
	s, err := Open(dir)
	test.OK(t, err)

	const batch = 10_000
	batchRec := make([]Record, batch)
	start := time.Now()
	for i := range n {
		rec := mkRecord(restic.NewRandomID(), StageVerified)
		rec.Reason = ""
		batchRec[i%batch] = rec
		if i%batch == batch-1 {
			test.OK(t, s.Apply(batchRec, nil))
		}
	}
	test.Equals(t, n, s.Len())

	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	perRecord := ms.HeapAlloc / n
	t.Logf("1M records: write=%v heap=%dMiB (%dB/record) segsize~=%dMiB",
		time.Since(start).Round(time.Second), ms.HeapAlloc/1024/1024, perRecord, s.segSize/1024/1024)
	test.Assert(t, perRecord < 256, "per-record heap %dB should stay below 256B", perRecord)

	compactStart := time.Now()
	test.OK(t, s.Compact())
	t.Logf("compaction took %v", time.Since(compactStart).Round(time.Millisecond))
	test.OK(t, s.Close())

	reopenStart := time.Now()
	s2, err := Open(dir)
	test.OK(t, err)
	t.Logf("replay from snapshot took %v", time.Since(reopenStart).Round(time.Millisecond))
	test.Equals(t, n, s2.Len())
	test.OK(t, s2.Close())
}
