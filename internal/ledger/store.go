package ledger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
)

const (
	formatVersion  = 1
	maxSegmentSize = 16 * 1024 * 1024

	segDirName  = "segments"
	snapDirName = "snapshots"
	snapName    = "snap"
	lockName    = "lock"
)

var (
	errLockBusy = errors.New("ledger is already in use by another process")

	// ErrStructural is returned when ledger content is damaged beyond the
	// tolerated torn tail of the last segment. The ledger directory is left
	// untouched.
	ErrStructural = errors.New("ledger structurally corrupted")
	// ErrFailed is returned after an earlier persistence failure has rendered
	// the store unusable for the remaining lifetime of the process.
	ErrFailed = errors.New("ledger persistence failed")
	// ErrClosed is returned when operating on a closed store.
	ErrClosed = errors.New("ledger already closed")
)

func structural(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrStructural, fmt.Sprintf(format, args...))
}

// Store is an opened ledger. All methods are safe for concurrent use.
type Store struct {
	dir  string
	lock *os.File

	mu      sync.Mutex
	records map[restic.ID]Record
	nextSeq uint64

	// seg is the active append segment; it is opened lazily by the first
	// mutation and rotated once it exceeds maxSegmentSize.
	seg     *os.File
	segName string
	segSize int64

	// fsyncFileImpl is a field so that tests can inject fsync failures.
	fsyncFileImpl func(*os.File) error

	closed bool
	failed bool
}

// Open opens (initializing if necessary) the ledger in dir and acquires an
// exclusive local lock. The returned Store must be Closed.
func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("ledger directory must not be empty")
	}
	if err := os.MkdirAll(filepath.Join(dir, segDirName), 0o700); err != nil {
		return nil, fmt.Errorf("create ledger directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, snapDirName), 0o700); err != nil {
		return nil, fmt.Errorf("create ledger directory: %w", err)
	}

	lock, err := os.OpenFile(filepath.Join(dir, lockName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open ledger lock: %w", err)
	}
	if err := lockFile(lock); err != nil {
		_ = lock.Close()
		return nil, err
	}

	s := &Store{
		dir:           dir,
		lock:          lock,
		fsyncFileImpl: fsyncFile,
	}
	if err := s.load(); err != nil {
		_ = s.closeLocked()
		return nil, err
	}
	return s, nil
}

func (s *Store) segmentDir() string {
	return filepath.Join(s.dir, segDirName)
}

func (s *Store) snapshotPath() string {
	return filepath.Join(s.dir, snapDirName, snapName)
}

type wireSnapshot struct {
	Version int          `json:"version"`
	NextSeq uint64       `json:"next_seq"`
	Records []wireRecord `json:"records"`
}

func readSnapshot(path string) (map[restic.ID]Record, uint64, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return make(map[restic.ID]Record), 1, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("read ledger snapshot: %w", err)
	}

	var ws wireSnapshot
	if err := json.Unmarshal(data, &ws); err != nil {
		return nil, 0, structural("snapshot is not valid JSON: %v", err)
	}
	if ws.Version != formatVersion {
		return nil, 0, structural("unknown snapshot version %d", ws.Version)
	}
	if ws.NextSeq == 0 {
		return nil, 0, structural("snapshot has invalid sequence")
	}

	records := make(map[restic.ID]Record, len(ws.Records))
	for _, wr := range ws.Records {
		rec, err := decodeRecord(wr)
		if err != nil {
			return nil, 0, structural("snapshot record: %v", err)
		}
		if rec.Stage == StageNone || rec.Stage == StageRemoved {
			return nil, 0, structural("snapshot record for %v has invalid stage", rec.PackID)
		}
		if _, exists := records[rec.PackID]; exists {
			return nil, 0, structural("snapshot contains duplicate record for %v", rec.PackID)
		}
		records[rec.PackID] = rec
	}
	return records, ws.NextSeq, nil
}

// listSegmentNames returns the parseable segment file names sorted by their
// embedded first-frame sequence.
func listSegmentNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("list ledger segments: %w", err)
	}
	var names []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || len(name) != 20 {
			return nil, structural("unexpected file %q in segments directory", name)
		}
		if _, err := strconv.ParseUint(name, 10, 64); err != nil {
			return nil, structural("unexpected file %q in segments directory", name)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (s *Store) load() error {
	records, nextSeq, err := readSnapshot(s.snapshotPath())
	if err != nil {
		return err
	}
	s.records = records

	names, err := listSegmentNames(s.segmentDir())
	if err != nil {
		return err
	}

	for idx, name := range names {
		path := filepath.Join(s.segmentDir(), name)
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read ledger segment %s: %w", name, err)
		}

		off := 0
		isLast := idx == len(names)-1
		for off < len(data) {
			df, n, derr := decodeFrame(data[off:])
			if derr != nil {
				// Only the tail of the last segment may be torn, by a crash
				// while appending. Any corruption elsewhere is structural.
				if !isLast {
					return structural("invalid frame in segment %q at offset %d", name, off)
				}
				if err := truncateTail(path, off); err != nil {
					return err
				}
				debug.Log("truncated torn tail of ledger segment %q at offset %d", name, off)
				goto replayDone
			}

			if df.seq < nextSeq {
				// frame already incorporated into a newer snapshot
				off += n
				continue
			}
			if df.seq != nextSeq {
				return structural("frame sequence %d does not continue at %d in segment %q", df.seq, nextSeq, name)
			}
			s.applyFrame(df)
			nextSeq++
			off += n
		}
	}

replayDone:
	s.nextSeq = nextSeq
	return s.openActiveSegment(names)
}

func (s *Store) applyFrame(df decodedFrame) {
	switch df.op {
	case opUpsert:
		s.records[df.rec.PackID] = *df.rec
	case opDelete:
		id := restic.ID(df.del)
		delete(s.records, id)
	}
}

// truncateTail truncates path to keep exactly the first keep bytes and
// persists the truncation, so a torn tail cannot be replayed repeatedly.
func truncateTail(path string, keep int) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return structural("cannot open torn segment for truncation: %v", err)
	}
	if err := f.Truncate(int64(keep)); err != nil {
		_ = f.Close()
		return structural("cannot truncate torn segment: %v", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return structural("cannot sync truncated segment: %v", err)
	}
	if err := f.Close(); err != nil {
		return structural("cannot close truncated segment: %v", err)
	}
	return nil
}

// openActiveSegment reopens the last existing segment for appending.
func (s *Store) openActiveSegment(existing []string) error {
	if len(existing) == 0 {
		return nil
	}
	name := existing[len(existing)-1]
	path := filepath.Join(s.segmentDir(), name)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return fmt.Errorf("open active ledger segment: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("stat active ledger segment: %w", err)
	}
	s.seg = f
	s.segName = name
	s.segSize = fi.Size()
	return nil
}

type pendingChange struct {
	del bool
	id  restic.ID
	rec Record
}

// Apply persists all upserts and deletes as a single fsynced batch. The
// in-memory state is updated only after the batch has been durably written.
// On a persistence error the store is marked failed and every later mutation
// returns ErrFailed.
func (s *Store) Apply(upserts []Record, deletes []restic.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}
	if s.failed {
		return ErrFailed
	}
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	changes := make([]pendingChange, 0, len(upserts)+len(deletes))
	var batch bytes.Buffer
	firstSeq := s.nextSeq
	seq := firstSeq

	for i := range upserts {
		rec := upserts[i]
		if rec.Stage == StageNone || rec.Stage == StageRemoved {
			return fmt.Errorf("ledger record for %v has invalid stage", rec.PackID)
		}
		if rec.UpdatedAt.IsZero() {
			rec.UpdatedAt = time.Now().UTC()
		}
		wr := encodeRecord(rec)
		payload, err := json.Marshal(wireFrame{Seq: seq, Op: opUpsert, Rec: &wr})
		if err != nil {
			return fmt.Errorf("encode ledger frame: %w", err)
		}
		frame, err := encodeFrame(seq, opUpsert, payload)
		if err != nil {
			return err
		}
		batch.Write(frame)
		changes = append(changes, pendingChange{rec: rec})
		seq++
	}

	for _, id := range deletes {
		payload, err := json.Marshal(wireFrame{Seq: seq, Op: opDelete, Del: id.String()})
		if err != nil {
			return fmt.Errorf("encode ledger frame: %w", err)
		}
		frame, err := encodeFrame(seq, opDelete, payload)
		if err != nil {
			return err
		}
		batch.Write(frame)
		changes = append(changes, pendingChange{del: true, id: id})
		seq++
	}

	if s.seg == nil || s.segSize+int64(batch.Len()) > maxSegmentSize {
		if err := s.rotateSegment(firstSeq); err != nil {
			s.fail()
			return err
		}
	}

	n, err := s.seg.Write(batch.Bytes())
	if err != nil {
		s.fail()
		return fmt.Errorf("append ledger segment: %w", err)
	}
	if err := s.fsyncFileImpl(s.seg); err != nil {
		s.fail()
		return fmt.Errorf("sync ledger segment: %w", err)
	}

	for _, ch := range changes {
		if ch.del {
			delete(s.records, ch.id)
		} else {
			s.records[ch.rec.PackID] = ch.rec
		}
	}
	s.nextSeq = seq
	s.segSize += int64(n)
	return nil
}

// rotateSegment closes the active segment (if any) and creates a new empty one
// whose name is the sequence number of its first frame.
func (s *Store) rotateSegment(firstSeq uint64) error {
	replaced := ""
	if s.seg != nil {
		replaced = s.segName
		if err := s.seg.Close(); err != nil {
			return fmt.Errorf("close ledger segment: %w", err)
		}
		s.seg = nil
		s.segName = ""
		s.segSize = 0
	}

	name := fmt.Sprintf("%020d", firstSeq)
	path := filepath.Join(s.segmentDir(), name)
	if replaced == name {
		// reopening an empty segment that never received a durable frame: the
		// old file carries the same name and must be replaced.
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("replace empty ledger segment: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("create ledger segment: %w", err)
	}
	// make the new directory entry durable before frames are appended
	if err := syncDir(s.segmentDir()); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync ledger segment directory: %w", err)
	}
	s.seg = f
	s.segName = name
	return nil
}

// Put persists a single record.
func (s *Store) Put(rec Record) error {
	return s.Apply([]Record{rec}, nil)
}

// Delete persists the removal of a record.
func (s *Store) Delete(id restic.ID) error {
	return s.Apply(nil, []restic.ID{id})
}

// Compact rewrites the complete current state into a new snapshot and removes
// superseded segment files. It is safe to call repeatedly; failures mark the
// store as failed.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.failed {
		return ErrFailed
	}

	records := s.snapshotRecordsLocked()
	err := writeFileAtomicFn(s.snapshotPath(), func(w io.Writer) error {
		// stream the snapshot to avoid materializing the whole ledger in JSON
		if _, err := fmt.Fprintf(w, `{"version":%d,"next_seq":%d,"records":[`, formatVersion, s.nextSeq); err != nil {
			return err
		}
		for i, rec := range records {
			if i > 0 {
				if _, err := io.WriteString(w, ","); err != nil {
					return err
				}
			}
			data, err := json.Marshal(encodeRecord(rec))
			if err != nil {
				return fmt.Errorf("encode ledger snapshot record: %w", err)
			}
			if _, err := w.Write(data); err != nil {
				return err
			}
		}
		_, err := io.WriteString(w, "]}")
		return err
	})
	if err != nil {
		s.fail()
		return fmt.Errorf("write ledger snapshot: %w", err)
	}

	if s.seg != nil {
		_ = s.seg.Close()
		s.seg = nil
		s.segName = ""
		s.segSize = 0
	}
	names, err := listSegmentNames(s.segmentDir())
	if err != nil {
		return err
	}
	for _, name := range names {
		err := os.Remove(filepath.Join(s.segmentDir(), name))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			// a leftover segment is harmless: replay ignores frames below the
			// snapshot sequence and it will be removed by the next compaction.
			debug.Log("failed to remove obsolete ledger segment %v: %v", name, err)
		}
	}
	_ = syncDir(s.segmentDir())
	return nil
}

func (s *Store) fail() {
	s.failed = true
	if s.seg != nil {
		_ = s.seg.Close()
		s.seg = nil
	}
}

// Get returns the current record for a pack.
func (s *Store) Get(id restic.ID) (Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[id]
	return rec, ok
}

// Len returns the number of records currently stored.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

// Records returns all records ordered by pack ID.
func (s *Store) Records() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotRecordsLocked()
}

func (s *Store) snapshotRecordsLocked() []Record {
	records := make([]Record, 0, len(s.records))
	for _, rec := range s.records {
		records = append(records, rec)
	}
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].PackID[:], records[j].PackID[:]) < 0
	})
	return records
}

// SegmentBytes reports the total size of the append-only segment files.
func (s *Store) SegmentBytes() (int64, error) {
	names, err := listSegmentNames(s.segmentDir())
	if err != nil {
		return 0, err
	}
	var total int64
	for _, name := range names {
		fi, err := os.Stat(filepath.Join(s.segmentDir(), name))
		if err != nil {
			return 0, err
		}
		total += fi.Size()
	}
	return total, nil
}

// Dir returns the ledger directory.
func (s *Store) Dir() string {
	return s.dir
}

// Close flushes the OS handles and releases the local lock.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeLocked()
}

func (s *Store) closeLocked() error {
	if s.closed {
		return nil
	}
	s.closed = true
	var errs []error
	if s.seg != nil {
		errs = append(errs, s.seg.Close())
		s.seg = nil
	}
	if s.lock != nil {
		errs = append(errs, unlockFile(s.lock))
		errs = append(errs, s.lock.Close())
		s.lock = nil
	}
	return errors.Join(errs...)
}
