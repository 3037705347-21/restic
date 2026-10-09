package copier

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/mem"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

var (
	errDiskFull   = errors.New("disk full")
	errPermission = errors.New("permission denied")
)

func testRepo(t testing.TB) (*repository.Repository, backend.Backend) {
	t.Helper()
	return repository.TestRepositoryWithBackend(t, mem.New(), 0, repository.Options{})
}

func TestLedgerLoadEmpty(t *testing.T) {
	repo, _ := testRepo(t)

	ledger, files, err := LoadLedger(context.TODO(), repo)
	rtest.OK(t, err)
	rtest.Assert(t, ledger.Empty(), "expected empty ledger")
	rtest.Equals(t, 0, len(files))
}

func TestLedgerSaveLoad(t *testing.T) {
	ctx := context.TODO()
	repo, _ := testRepo(t)

	srcID := restic.TestParseID("1111111111111111111111111111111111111111111111111111111111111111")
	origID := restic.TestParseID("2222222222222222222222222222222222222222222222222222222222222222")
	targetID := restic.TestParseID("3333333333333333333333333333333333333333333333333333333333333333")

	ledger := New()
	rtest.OK(t, ledger.Mark(srcID, origID, StageDataCopied, nil))
	_, err := ledger.Save(ctx, repo)
	rtest.OK(t, err)

	rtest.OK(t, ledger.Mark(srcID, origID, StageComplete, &targetID))
	_, err = ledger.Save(ctx, repo)
	rtest.OK(t, err)

	loaded, files, err := LoadLedger(ctx, repo)
	rtest.OK(t, err)
	rtest.Equals(t, 2, len(files))

	e, ok := loaded.Get(srcID)
	rtest.Assert(t, ok, "expected progress entry")
	rtest.Equals(t, StageComplete, e.Stage)
	rtest.Assert(t, e.OriginalID.Equal(origID), "unexpected original id")
	rtest.Assert(t, e.TargetID != nil && e.TargetID.Equal(targetID), "unexpected target id")

	rtest.Assert(t, loaded.IsComplete(srcID, func(id restic.ID) bool { return id.Equal(targetID) }),
		"expected snapshot to be complete")
	rtest.Assert(t, !loaded.IsComplete(srcID, func(restic.ID) bool { return false }),
		"missing target snapshot must not be considered complete")
}

// writeStateFile writes one journal file with explicit entries.
func writeStateFile(t testing.TB, ctx context.Context, repo *repository.Repository, entries ...SnapshotProgress) restic.ID {
	t.Helper()
	l := New()
	for _, e := range entries {
		l.Snapshots[e.SourceID.String()] = e
	}
	id, err := l.Save(ctx, repo)
	rtest.OK(t, err)
	return id
}

func TestLedgerMerge(t *testing.T) {
	ctx := context.TODO()
	repo, _ := testRepo(t)

	id1 := restic.TestParseID("1111111111111111111111111111111111111111111111111111111111111111")
	id2 := restic.TestParseID("2222222222222222222222222222222222222222222222222222222222222222")
	old := time.Now().Add(-time.Hour).UTC()
	recent := time.Now().UTC()

	// higher stage wins regardless of timestamps
	writeStateFile(t, ctx, repo, SnapshotProgress{SourceID: id1, Stage: StageComplete, UpdatedAt: old})
	writeStateFile(t, ctx, repo, SnapshotProgress{SourceID: id1, Stage: StageDataCopied, UpdatedAt: recent})
	// equal stage: newer timestamp wins, carrying its TargetID
	target := id2
	writeStateFile(t, ctx, repo,
		SnapshotProgress{SourceID: id2, Stage: StageDataCopied, UpdatedAt: old},
		SnapshotProgress{SourceID: id2, Stage: StageComplete, TargetID: &target, UpdatedAt: recent},
	)

	loaded, _, err := LoadLedger(ctx, repo)
	rtest.OK(t, err)

	e1, ok := loaded.Get(id1)
	rtest.Assert(t, ok, "missing entry")
	rtest.Equals(t, StageComplete, e1.Stage)

	e2, ok := loaded.Get(id2)
	rtest.Assert(t, ok, "missing entry")
	rtest.Equals(t, StageComplete, e2.Stage)
	rtest.Assert(t, e2.TargetID != nil && e2.TargetID.Equal(target), "expected newer entry to win")
}

func TestLedgerMarkRegression(t *testing.T) {
	ledger := New()
	id := restic.TestParseID("1111111111111111111111111111111111111111111111111111111111111111")

	rtest.OK(t, ledger.Mark(id, id, StageComplete, nil))
	rtest.Assert(t, ledger.Mark(id, id, StageDataCopied, nil) != nil,
		"regressing the progress stage must fail")
	rtest.Assert(t, ledger.Mark(restic.ID{}, restic.ID{}, StageComplete, nil) != nil,
		"recording an empty source id must fail")
}

func TestLedgerCorruptFileFails(t *testing.T) {
	ctx := context.TODO()
	repo, be := testRepo(t)

	ledger := New()
	id := restic.TestParseID("1111111111111111111111111111111111111111111111111111111111111111")
	rtest.OK(t, ledger.Mark(id, id, StageComplete, nil))
	_, err := ledger.Save(ctx, repo)
	rtest.OK(t, err)

	// add an unreadable file
	bogus := restic.TestParseID("9999999999999999999999999999999999999999999999999999999999999999")
	h := backend.Handle{Type: backend.CopyStateFile, Name: bogus.String()}
	rtest.OK(t, be.Save(ctx, h, backend.NewByteReader([]byte("not encrypted"), be.Hasher())))

	_, _, err = LoadLedger(ctx, repo)
	rtest.Assert(t, err != nil, "corrupt copy state file must be fatal, got nil error")
}

func TestLedgerUnsupportedVersionFails(t *testing.T) {
	ctx := context.TODO()
	repo, _ := testRepo(t)

	id := restic.TestParseID("1111111111111111111111111111111111111111111111111111111111111111")
	l := New()
	l.Version = 99
	l.Snapshots[id.String()] = SnapshotProgress{SourceID: id, Stage: StageComplete, UpdatedAt: time.Now()}
	_, err := l.Save(ctx, repo)
	rtest.OK(t, err)

	_, _, err = LoadLedger(ctx, repo)
	rtest.Assert(t, err != nil, "unsupported ledger version must be fatal, got nil error")
}

type failingSaveBackend struct {
	backend.Backend
	failType backend.FileType
}

func (b *failingSaveBackend) Save(ctx context.Context, h backend.Handle, rd backend.RewindReader) error {
	if h.Type == b.failType {
		return errDiskFull
	}
	return b.Backend.Save(ctx, h, rd)
}

func TestLedgerSaveFailure(t *testing.T) {
	ctx := context.TODO()
	be := &failingSaveBackend{Backend: mem.New(), failType: backend.CopyStateFile}
	repo, _ := repository.TestRepositoryWithBackend(t, be, 0, repository.Options{})

	ledger := New()
	id := restic.TestParseID("1111111111111111111111111111111111111111111111111111111111111111")
	rtest.OK(t, ledger.Mark(id, id, StageDataCopied, nil))
	_, err := ledger.Save(ctx, repo)
	rtest.Assert(t, err != nil, "failed progress writes must return an error")

	_, err = ledger.Compact(ctx, repo, restic.NewIDSet())
	rtest.Assert(t, err != nil, "compact must fail when the merged state cannot be written")
}

func countStateFiles(t testing.TB, ctx context.Context, be backend.Backend) int {
	t.Helper()
	count := 0
	rtest.OK(t, be.List(ctx, backend.CopyStateFile, func(backend.FileInfo) error {
		count++
		return nil
	}))
	return count
}

func TestLedgerCompact(t *testing.T) {
	ctx := context.TODO()
	repo, be := testRepo(t)

	id1 := restic.TestParseID("1111111111111111111111111111111111111111111111111111111111111111")
	id2 := restic.TestParseID("2222222222222222222222222222222222222222222222222222222222222222")

	ledger := New()
	rtest.OK(t, ledger.Mark(id1, id1, StageDataCopied, nil))
	_, err := ledger.Save(ctx, repo)
	rtest.OK(t, err)
	rtest.OK(t, ledger.Mark(id1, id1, StageComplete, nil))
	_, err = ledger.Save(ctx, repo)
	rtest.OK(t, err)
	rtest.OK(t, ledger.Mark(id2, id2, StageComplete, nil))
	_, err = ledger.Save(ctx, repo)
	rtest.OK(t, err)
	rtest.Equals(t, 3, countStateFiles(t, ctx, be))

	loaded, knownFiles, err := LoadLedger(ctx, repo)
	rtest.OK(t, err)
	rtest.Equals(t, 3, len(knownFiles))

	warnings, err := loaded.Compact(ctx, repo, knownFiles)
	rtest.OK(t, err)
	rtest.Equals(t, 0, len(warnings))
	rtest.Equals(t, 1, countStateFiles(t, ctx, be))

	// all entries survive compaction
	reloaded, _, err := LoadLedger(ctx, repo)
	rtest.OK(t, err)
	e1, ok := reloaded.Get(id1)
	rtest.Assert(t, ok && e1.Stage == StageComplete, "entry lost during compaction")
	_, ok = reloaded.Get(id2)
	rtest.Assert(t, ok, "entry lost during compaction")
}

func TestLedgerCompactKeepsConcurrentlyWrittenFile(t *testing.T) {
	ctx := context.TODO()
	repo, be := testRepo(t)

	id1 := restic.TestParseID("1111111111111111111111111111111111111111111111111111111111111111")
	id2 := restic.TestParseID("2222222222222222222222222222222222222222222222222222222222222222")

	ledger := New()
	rtest.OK(t, ledger.Mark(id1, id1, StageComplete, nil))
	_, err := ledger.Save(ctx, repo)
	rtest.OK(t, err)

	_, knownFiles, err := LoadLedger(ctx, repo)
	rtest.OK(t, err)

	// another writer appends a journal file after our listing
	extra := writeStateFile(t, ctx, repo, SnapshotProgress{SourceID: id2, Stage: StageComplete, UpdatedAt: time.Now()})

	warnings, err := ledger.Compact(ctx, repo, knownFiles)
	rtest.OK(t, err)
	rtest.Equals(t, 0, len(warnings))

	files := countStateFiles(t, ctx, be)
	rtest.Equals(t, 2, files, "concurrently written state file must not be removed")

	reloaded, _, err := LoadLedger(ctx, repo)
	rtest.OK(t, err)
	_, ok := reloaded.Get(id2)
	rtest.Assert(t, ok, "entry of concurrent writer lost")
	_ = extra
}

type failingRemoveBackend struct {
	backend.Backend
}

func (b *failingRemoveBackend) Remove(ctx context.Context, h backend.Handle) error {
	if h.Type == backend.CopyStateFile {
		return errPermission
	}
	return b.Backend.Remove(ctx, h)
}

func TestLedgerCompactRemoveFailure(t *testing.T) {
	ctx := context.TODO()
	be := &failingRemoveBackend{Backend: mem.New()}
	repo, _ := repository.TestRepositoryWithBackend(t, be, 0, repository.Options{})

	id := restic.TestParseID("1111111111111111111111111111111111111111111111111111111111111111")
	ledger := New()
	rtest.OK(t, ledger.Mark(id, id, StageDataCopied, nil))
	_, err := ledger.Save(ctx, repo)
	rtest.OK(t, err)

	_, knownFiles, err := LoadLedger(ctx, repo)
	rtest.OK(t, err)
	rtest.OK(t, ledger.Mark(id, id, StageComplete, nil))

	warnings, err := ledger.Compact(ctx, repo, knownFiles)
	rtest.OK(t, err) // writing the merged file must succeed
	rtest.Assert(t, len(warnings) > 0, "remove failures must be reported as warnings")

	// the journal entry is still intact despite the failed cleanup
	reloaded, _, err := LoadLedger(ctx, repo)
	rtest.OK(t, err)
	e, ok := reloaded.Get(id)
	rtest.Assert(t, ok && e.Stage == StageComplete, "progress entry lost after remove failure")
}
