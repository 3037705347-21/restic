package main

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
)

func testRunRecover(t testing.TB, gopts global.Options) {
	t.Helper()
	rtest.OK(t, testRunRecoverMayFail(t, gopts))
}

func testRunRecoverMayFail(t testing.TB, gopts global.Options) error {
	return withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runRecover(ctx, gopts, gopts.Term)
	})
}

// testLoadRecoveryRecord loads the recovery record via a read-only repository
// access. expectedFiles asserts the number of backing record files.
func testLoadRecoveryRecord(t testing.TB, gopts global.Options, expectedFiles int) *restic.RecoveryRecord {
	var record *restic.RecoveryRecord
	err := withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, gopts.Term)
		_, repo, unlock, err := openWithReadLock(ctx, gopts, false, printer)
		rtest.OK(t, err)
		defer unlock()

		records, fileIDs, err := restic.LoadRecoveryRecords(ctx, repo)
		if err != nil {
			return err
		}
		rtest.Assert(t, len(fileIDs) == expectedFiles,
			"expected %d recovery record files, got %d", expectedFiles, len(fileIDs))
		if len(records) > 0 {
			record, _, _ = selectRecoveryRecord(records)
		}
		return nil
	})
	rtest.OK(t, err)
	return record
}

// testCreateRecoverData creates a small directory with a single file of the
// given content.
func testCreateRecoverData(t testing.TB, path, content string) {
	rtest.OK(t, os.MkdirAll(path, 0700))
	rtest.OK(t, os.WriteFile(filepath.Join(path, "file"), []byte(content), 0600))
}

// testCreateSnapshotForRoot creates a regular (not tagged as recovered) snapshot
// referencing the given root tree, as if another user had created that snapshot.
func testCreateSnapshotForRoot(t testing.TB, gopts global.Options, root restic.ID) restic.ID {
	var snapshotID restic.ID
	err := withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, gopts.Term)
		_, repo, unlock, err := openWithExclusiveLock(ctx, gopts, false, printer)
		rtest.OK(t, err)
		defer unlock()

		sn := &data.Snapshot{
			Time:     time.Now(),
			Tree:     &root,
			Paths:    []string{"/some/user/path"},
			Hostname: "other-host",
		}
		snapshotID, err = data.SaveSnapshot(ctx, repo, sn)
		return err
	})
	rtest.OK(t, err)
	return snapshotID
}

// recoveredSnapshotIDs returns the IDs of snapshots tagged with the recover tag.
func recoveredSnapshotIDs(t testing.TB, gopts global.Options) restic.IDs {
	ids := testRunList(t, gopts, "snapshots")
	result := restic.IDs{}
	for _, id := range ids {
		sn := testLoadSnapshot(t, gopts, id)
		if sn.HasTags([]string{recoverTag}) {
			result = append(result, id)
		}
	}
	return result
}

// testRecoverRepo sets up a repository, backs up dataPath and removes the
// resulting snapshot, so that recover has something to work with.
func testRecoverRepo(t testing.TB) (env *testEnvironment, cleanup func(), dataPath string) {
	env, cleanup = withTestEnvironment(t)
	// recover lists the index twice (repair, then load), so it cannot use the
	// list-once test hook.
	env.gopts.BackendTestHook = nil
	testRunInit(t, env.gopts)

	dataPath = filepath.Join(env.base, "data")
	testCreateRecoverData(t, dataPath, "recover-test-content")

	testRunBackup(t, "", []string{dataPath}, BackupOptions{}, env.gopts)
	snapshots := testListSnapshots(t, env.gopts, 1)
	testRunForget(t, env.gopts, ForgetOptions{}, snapshots[0].String())
	testListSnapshots(t, env.gopts, 0)

	return env, cleanup, dataPath
}

func TestRecoverIdempotent(t *testing.T) {
	env, cleanup, _ := testRecoverRepo(t)
	defer cleanup()

	// first run: one recovery snapshot and a record
	testRunRecover(t, env.gopts)
	snapshots := testListSnapshots(t, env.gopts, 1)
	sn := testLoadSnapshot(t, env.gopts, snapshots[0])
	rtest.Assert(t, sn.HasTags([]string{recoverTag}), "expected snapshot tagged %q", recoverTag)

	record := testLoadRecoveryRecord(t, env.gopts, 1)
	rtest.Assert(t, record.Generation == 1, "expected generation 1, got %d", record.Generation)
	rtest.Assert(t, len(record.Roots) == 1, "expected one recorded root, got %d", len(record.Roots))
	root := record.Roots[0]

	// second run: no new snapshot, record untouched
	testRunRecover(t, env.gopts)
	testListSnapshots(t, env.gopts, 1)
	record = testLoadRecoveryRecord(t, env.gopts, 1)
	rtest.Assert(t, record.Generation == 1, "record must not be rewritten, got generation %d", record.Generation)
	rtest.Assert(t, record.Roots[0].String() == root.String(), "recorded root changed")

	// simulate an interruption after the snapshot was saved but before the
	// record persisted: rerun must not duplicate the snapshot
	rtest.OK(t, os.RemoveAll(filepath.Join(env.repo, "recovery")))
	testRunRecover(t, env.gopts)
	testListSnapshots(t, env.gopts, 1)
	record = testLoadRecoveryRecord(t, env.gopts, 1)
	rtest.Assert(t, len(record.Roots) == 1 && record.Roots[0].String() == root.String(),
		"record must be recreated with the same root")
}

func TestRecoverNewRootsOnly(t *testing.T) {
	env, cleanup, _ := testRecoverRepo(t)
	defer cleanup()

	testRunRecover(t, env.gopts)
	testListSnapshots(t, env.gopts, 1)
	record := testLoadRecoveryRecord(t, env.gopts, 1)
	rtest.Assert(t, len(record.Roots) == 1, "expected one recorded root")

	// a second, unrelated root appears
	dataPathB := filepath.Join(env.base, "data-b")
	testCreateRecoverData(t, dataPathB, "different-content")
	testRunBackup(t, "", []string{dataPathB}, BackupOptions{}, env.gopts)
	newSnapshots := testListSnapshots(t, env.gopts, 2)
	testRunForget(t, env.gopts, ForgetOptions{}, newSnapshots[1].String())

	// only the new root is recovered
	testRunRecover(t, env.gopts)
	testListSnapshots(t, env.gopts, 2)
	record = testLoadRecoveryRecord(t, env.gopts, 1)
	rtest.Assert(t, record.Generation == 2, "expected generation 2, got %d", record.Generation)
	rtest.Assert(t, len(record.Roots) == 2, "expected two recorded roots, got %d", len(record.Roots))

	// rerun: nothing happens
	testRunRecover(t, env.gopts)
	testListSnapshots(t, env.gopts, 2)
	record = testLoadRecoveryRecord(t, env.gopts, 1)
	rtest.Assert(t, record.Generation == 2, "record must not be rewritten")
}

func TestRecoverConvergence(t *testing.T) {
	env, cleanup, _ := testRecoverRepo(t)
	defer cleanup()

	testRunRecover(t, env.gopts)
	record := testLoadRecoveryRecord(t, env.gopts, 1)
	rtest.Assert(t, len(record.Roots) == 1, "expected one recorded root")

	// the recovered root gets referenced again by a regular (untagged) snapshot
	// that belongs to somebody else.
	rootID := record.Roots[0]
	userID := testCreateSnapshotForRoot(t, env.gopts, rootID)
	userSnapshot := testLoadSnapshot(t, env.gopts, userID)
	rtest.Assert(t, !userSnapshot.HasTags([]string{recoverTag}), "expected a regular snapshot")
	rtest.Assert(t, userSnapshot.Tree.String() == rootID.String(),
		"new snapshot must reference the recovered root")
	testListSnapshots(t, env.gopts, 2)

	// rerun: the record converges the root, snapshots are untouched
	testRunRecover(t, env.gopts)
	testListSnapshots(t, env.gopts, 2)
	rtest.Assert(t, testLoadRecoveryRecord(t, env.gopts, 0) == nil, "record must be converged away")

	// the regular snapshot disappears again (without prune). The original
	// recovery snapshot still references the root, so recover only recreates
	// the record without generating a duplicate snapshot.
	testRunForget(t, env.gopts, ForgetOptions{}, userID.String())
	testRunRecover(t, env.gopts)
	testListSnapshots(t, env.gopts, 1)
	record = testLoadRecoveryRecord(t, env.gopts, 1)
	rtest.Assert(t, len(record.Roots) == 1, "expected root to be recorded again")
}

// failRecoverySaveBackend injects a failure whenever a recovery record file is
// saved, while leaving all other backend operations untouched.
type failRecoverySaveBackend struct {
	backend.Backend
}

func (b *failRecoverySaveBackend) Save(ctx context.Context, h backend.Handle, rd backend.RewindReader) error {
	if h.Type == backend.RecoveryFile {
		return errors.New("injected recovery record write failure")
	}
	return b.Backend.Save(ctx, h, rd)
}

func TestRecoverRecordWriteFailure(t *testing.T) {
	env, cleanup, _ := testRecoverRepo(t)
	defer cleanup()

	failingGopts := env.gopts
	failingGopts.BackendTestHook = func(r backend.Backend) (backend.Backend, error) {
		return &failRecoverySaveBackend{Backend: r}, nil
	}

	// record write failure fails the whole command
	err := testRunRecoverMayFail(t, failingGopts)
	rtest.Assert(t, err != nil, "expected recover to fail")

	// the recovery snapshot has already been saved, but only one
	recovered := recoveredSnapshotIDs(t, env.gopts)
	rtest.Assert(t, len(recovered) == 1, "expected one recovery snapshot, got %d", len(recovered))

	// rerun with a working backend adopts the existing snapshot and writes
	// the record: no duplicate snapshot
	testRunRecover(t, env.gopts)
	recovered = recoveredSnapshotIDs(t, env.gopts)
	rtest.Assert(t, len(recovered) == 1, "expected one recovery snapshot, got %d", len(recovered))
	record := testLoadRecoveryRecord(t, env.gopts, 1)
	rtest.Assert(t, len(record.Roots) == 1, "expected one recorded root")
}

func TestRecoverNoUnreferencedData(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	env.gopts.BackendTestHook = nil
	testRunInit(t, env.gopts)
	dataPath := filepath.Join(env.base, "data")
	testCreateRecoverData(t, dataPath, "content")
	testRunBackup(t, "", []string{dataPath}, BackupOptions{}, env.gopts)

	// everything is referenced: no snapshot, no record
	testRunRecover(t, env.gopts)
	testListSnapshots(t, env.gopts, 1)
	rtest.Assert(t, testLoadRecoveryRecord(t, env.gopts, 0) == nil, "no record expected")

	// idempotent
	testRunRecover(t, env.gopts)
	testListSnapshots(t, env.gopts, 1)
}

func TestSelectRecoveryRecord(t *testing.T) {
	now := time.Now()
	older := now.Add(-time.Hour)
	newer := now.Add(time.Hour)

	records := []*restic.RecoveryRecord{
		restic.NewRecoveryRecord(2, older, older, restic.IDs{}),
		restic.NewRecoveryRecord(1, older, older, restic.IDs{}),
	}
	selected, generation, _ := selectRecoveryRecord(records)
	rtest.Assert(t, generation == 2 && selected.Generation == 2, "expected highest generation")

	// ties broken by update time
	records = []*restic.RecoveryRecord{
		restic.NewRecoveryRecord(1, older, newer, restic.IDs{}),
		restic.NewRecoveryRecord(1, older, older, restic.IDs{}),
	}
	selected, generation, _ = selectRecoveryRecord(records)
	rtest.Assert(t, generation == 1 && selected.Updated.Equal(newer), "expected newest update time")

	// sorted roots
	ids := restic.IDs{restic.NewRandomID(), restic.NewRandomID()}
	record := restic.NewRecoveryRecord(1, now, now, ids)
	rtest.Assert(t, sort.IsSorted(restic.IDs(record.Roots)), "roots must be sorted")
}
