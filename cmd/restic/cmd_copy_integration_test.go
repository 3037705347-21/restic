package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/restic/restic/internal/copier"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
)

func testRunCopy(t testing.TB, srcGopts global.Options, dstGopts global.Options) {
	gopts := srcGopts
	gopts.Repo = dstGopts.Repo
	gopts.Password = dstGopts.Password
	gopts.InsecureNoPassword = dstGopts.InsecureNoPassword
	copyOpts := CopyOptions{
		SecondaryRepoOptions: global.SecondaryRepoOptions{
			Repo:               srcGopts.Repo,
			Password:           srcGopts.Password,
			InsecureNoPassword: srcGopts.InsecureNoPassword,
		},
	}

	rtest.OK(t, withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runCopy(context.TODO(), copyOpts, gopts, nil, gopts.Term)
	}))
}

func TestCopy(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env2, cleanup2 := withTestEnvironment(t)
	defer cleanup2()

	testSetupBackupData(t, env)
	opts := BackupOptions{}
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, opts, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, opts, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "3")}, opts, env.gopts)
	testRunCheck(t, env.gopts)

	testRunInit(t, env2.gopts)
	testRunCopy(t, env.gopts, env2.gopts)

	snapshotIDs := testListSnapshots(t, env.gopts, 3)
	copiedSnapshotIDs := testListSnapshots(t, env2.gopts, 3)

	// Check that the copies size seems reasonable
	stat := dirStats(t, env.repo)
	stat2 := dirStats(t, env2.repo)
	sizeDiff := int64(stat.size) - int64(stat2.size)
	if sizeDiff < 0 {
		sizeDiff = -sizeDiff
	}
	rtest.Assert(t, sizeDiff < int64(stat.size)/50, "expected less than 2%% size difference: %v vs. %v",
		stat.size, stat2.size)

	// Check integrity of the copy
	testRunCheck(t, env2.gopts)

	// Check that the copied snapshots have the same tree contents as the old ones (= identical tree hash)
	origRestores := make(map[string]struct{})
	for i, snapshotID := range snapshotIDs {
		restoredir := filepath.Join(env.base, fmt.Sprintf("restore%d", i))
		origRestores[restoredir] = struct{}{}
		testRunRestore(t, env.gopts, restoredir, snapshotID.String())
	}
	for i, snapshotID := range copiedSnapshotIDs {
		restoredir := filepath.Join(env2.base, fmt.Sprintf("restore%d", i))
		testRunRestore(t, env2.gopts, restoredir, snapshotID.String())
		foundMatch := false
		for cmpdir := range origRestores {
			diff := directoriesContentsDiff(t, restoredir, cmpdir)
			if diff == "" {
				delete(origRestores, cmpdir)
				foundMatch = true
			}
		}

		rtest.Assert(t, foundMatch, "found no counterpart for snapshot %v", snapshotID)
	}

	rtest.Assert(t, len(origRestores) == 0, "found not copied snapshots")

	// check that snapshots were properly batched while copying
	_, _, countBlobs := testPackAndBlobCounts(t, env.gopts)
	countTreePacksDst, countDataPacksDst, countBlobsDst := testPackAndBlobCounts(t, env2.gopts)

	rtest.Equals(t, countBlobs, countBlobsDst, "expected blob count in both repos to be equal")
	rtest.Equals(t, countTreePacksDst, 1, "expected 1 tree packfile")
	rtest.Equals(t, countDataPacksDst, 1, "expected 1 data packfile")
}

func testPackAndBlobCounts(t testing.TB, gopts global.Options) (countTreePacks int, countDataPacks int, countBlobs int) {
	rtest.OK(t, withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, gopts.Term)
		_, repo, unlock, err := openWithReadLock(ctx, gopts, false, printer)
		rtest.OK(t, err)
		defer unlock()

		rtest.OK(t, repo.List(context.TODO(), restic.PackFile, func(id restic.ID, size int64) error {
			handles, err := repo.ListPackHandles(context.TODO(), id, size)
			rtest.OK(t, err)
			rtest.Assert(t, len(handles) > 0, "a packfile should contain at least one blob")

			switch handles[0].Type {
			case restic.TreeBlob:
				countTreePacks++
			case restic.DataBlob:
				countDataPacks++
			}
			countBlobs += len(handles)
			return nil
		}))
		return nil
	}))

	return countTreePacks, countDataPacks, countBlobs
}

func TestCopyIncremental(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env2, cleanup2 := withTestEnvironment(t)
	defer cleanup2()

	testSetupBackupData(t, env)
	opts := BackupOptions{}
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, opts, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, opts, env.gopts)
	testRunCheck(t, env.gopts)

	testRunInit(t, env2.gopts)
	testRunCopy(t, env.gopts, env2.gopts)

	testListSnapshots(t, env.gopts, 2)
	testListSnapshots(t, env2.gopts, 2)

	// Check that the copies size seems reasonable
	testRunCheck(t, env2.gopts)

	// check that no snapshots are copied, as there are no new ones
	testRunCopy(t, env.gopts, env2.gopts)
	testRunCheck(t, env2.gopts)
	testListSnapshots(t, env2.gopts, 2)

	// check that only new snapshots are copied
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "3")}, opts, env.gopts)
	testRunCopy(t, env.gopts, env2.gopts)
	testRunCheck(t, env2.gopts)
	testListSnapshots(t, env.gopts, 3)
	testListSnapshots(t, env2.gopts, 3)

	// also test the reverse direction
	testRunCopy(t, env2.gopts, env.gopts)
	testRunCheck(t, env.gopts)
	testListSnapshots(t, env.gopts, 3)
}

func TestCopyUnstableJSON(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env2, cleanup2 := withTestEnvironment(t)
	defer cleanup2()

	// contains a symlink created using `ln -s '../i/'$'\355\246\361''d/samba' broken-symlink`
	datafile := filepath.Join("testdata", "copy-unstable-json.tar.gz")
	rtest.SetupTarTestFixture(t, env.base, datafile)

	testRunInit(t, env2.gopts)
	testRunCopy(t, env.gopts, env2.gopts)
	testRunCheck(t, env2.gopts)
	testListSnapshots(t, env2.gopts, 1)
}

func TestCopyToEmptyPassword(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env2, cleanup2 := withTestEnvironment(t)
	defer cleanup2()
	env2.gopts.Password = ""
	env2.gopts.InsecureNoPassword = true

	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, BackupOptions{}, env.gopts)

	testRunInit(t, env2.gopts)
	testRunCopy(t, env.gopts, env2.gopts)

	testListSnapshots(t, env.gopts, 1)
	testListSnapshots(t, env2.gopts, 1)
	testRunCheck(t, env2.gopts)
}

// withAppendLockedRepo runs fn with the repository opened using an append lock.
func withAppendLockedRepo(t testing.TB, gopts global.Options,
	fn func(ctx context.Context, repo restic.Repository),
) {
	t.Helper()
	rtest.OK(t, withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, gopts.Term)
		_, repo, unlock, err := openWithAppendLock(ctx, gopts, false, printer)
		rtest.OK(t, err)
		defer unlock()
		fn(ctx, repo)
		return nil
	}))
}

func testLoadSnapshots(t testing.TB, gopts global.Options) []*data.Snapshot {
	t.Helper()
	var snapshots []*data.Snapshot
	withAppendLockedRepo(t, gopts, func(ctx context.Context, repo restic.Repository) {
		lister, err := restic.MemorizeList(ctx, repo, restic.SnapshotFile)
		rtest.OK(t, err)
		rtest.OK(t, lister.List(ctx, restic.SnapshotFile, func(id restic.ID, _ int64) error {
			sn, err := data.LoadSnapshot(ctx, repo, id)
			rtest.OK(t, err)
			snapshots = append(snapshots, sn)
			return nil
		}))
	})
	return snapshots
}

func testCopyStateFileCount(t testing.TB, gopts global.Options) int {
	t.Helper()
	count := 0
	withAppendLockedRepo(t, gopts, func(ctx context.Context, repo restic.Repository) {
		rtest.OK(t, repo.List(ctx, restic.CopyStateFile, func(restic.ID, int64) error {
			count++
			return nil
		}))
	})
	return count
}

// TestCopyLedgerResume verifies that successful copies are recorded in the
// destination repository and that a rerun neither transfers data nor creates
// new snapshots or pack files.
func TestCopyLedgerResume(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env2, cleanup2 := withTestEnvironment(t)
	defer cleanup2()

	testSetupBackupData(t, env)
	opts := BackupOptions{}
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, opts, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, opts, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "3")}, opts, env.gopts)

	testRunInit(t, env2.gopts)
	testRunCopy(t, env.gopts, env2.gopts)
	testRunCheck(t, env2.gopts)

	srcIDs := testListSnapshots(t, env.gopts, 3)
	dstIDs := testListSnapshots(t, env2.gopts, 3)
	dstIDSet := restic.NewIDSet(dstIDs...)

	// the journal was consolidated into a single state file
	rtest.Equals(t, 1, testCopyStateFileCount(t, env2.gopts),
		"expected a single consolidated copy state file")

	// every source snapshot is recorded as complete with an existing target
	withAppendLockedRepo(t, env2.gopts, func(ctx context.Context, repo restic.Repository) {
		ledger, _, err := copier.LoadLedger(ctx, repo)
		rtest.OK(t, err)
		for _, id := range srcIDs {
			entry, ok := ledger.Get(id)
			rtest.Assert(t, ok, "missing ledger entry for source snapshot %s", id.Str())
			rtest.Equals(t, copier.StageComplete, entry.Stage)
			rtest.Assert(t, entry.TargetID != nil && dstIDSet.Has(*entry.TargetID),
				"complete ledger entry does not point to an existing destination snapshot")
		}
	})

	_, dataPacksBefore, blobsBefore := testPackAndBlobCounts(t, env2.gopts)

	// a rerun must not transfer anything or add snapshots
	testRunCopy(t, env.gopts, env2.gopts)
	testRunCheck(t, env2.gopts)
	testListSnapshots(t, env2.gopts, 3)

	_, dataPacksAfter, blobsAfter := testPackAndBlobCounts(t, env2.gopts)
	rtest.Equals(t, dataPacksBefore, dataPacksAfter, "rerun added data pack files")
	rtest.Equals(t, blobsBefore, blobsAfter, "rerun changed the number of blobs in the destination")
}

// TestCopyManualSnapshotsKept verifies that snapshots created directly in the
// destination repository are not modified or removed by copy.
func TestCopyManualSnapshotsKept(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env2, cleanup2 := withTestEnvironment(t)
	defer cleanup2()

	testSetupBackupData(t, env)
	opts := BackupOptions{}
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, opts, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, opts, env.gopts)

	testRunInit(t, env2.gopts)
	// create a snapshot directly in the destination repository
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "3")}, opts, env2.gopts)
	manualIDs := testListSnapshots(t, env2.gopts, 1)

	testRunCopy(t, env.gopts, env2.gopts)
	testRunCheck(t, env2.gopts)

	allDst := testListSnapshots(t, env2.gopts, 3)
	allDstSet := restic.NewIDSet(allDst...)
	rtest.Assert(t, allDstSet.Has(manualIDs[0]),
		"manually created destination snapshot %s was modified or removed", manualIDs[0].Str())

	// a rerun must leave the destination unchanged
	testRunCopy(t, env.gopts, env2.gopts)
	testRunCheck(t, env2.gopts)
	allDst2 := testListSnapshots(t, env2.gopts, 3)
	rtest.Equals(t, 3, len(allDst2))
	rtest.Assert(t, restic.NewIDSet(allDst2...).Has(manualIDs[0]),
		"manually created destination snapshot %s was modified or removed on rerun", manualIDs[0].Str())
}

// simulateInterruptedCopy rewrites the destination ledger for one copied
// snapshot to the given stage and removes its destination snapshot file. It
// models the state left behind by a copy run interrupted between durable
// progress boundaries.
func simulateInterruptedCopy(t testing.TB, gopts global.Options, srcID restic.ID,
	stage copier.Stage, deleteTarget bool,
) restic.ID {
	t.Helper()
	var removedTarget restic.ID
	withAppendLockedRepo(t, gopts, func(ctx context.Context, repo restic.Repository) {
		ledger, knownFiles, err := copier.LoadLedger(ctx, repo)
		rtest.OK(t, err)
		entry, ok := ledger.Get(srcID)
		rtest.Assert(t, ok, "expected a ledger entry for source snapshot %s", srcID.Str())
		rtest.Assert(t, entry.TargetID != nil, "expected a target snapshot id in ledger entry")
		removedTarget = *entry.TargetID

		if deleteTarget {
			rtest.OK(t, repo.RemoveUnpacked(ctx, restic.WriteableSnapshotFile, removedTarget))
		}

		// replace the journal with a rewritten state
		for id := range knownFiles {
			rtest.OK(t, repo.RemoveUnpacked(ctx, restic.WriteableCopyStateFile, id))
		}
		fresh := copier.New()
		for _, current := range ledger.Snapshots {
			target := current.TargetID
			if current.SourceID.Equal(srcID) {
				if stage == copier.StageDataCopied {
					target = nil
				}
				rtest.OK(t, fresh.Mark(srcID, current.OriginalID, stage, target))
				continue
			}
			rtest.OK(t, fresh.Mark(current.SourceID, current.OriginalID, copier.StageComplete, target))
		}
		_, err = fresh.Save(ctx, repo)
		rtest.OK(t, err)
	})
	return removedTarget
}

func pickCopiedSnapshot(t testing.TB, gopts global.Options, used restic.IDSet) restic.ID {
	t.Helper()
	for _, sn := range testLoadSnapshots(t, gopts) {
		if sn.Original == nil || used.Has(*sn.Original) {
			continue
		}
		used.Insert(*sn.Original)
		return *sn.Original
	}
	t.Fatal("no copied snapshot found")
	return restic.ID{}
}

// TestCopyInterruptedResume verifies that an interrupted run is resumed
// without retransferring data and that a "complete" record whose destination
// snapshot vanished is repaired.
func TestCopyInterruptedResume(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env2, cleanup2 := withTestEnvironment(t)
	defer cleanup2()

	testSetupBackupData(t, env)
	opts := BackupOptions{}
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, opts, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, opts, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "3")}, opts, env.gopts)

	testRunInit(t, env2.gopts)
	testRunCopy(t, env.gopts, env2.gopts)
	testRunCheck(t, env2.gopts)
	testListSnapshots(t, env2.gopts, 3)

	used := restic.NewIDSet()

	// case 1: data was copied, snapshot metadata was never saved
	dataID := pickCopiedSnapshot(t, env2.gopts, used)
	simulateInterruptedCopy(t, env2.gopts, dataID, copier.StageDataCopied, true)
	testListSnapshots(t, env2.gopts, 2)

	_, dataPacksBefore, blobsBefore := testPackAndBlobCounts(t, env2.gopts)
	testRunCopy(t, env.gopts, env2.gopts)
	_, dataPacksAfter, blobsAfter := testPackAndBlobCounts(t, env2.gopts)
	rtest.Equals(t, dataPacksBefore, dataPacksAfter, "resume must not retransfer data pack files")
	rtest.Equals(t, blobsBefore, blobsAfter, "resume must not change the number of blobs")
	testRunCheck(t, env2.gopts)
	testListSnapshots(t, env2.gopts, 3)

	// case 2: a complete record points to a snapshot that was deleted manually
	completeID := pickCopiedSnapshot(t, env2.gopts, used)
	removed := simulateInterruptedCopy(t, env2.gopts, completeID, copier.StageComplete, true)
	testListSnapshots(t, env2.gopts, 2)

	testRunCopy(t, env.gopts, env2.gopts)
	testRunCheck(t, env2.gopts)
	restored := testListSnapshots(t, env2.gopts, 3)
	rtest.Assert(t, !restic.NewIDSet(restored...).Has(removed),
		"deleted snapshot was not replaced with a new copy")
}

// TestCopyLedgerWriteFails verifies that the copy command fails when the
// progress records cannot be written to the destination.
func TestCopyLedgerWriteFails(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env2, cleanup2 := withTestEnvironment(t)
	defer cleanup2()

	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, BackupOptions{}, env.gopts)

	testRunInit(t, env2.gopts)

	// make the copy-state directory readable but not writable
	stateDir := filepath.Join(env2.repo, "copy-state")
	rtest.OK(t, os.Chmod(stateDir, 0o555))
	defer func() { _ = os.Chmod(stateDir, 0o755) }()

	// if we cannot actually deny write access (e.g. running as root), skip
	probe := filepath.Join(stateDir, "write-probe")
	if err := os.WriteFile(probe, []byte("x"), 0o644); err == nil {
		_ = os.Remove(probe)
		t.Skip("cannot make copy-state directory read-only")
	}

	gopts := env.gopts
	gopts.Repo = env2.gopts.Repo
	gopts.Password = env2.gopts.Password
	gopts.InsecureNoPassword = env2.gopts.InsecureNoPassword
	copyOpts := CopyOptions{
		SecondaryRepoOptions: global.SecondaryRepoOptions{
			Repo:               env.gopts.Repo,
			Password:           env.gopts.Password,
			InsecureNoPassword: env.gopts.InsecureNoPassword,
		},
	}
	err := withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runCopy(context.TODO(), copyOpts, gopts, nil, gopts.Term)
	})
	rtest.Assert(t, err != nil, "copy must fail when the progress ledger cannot be written")

	// snapshot metadata must not have been saved without progress tracking
	testListSnapshots(t, env2.gopts, 0)
}
