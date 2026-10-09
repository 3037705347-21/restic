package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/inventory"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
)

func testRunInventory(t testing.TB, gopts global.Options, file string) (string, error) {
	t.Helper()
	buf, err := withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runInventory(ctx, InventoryOptions{File: file}, gopts, gopts.Term)
	})
	return buf.String(), err
}

func TestInventoryBaselineReconcileAndDrift(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env.gopts.Quiet = false
	env.gopts.Verbosity = 2

	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{env.testdata}, BackupOptions{}, env.gopts)

	// capture the backend of later opens so that the test can tamper with it
	getBackend := captureBackend(&env.gopts)

	ledgerFile := filepath.Join(env.base, "inventory.json")

	// the first run establishes a baseline
	out, err := testRunInventory(t, env.gopts, ledgerFile)
	rtest.OK(t, err)
	rtest.Assert(t, strings.Contains(out, "creating baseline"), "expected baseline message, got %q", out)

	baseline, err := inventory.Load(ledgerFile)
	rtest.OK(t, err)
	rtest.Assert(t, baseline.Count() > 0, "expected baseline to contain objects")
	rtest.Equals(t, 1, len(baseline.Objects["snapshot"]))
	rtest.Assert(t, baseline.RepoID != "", "expected repo ID to be recorded")

	// an identical run must not report any changes; lock create/remove during
	// the runs must not show up as removed objects
	out, err = testRunInventory(t, env.gopts, ledgerFile)
	rtest.OK(t, err)
	rtest.Assert(t, !strings.Contains(out, "removed object"), "unexpected removal report: %q", out)
	rtest.Assert(t, !strings.Contains(out, "replaced object"), "unexpected replacement report: %q", out)

	// a concurrent backup only adds objects, which must not be flagged
	testRunBackup(t, "", []string{env.testdata}, BackupOptions{}, env.gopts)
	out, err = testRunInventory(t, env.gopts, ledgerFile)
	rtest.OK(t, err)
	rtest.Assert(t, !strings.Contains(out, "removed object"), "new objects must not be reported as removals: %q", out)
	rtest.Assert(t, strings.Contains(out, "new objects"), "expected new-object count in output: %q", out)

	updated, err := inventory.Load(ledgerFile)
	rtest.OK(t, err)
	rtest.Assert(t, updated.Count() > baseline.Count(), "expected new objects in the updated inventory")
	rtest.Equals(t, 2, len(updated.Objects["snapshot"]))

	// simulate tampering: one pack is deleted and one snapshot is replaced
	// with different content of a different size
	var (
		removedPack      restic.ID
		replacedSnapshot restic.ID
		oldSnapshotSize  int64
		newSnapshotSize  int64
		replacementBytes []byte
	)
	rtest.OK(t, withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, gopts.Term)
		ctx, repo, unlock, err := openWithReadLock(ctx, gopts, false, printer)
		rtest.OK(t, err)
		defer unlock()

		be := getBackend()

		rtest.OK(t, repo.List(ctx, restic.PackFile, func(id restic.ID, _ int64) error {
			if removedPack.IsNull() {
				removedPack = id
			}
			return nil
		}))
		rtest.OK(t, be.Remove(ctx, backend.Handle{Type: backend.PackFile, Name: removedPack.String()}))

		rtest.OK(t, repo.List(ctx, restic.SnapshotFile, func(id restic.ID, size int64) error {
			if replacedSnapshot.IsNull() {
				replacedSnapshot = id
				oldSnapshotSize = size
			}
			return nil
		}))
		replacementBytes = bytes.Repeat([]byte("tampered"), int(oldSnapshotSize/9)+37)
		newSnapshotSize = int64(len(replacementBytes))
		rtest.Assert(t, newSnapshotSize != oldSnapshotSize, "replacement must have a different size")
		return be.Save(ctx, backend.Handle{Type: backend.SnapshotFile, Name: replacedSnapshot.String()},
			backend.NewByteReader(replacementBytes, nil))
	}))

	// the next run detects both changes and exits with the drift error
	out, err = testRunInventory(t, env.gopts, ledgerFile)
	rtest.Assert(t, errors.Is(err, ErrInventoryDrift), "expected drift error, got %v", err)
	rtest.Assert(t, strings.Contains(out, "removed object:"), "expected removal report: %q", out)
	rtest.Assert(t, strings.Contains(out, removedPack.String()), "removed pack ID missing from report: %q", out)
	rtest.Assert(t, strings.Contains(out, "replaced object:"), "expected replacement report: %q", out)
	rtest.Assert(t, strings.Contains(out, replacedSnapshot.String()), "replaced snapshot ID missing from report: %q", out)
	rtest.Assert(t, strings.Contains(out, "(size"), "expected size details in replacement report: %q", out)

	// a successful enumeration nevertheless updates the baseline atomically
	tampered, err := inventory.Load(ledgerFile)
	rtest.OK(t, err)
	for _, record := range tampered.Objects["data"] {
		rtest.Assert(t, record.ID != removedPack.String(), "removed pack must no longer be in the inventory")
	}
	found := false
	for _, record := range tampered.Objects["snapshot"] {
		if record.ID == replacedSnapshot.String() {
			found = true
			rtest.Equals(t, newSnapshotSize, record.Size)
		}
	}
	rtest.Assert(t, found, "replaced snapshot must still be recorded")
}

// failingListBackend makes listing a specific file type fail, simulating an
// incomplete backend enumeration.
type failingListBackend struct {
	backend.Backend
	failType backend.FileType
}

func (b *failingListBackend) List(ctx context.Context, t backend.FileType, fn func(backend.FileInfo) error) error {
	if t == b.failType {
		return errors.Errorf("simulated failure while listing %v", t)
	}
	return b.Backend.List(ctx, t, fn)
}

func TestInventoryEnumerationFailureKeepsBaseline(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{env.testdata}, BackupOptions{}, env.gopts)

	// use a passthrough hook instead of the list-once test backend
	captureBackend(&env.gopts)

	ledgerFile := filepath.Join(env.base, "inventory.json")

	_, err := testRunInventory(t, env.gopts, ledgerFile)
	rtest.OK(t, err)
	original, err := os.ReadFile(ledgerFile)
	rtest.OK(t, err)

	// make the enumeration fail partway through and verify the baseline is
	// not touched
	env.gopts.BackendTestHook = func(r backend.Backend) (backend.Backend, error) {
		return &failingListBackend{Backend: r, failType: backend.PackFile}, nil
	}

	_, err = testRunInventory(t, env.gopts, ledgerFile)
	rtest.Assert(t, err != nil, "expected enumeration failure")
	rtest.Assert(t, !errors.Is(err, ErrInventoryDrift), "enumeration failure must not be mistaken for drift")

	after, err := os.ReadFile(ledgerFile)
	rtest.OK(t, err)
	rtest.Equals(t, original, after)
}

func TestInventoryJSONOutput(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	// use a passthrough hook instead of the list-once test backend
	captureBackend(&env.gopts)

	ledgerFile := filepath.Join(env.base, "inventory.json")

	gopts := env.gopts
	buf, runErr := withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		gopts.JSON = true
		return runInventory(ctx, InventoryOptions{File: ledgerFile}, gopts, gopts.Term)
	})
	rtest.OK(t, runErr)

	var status inventoryStatus
	rtest.OK(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &status))
	rtest.Equals(t, "inventory", status.MessageType)
	rtest.Assert(t, status.Created, "first run must be reported as a new baseline")
	rtest.Assert(t, status.ObjectCount > 0, "expected objects in baseline")

	testRunBackup(t, "", []string{env.testdata}, BackupOptions{}, env.gopts)

	buf, runErr = withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		gopts.JSON = true
		return runInventory(ctx, InventoryOptions{File: ledgerFile}, gopts, gopts.Term)
	})
	rtest.OK(t, runErr)

	rtest.OK(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &status))
	rtest.Assert(t, !status.Created, "second run must not be reported as a new baseline")
	rtest.Assert(t, status.BaselineGeneratedAt != nil, "expected baseline timestamp")
	rtest.Equals(t, 0, len(status.Removed))
	rtest.Equals(t, 0, len(status.Replaced))
	rtest.Assert(t, len(status.Added) > 0, "new backup objects must be listed as added")
}

func TestInventoryRequiresFile(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	env.gopts.NoLock = true
	t.Setenv("RESTIC_INVENTORY_FILE", "")

	err := withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		return runInventory(ctx, InventoryOptions{}, gopts, gopts.Term)
	})
	rtest.Assert(t, err != nil, "expected an error when no inventory file is given")
}
