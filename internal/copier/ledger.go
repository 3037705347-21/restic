// Package copier contains the progress ledger used by the `copy` command.
//
// The ledger records how far each source snapshot has been copied. It is stored
// as encrypted JSON files of type restic.CopyStateFile inside the *destination*
// repository, so a rerun after a network failure or other interruption can
// skip completed work and only finish what was left unfinished.
package copier

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
)

// ledgerVersion is the on-disk format version of a copy state file.
const ledgerVersion = 1

// Stage describes the copy progress of a single source snapshot.
// Stages are ordered and only advance forward.
type Stage string

const (
	// StageDataCopied means that all blobs of the snapshot have been copied
	// and the destination index has been flushed. A rerun does not have to
	// transfer the snapshot data again.
	StageDataCopied Stage = "data-copied"
	// StageComplete means that the snapshot metadata has been saved in the
	// destination repository. This is the final stage; a rerun skips the
	// snapshot entirely.
	StageComplete Stage = "complete"
)

func stageRank(s Stage) (int, error) {
	switch s {
	case StageDataCopied:
		return 1, nil
	case StageComplete:
		return 2, nil
	default:
		return 0, fmt.Errorf("unknown copy progress stage %q", s)
	}
}

// SnapshotProgress is the recorded progress for one source snapshot. The
// source snapshot ID is the immutable identity of the entry (it is the hash of
// the plaintext snapshot JSON in the source repository).
type SnapshotProgress struct {
	SourceID   restic.ID `json:"source_id"`
	OriginalID restic.ID `json:"original_id"`
	Stage      Stage     `json:"stage"`
	// TargetID is the ID of the copied snapshot in the destination repository.
	// It is only set once the snapshot has been saved.
	TargetID  *restic.ID `json:"target_id,omitempty"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// Ledger is the merged view of all copy state files stored in a repository.
type Ledger struct {
	Version   int                         `json:"version"`
	Snapshots map[string]SnapshotProgress `json:"snapshots"`
}

// New returns an empty ledger.
func New() *Ledger {
	return &Ledger{
		Version:   ledgerVersion,
		Snapshots: make(map[string]SnapshotProgress),
	}
}

// Empty reports whether the ledger contains no progress entries.
func (l *Ledger) Empty() bool {
	return len(l.Snapshots) == 0
}

// Forget discards the progress record for a source snapshot. It is used when a
// "complete" record points to a destination snapshot that no longer exists, so
// the snapshot has to be copied again instead of being skipped.
func (l *Ledger) Forget(sourceID restic.ID) {
	delete(l.Snapshots, sourceID.String())
}

// Get returns the recorded progress for a source snapshot.
func (l *Ledger) Get(sourceID restic.ID) (SnapshotProgress, bool) {
	e, ok := l.Snapshots[sourceID.String()]
	return e, ok
}

// IsComplete reports whether the source snapshot was copied completely and the
// destination snapshot is still present according to targetExists.
func (l *Ledger) IsComplete(sourceID restic.ID, targetExists func(restic.ID) bool) bool {
	e, ok := l.Get(sourceID)
	if !ok || e.Stage != StageComplete || e.TargetID == nil {
		return false
	}
	// a complete entry whose destination snapshot vanished is re-copied
	return targetExists(*e.TargetID)
}

// Mark records or advances the progress for a source snapshot. Stages may only
// advance forward; Mark returns an error on a stage regression. UpdatedAt is
// set to the current time.
func (l *Ledger) Mark(sourceID, originalID restic.ID, stage Stage, targetID *restic.ID) error {
	if sourceID.IsNull() {
		return errors.New("cannot record progress for empty snapshot ID")
	}
	newRank, err := stageRank(stage)
	if err != nil {
		return err
	}

	key := sourceID.String()
	if cur, ok := l.Snapshots[key]; ok {
		curRank, err := stageRank(cur.Stage)
		if err != nil {
			return fmt.Errorf("internal error: invalid stored stage for snapshot %s: %w", sourceID.Str(), err)
		}
		if newRank < curRank {
			return fmt.Errorf("refusing to regress copy progress for snapshot %s from %s to %s",
				sourceID.Str(), cur.Stage, stage)
		}
		if newRank == curRank && stage != StageComplete {
			// no point in rewriting an entry at the same non-final stage
			return nil
		}
		if originalID.IsNull() {
			originalID = cur.OriginalID
		}
		if targetID == nil {
			targetID = cur.TargetID
		}
	}

	l.Snapshots[key] = SnapshotProgress{
		SourceID:   sourceID,
		OriginalID: originalID,
		Stage:      stage,
		TargetID:   targetID,
		UpdatedAt:  time.Now(),
	}
	return nil
}

// merge folds a single entry loaded from a state file into the ledger. When
// several files record progress for the same snapshot, the more advanced stage
// wins; for equal stages the more recently updated entry wins.
func (l *Ledger) merge(e SnapshotProgress) error {
	if e.SourceID.IsNull() {
		return errors.New("copy state entry without source snapshot ID")
	}
	if _, err := stageRank(e.Stage); err != nil {
		return fmt.Errorf("copy state for snapshot %s: %w", e.SourceID.Str(), err)
	}

	key := e.SourceID.String()
	cur, ok := l.Snapshots[key]
	if !ok {
		l.Snapshots[key] = e
		return nil
	}

	curRank, _ := stageRank(cur.Stage)
	newRank, _ := stageRank(e.Stage)
	switch {
	case newRank > curRank:
		l.Snapshots[key] = e
	case newRank == curRank && e.UpdatedAt.After(cur.UpdatedAt):
		l.Snapshots[key] = e
	}
	return nil
}

// Save writes the current ledger as a new, immutable encrypted state file and
// returns its storage ID. Every call creates a new journal file; callers must
// only invoke it at durable progress boundaries.
func (l *Ledger) Save(ctx context.Context, saver restic.SaverUnpacked[restic.WriteableFileType]) (restic.ID, error) {
	return restic.SaveJSONUnpacked(ctx, saver, restic.WriteableCopyStateFile, l)
}

// LoadLedger reads and merges all copy state files in the repository. The
// returned ID set contains the state files that were merged and can be passed
// to Compact. A single unreadable or malformed state file is fatal: the
// ledger must never silently forget progress and re-copy data.
func LoadLedger(ctx context.Context, repo restic.ListerLoaderUnpacked) (*Ledger, restic.IDSet, error) {
	ledger := New()
	knownFiles := restic.NewIDSet()

	var m sync.Mutex
	err := restic.ParallelList(ctx, repo, restic.CopyStateFile, repo.Connections(), func(ctx context.Context, id restic.ID, _ int64) error {
		var state Ledger
		loadErr := restic.LoadJSONUnpacked(ctx, repo, restic.CopyStateFile, id, &state)
		if loadErr != nil {
			// A state file may vanish between listing and loading when another
			// copy run compacts the journal concurrently. Verify that before
			// treating the error as fatal.
			exists, err := stateFileExists(ctx, repo, id)
			if err != nil {
				return fmt.Errorf("failed to load copy state file %s: %w", id.Str(), err)
			}
			if !exists {
				debug.Log("copy state file %s vanished while loading, ignoring", id.Str())
				return nil
			}
			return fmt.Errorf("failed to load copy state file %s: %w", id.Str(), loadErr)
		}

		if state.Version != ledgerVersion {
			return fmt.Errorf("unsupported copy state version %d in file %s", state.Version, id.Str())
		}

		m.Lock()
		defer m.Unlock()
		knownFiles.Insert(id)
		for _, e := range state.Snapshots {
			if err := ledger.merge(e); err != nil {
				return fmt.Errorf("invalid copy state file %s: %w", id.Str(), err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	return ledger, knownFiles, nil
}

// stateFileExists re-lists the copy state files and reports whether id is still
// present.
func stateFileExists(ctx context.Context, lister restic.Lister, id restic.ID) (bool, error) {
	found := false
	err := lister.List(ctx, restic.CopyStateFile, func(existing restic.ID, _ int64) error {
		if existing.Equal(id) {
			found = true
		}
		return nil
	})
	return found, err
}

// Compact writes one merged state file and removes the journal files that were
// superseded by it. knownFiles is the set returned by LoadLedger: only files
// that existed when this run started are removed, so state files written
// concurrently by another copy run are never touched.
//
// A failure to write the merged file is returned as error. Failures to remove
// obsolete files are returned as warnings: the entries they contain remain
// safely journaled and will be compacted by a later run.
func (l *Ledger) Compact(ctx context.Context, repo restic.Unpacked[restic.WriteableFileType], knownFiles restic.IDSet) (warnings []error, err error) {
	if l.Empty() {
		return nil, nil
	}

	mergedID, err := l.Save(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("failed to persist copy progress: %w", err)
	}

	for id := range knownFiles {
		if id.Equal(mergedID) {
			continue
		}
		if rmErr := repo.RemoveUnpacked(ctx, restic.WriteableCopyStateFile, id); rmErr != nil {
			// re-check whether the file is already gone
			exists, statErr := stateFileExists(ctx, repo, id)
			if statErr == nil && !exists {
				continue
			}
			warnings = append(warnings, fmt.Errorf("failed to remove obsolete copy state file %s: %w", id.Str(), rmErr))
		}
	}
	return warnings, nil
}
