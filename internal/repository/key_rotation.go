package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
)

// Roster rotation state is stored next to the repository keys in the keys
// directory. The file names are not valid restic IDs, so they are ignored when
// listing or searching repository keys.
const keyRosterStatePrefix = "roster-state-"

const keyRosterStateVersion = 1

// keyRosterState is the on-disk format of a roster rotation state file.
//
// Managed lists the key IDs that the last successful rotation considered to
// be current member keys. Only keys that are (or were) present in Managed can
// ever be removed by a rotation: keys added manually to the repository are not
// touched until a roster explicitly adopts them.
//
// PendingRetire lists Managed keys that a rotation has committed to remove
// but has not removed yet. It exists to complete an interrupted rotation on
// the next run instead of leaving stale keys behind.
type keyRosterState struct {
	Version       int      `json:"version"`
	Generation    uint64   `json:"generation"`
	Managed       []string `json:"managed"`
	PendingRetire []string `json:"pending_retire,omitempty"`
}

type rosterStateFile struct {
	name  string
	data  []byte
	state keyRosterState
}

// KeyRosterPlan is the result of reconciling the keys in a repository with a
// roster of current member key IDs.
type KeyRosterPlan struct {
	// Current is the key used to open the repository.
	Current restic.ID
	// Roster are the key IDs listed in the roster.
	Roster restic.IDSet
	// Managed are the key IDs recorded by the last committed rotation state.
	Managed restic.IDSet
	// Retire are Managed keys that still exist but are no longer listed; they
	// are removed when the plan is applied.
	Retire restic.IDSet
	// Adopt are roster keys that were not managed before; they are recorded in
	// the new state.
	Adopt restic.IDSet
	// Untouched are existing keys that no rotation has ever managed; they are
	// left as-is even when they are not part of the roster.
	Untouched restic.IDSet

	// Baseline is true when no previous rotation state exists. A baseline
	// rotation records the roster but never removes keys.
	Baseline bool
	// NoOp is true when applying the plan would not modify the repository at
	// all (roster matches the committed state and there is nothing to clean
	// up).
	NoOp bool

	// keyBytes contains the raw key file bytes of every existing key. They are
	// loaded during planning so that removed keys can be restored verbatim if
	// a write fails while the plan is applied.
	keyBytes map[restic.ID][]byte
	// oldStates are the committed rotation state files, sorted by ascending
	// generation.
	oldStates []rosterStateFile
	// generation is the generation of the current state, or zero when the
	// baseline is established.
	generation uint64
	// pending are the keys the current state still has scheduled for removal.
	pending restic.IDSet
}

// loadRosterStates returns all valid rotation state files, sorted by ascending
// generation and name. Unreadable, corrupt or unsupported state is a fatal
// error: rotation fails closed instead of risking to remove the wrong keys.
func (s *Repository) loadRosterStates(ctx context.Context) ([]rosterStateFile, error) {
	var files []rosterStateFile

	err := s.be.List(ctx, backend.KeyFile, func(fi backend.FileInfo) error {
		if !strings.HasPrefix(fi.Name, keyRosterStatePrefix) {
			return nil
		}

		name := fi.Name
		suffix := strings.TrimPrefix(name, keyRosterStatePrefix)
		id, err := restic.ParseID(suffix)
		if err != nil {
			return errors.Errorf("roster state file %q has an invalid name", name)
		}

		data, err := loadRaw(ctx, s.be, backend.Handle{Type: backend.KeyFile, Name: name})
		if err != nil {
			return fmt.Errorf("loading roster state file %q: %w", name, err)
		}

		if id != restic.Hash(data) {
			return errors.Errorf("roster state file %q is corrupt (content hash mismatch)", name)
		}

		var st keyRosterState
		if err := json.Unmarshal(data, &st); err != nil {
			return fmt.Errorf("parsing roster state file %q: %w", name, err)
		}

		if st.Version != keyRosterStateVersion {
			return errors.Errorf("roster state file %q uses unsupported version %d", name, st.Version)
		}
		if st.Generation == 0 {
			return errors.Errorf("roster state file %q has an invalid generation", name)
		}

		for _, field := range []struct {
			name string
			ids  []string
		}{
			{"managed", st.Managed},
			{"pending_retire", st.PendingRetire},
		} {
			for _, idStr := range field.ids {
				if _, err := restic.ParseID(idStr); err != nil {
					return errors.Errorf("roster state file %q has invalid key ID %q in %q: %w", name, idStr, field.name, err)
				}
			}
		}

		files = append(files, rosterStateFile{name: name, data: data, state: st})
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].state.Generation != files[j].state.Generation {
			return files[i].state.Generation < files[j].state.Generation
		}
		return files[i].name < files[j].name
	})

	return files, nil
}

// PlanKeyRoster reconciles the repository keys with rosterRefs, which are key
// IDs or unique prefixes of key IDs as accepted by restic.Find. The plan only
// reads the repository; call ApplyKeyRoster to execute it.
func PlanKeyRoster(ctx context.Context, s *Repository, rosterRefs []string) (*KeyRosterPlan, error) {
	existing := restic.NewIDSet()
	if err := s.List(ctx, restic.KeyFile, func(id restic.ID, _ int64) error {
		existing.Insert(id)
		return nil
	}); err != nil {
		return nil, err
	}

	// Resolve and validate the roster before anything else.
	roster := restic.NewIDSet()
	for _, ref := range rosterRefs {
		id, err := restic.Find(ctx, s, restic.KeyFile, ref)
		if err != nil {
			return nil, fmt.Errorf("resolving roster entry %q: %w", ref, err)
		}
		if roster.Has(id) {
			return nil, errors.Errorf("roster lists key %v more than once", id.Str())
		}
		roster.Insert(id)
	}

	if len(roster) == 0 {
		return nil, errors.New("roster must contain at least one key")
	}

	current := s.KeyID()
	if !roster.Has(current) {
		return nil, errors.Errorf("refusing to rotate: the current key %v used to access the repository is not listed in the roster", current.Str())
	}

	stateFiles, err := s.loadRosterStates(ctx)
	if err != nil {
		return nil, err
	}

	plan := &KeyRosterPlan{
		Current:   current,
		Roster:    roster,
		Managed:   restic.NewIDSet(),
		Retire:    restic.NewIDSet(),
		Adopt:     restic.NewIDSet(),
		Untouched: restic.NewIDSet(),
		Baseline:  len(stateFiles) == 0,
		keyBytes:  make(map[restic.ID][]byte),
	}

	committed := restic.NewIDSet()
	pending := restic.NewIDSet()
	if len(stateFiles) > 0 {
		cur := stateFiles[len(stateFiles)-1].state
		plan.generation = cur.Generation
		plan.oldStates = stateFiles
		for _, idStr := range cur.Managed {
			id, err := restic.ParseID(idStr)
			if err != nil {
				return nil, errors.Errorf("roster state contains invalid managed key %q: %w", idStr, err)
			}
			committed.Insert(id)
		}
		for _, idStr := range cur.PendingRetire {
			id, err := restic.ParseID(idStr)
			if err != nil {
				return nil, errors.Errorf("roster state contains invalid pending key %q: %w", idStr, err)
			}
			pending.Insert(id)
		}
	}
	plan.pending = pending
	plan.Managed = committed

	// Keys are only removed when a previous rotation knew about them and they
	// are either still scheduled for removal or no longer part of the roster.
	known := committed.Clone()
	known.Merge(pending)
	plan.Retire = known.Intersect(existing).Sub(roster)

	if plan.Retire.Has(current) {
		// This can never happen as the current key is part of the roster; keep
		// the guard to make the unlock guarantee explicit.
		return nil, errors.Errorf("refusing to rotate: current key %v would be removed", current.Str())
	}

	plan.Adopt = roster.Sub(committed)
	// Keys outside the roster and never managed are manual keys and stay
	// untouched; roster keys that are merely being adopted are not reported
	// here to avoid listing them in both categories.
	plan.Untouched = existing.Sub(committed).Sub(roster)

	// Snapshot all key files before any write: this both rejects repositories
	// with unreadable keys up front and provides the bytes for rolling
	// removed keys back verbatim. Key file names are content addressed, so
	// saving the same bytes restores the exact same key.
	for id := range existing {
		buf, err := s.LoadRaw(ctx, restic.KeyFile, id)
		if err != nil {
			return nil, fmt.Errorf("loading key %v: %w", id.Str(), err)
		}
		plan.keyBytes[id] = buf
	}

	plan.NoOp = !plan.Baseline &&
		len(plan.Retire) == 0 &&
		len(pending) == 0 &&
		committed.Equals(roster) &&
		len(stateFiles) == 1

	return plan, nil
}

func marshalRosterState(generation uint64, managed restic.IDSet, retire restic.IDSet) (name string, data []byte, err error) {
	st := keyRosterState{
		Version:    keyRosterStateVersion,
		Generation: generation,
		Managed:    idSetStrings(managed),
	}
	if len(retire) > 0 {
		st.PendingRetire = idSetStrings(retire)
	}

	data, err = json.Marshal(&st)
	if err != nil {
		return "", nil, err
	}
	return keyRosterStatePrefix + restic.Hash(data).String(), data, nil
}

func idSetStrings(set restic.IDSet) []string {
	ids := set.List()
	strs := make([]string, 0, len(ids))
	for _, id := range ids {
		strs = append(strs, id.String())
	}
	return strs
}

// ApplyKeyRoster executes the plan. It either completes the rotation
// successfully or, when a write fails after the first modification, restores
// all removed keys and the previous rotation state. A successful run leaves
// the repository in a state where repeating the same roster is a no-op.
//
// The returned IDs are the keys that were removed.
func (s *Repository) ApplyKeyRoster(ctx context.Context, plan *KeyRosterPlan) (restic.IDs, error) {
	if plan.NoOp {
		return nil, nil
	}

	be := s.be
	save := func(name string, data []byte) error {
		return be.Save(ctx, backend.Handle{Type: backend.KeyFile, Name: name}, backend.NewByteReader(data, be.Hasher()))
	}
	remove := func(name string) error {
		return be.Remove(ctx, backend.Handle{Type: backend.KeyFile, Name: name})
	}
	exists := func(name string) (bool, error) {
		_, err := be.Stat(ctx, backend.Handle{Type: backend.KeyFile, Name: name})
		if err == nil {
			return true, nil
		}
		if be.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	saveIfAbsent := func(name string, data []byte) error {
		ok, err := exists(name)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		return save(name, data)
	}

	// Cleanup-only run: an earlier interrupted rotation left stale state files
	// behind, but the current state already matches the roster. Only remove the
	// stale generations, no key is touched.
	cleanupOnly := !plan.Baseline && len(plan.Retire) == 0 && len(plan.pending) == 0 &&
		plan.Managed.Equals(plan.Roster) && len(plan.oldStates) > 1
	if cleanupOnly {
		for _, old := range plan.oldStates[:len(plan.oldStates)-1] {
			if err := remove(old.name); err != nil && !be.IsNotExist(err) {
				return nil, fmt.Errorf("cleaning up stale roster state file %q: %w", old.name, err)
			}
			debug.Log("removed stale roster state file %v", old.name)
		}
		return nil, nil
	}

	var staged []string
	removed := map[restic.ID][]byte{}

	// rollback restores every removed key and reinstates the previous state.
	// A compensating state file with a generation higher than any staged file
	// makes the restored state win even if removing the staged state files
	// fails.
	rollback := func(cause error) (restic.IDs, error) {
		var rberr []error

		for id := range removed {
			if err := saveIfAbsent(id.String(), removed[id]); err != nil {
				rberr = append(rberr, fmt.Errorf("restoring key %v: %w", id.Str(), err))
			}
		}

		var maxGen uint64
		for _, old := range plan.oldStates {
			if err := saveIfAbsent(old.name, old.data); err != nil {
				rberr = append(rberr, fmt.Errorf("restoring roster state %q: %w", old.name, err))
			}
			if old.state.Generation > maxGen {
				maxGen = old.state.Generation
			}
		}

		// Remove the intent/finalization files staged by this run. The
		// compensating state below is intentionally not part of this list so
		// that it survives as the state with the highest generation.
		for _, name := range staged {
			if err := remove(name); err != nil && !be.IsNotExist(err) {
				// The compensating state outranks staged files, so a failed
				// cleanup is reported but does not affect the restored state.
				debug.Log("could not remove staged roster state file %q during rollback: %v", name, err)
			}
		}

		if len(plan.oldStates) > 0 {
			cur := plan.oldStates[len(plan.oldStates)-1].state
			// maxGen is the previous generation; staged files are at most
			// maxGen+2 (intent and finalization), so maxGen+3 always wins.
			name, data, err := marshalRosterState(maxGen+3, idSetFromStrings(cur.Managed), idSetFromStrings(cur.PendingRetire))
			if err != nil {
				rberr = append(rberr, err)
			} else if err := saveIfAbsent(name, data); err != nil {
				rberr = append(rberr, fmt.Errorf("writing rollback roster state: %w", err))
			}
		}

		if len(rberr) > 0 {
			return nil, fmt.Errorf("key rotation failed (%v) and rollback encountered errors: %v", cause, rberr)
		}
		return nil, fmt.Errorf("key rotation failed and has been rolled back: %w", cause)
	}

	// Phase 1: commit the intent. When keys have to be removed, an intermediate
	// state recording them as pending survives interruptions; otherwise the
	// final state is written directly.
	retireList := plan.Retire.List()
	intentGen := plan.generation + 1
	intentName, intentData, err := marshalRosterState(intentGen, plan.Roster, plan.Retire)
	if err != nil {
		return nil, err
	}
	if err := save(intentName, intentData); err != nil {
		// Nothing was modified before this point.
		return nil, fmt.Errorf("saving roster state: %w", err)
	}
	staged = append(staged, intentName)

	for _, old := range plan.oldStates {
		if err := remove(old.name); err != nil && !be.IsNotExist(err) {
			return rollback(err)
		}
	}

	if len(plan.Retire) > 0 {
		// Phase 2: remove the retired keys. Keys that no longer exist (an
		// earlier interrupted run may already have removed them) are skipped.
		for _, id := range retireList {
			err := RemoveKey(ctx, s, id)
			if err != nil {
				if be.IsNotExist(err) {
					debug.Log("key %v was already removed", id)
					continue
				}
				return rollback(err)
			}
			removed[id] = plan.keyBytes[id]
		}

		// Unlock guarantee: the current key must still be present.
		ok, err := exists(plan.Current.String())
		if err != nil {
			return rollback(err)
		}
		if !ok {
			return rollback(errors.Errorf("current key %v is missing after rotation", plan.Current.Str()))
		}

		// Phase 3: finalize with an empty pending set.
		finalName, finalData, err := marshalRosterState(intentGen+1, plan.Roster, nil)
		if err != nil {
			return rollback(err)
		}
		if err := save(finalName, finalData); err != nil {
			return rollback(err)
		}
		staged = append(staged, finalName)
	}

	// The last staged file is the state that should remain (the directly
	// committed state when no keys are removed, the finalization state
	// otherwise). Remove all intermediate files; a failed removal only leaves
	// garbage behind that the next run cleans up because the kept state has
	// the highest generation.
	for _, name := range staged[:len(staged)-1] {
		if err := remove(name); err != nil && !be.IsNotExist(err) {
			debug.Log("could not remove superseded roster state file %q: %v", name, err)
		}
	}

	return retireList, nil
}

func idSetFromStrings(ids []string) restic.IDSet {
	set := restic.NewIDSet()
	for _, idStr := range ids {
		id, err := restic.ParseID(idStr)
		if err != nil {
			// state files are validated when loaded
			panic(fmt.Sprintf("invalid key ID %q in roster state", idStr))
		}
		set.Insert(id)
	}
	return set
}
