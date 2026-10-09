package repository

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/mem"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

func addTestKey(t *testing.T, repo *Repository, password string) restic.ID {
	t.Helper()
	k, err := AddKey(context.TODO(), repo, password, "test", "host", repo.Key())
	rtest.OK(t, err)
	return k.ID()
}

func rosterRefs(ids ...restic.ID) []string {
	refs := make([]string, 0, len(ids))
	for _, id := range ids {
		refs = append(refs, id.String())
	}
	return refs
}

func listKeyIDs(t *testing.T, repo *Repository) restic.IDSet {
	t.Helper()
	ids := restic.NewIDSet()
	rtest.OK(t, repo.List(context.TODO(), restic.KeyFile, func(id restic.ID, _ int64) error {
		ids.Insert(id)
		return nil
	}))
	return ids
}

func listRosterStateFiles(t *testing.T, repo *Repository) []rosterStateFile {
	t.Helper()
	files, err := repo.loadRosterStates(context.TODO())
	rtest.OK(t, err)
	return files
}

func TestKeyRosterBaselineTouchesNoKeys(t *testing.T) {
	ctx := context.TODO()
	repo, _ := TestRepositoryWithBackend(t, nil, 0, Options{})
	current := repo.KeyID()
	manual := addTestKey(t, repo, "manual-password")

	plan, err := PlanKeyRoster(ctx, repo, rosterRefs(current))
	rtest.OK(t, err)
	rtest.Assert(t, plan.Baseline, "expected first rotation to be a baseline")
	rtest.Assert(t, plan.NoOp == false, "baseline must be applied to record state")
	rtest.Equals(t, 0, len(plan.Retire))
	rtest.Assert(t, plan.Untouched.Has(manual), "manual key must show up as untouched")

	retired, err := repo.ApplyKeyRoster(ctx, plan)
	rtest.OK(t, err)
	rtest.Equals(t, 0, len(retired))

	keys := listKeyIDs(t, repo)
	rtest.Assert(t, keys.Has(current) && keys.Has(manual), "no keys must be removed by the baseline")

	states := listRosterStateFiles(t, repo)
	rtest.Equals(t, 1, len(states))
	rtest.Equals(t, []string{current.String()}, states[0].state.Managed)

	// re-running the same roster must be a no-op
	plan2, err := PlanKeyRoster(ctx, repo, rosterRefs(current))
	rtest.OK(t, err)
	rtest.Assert(t, plan2.NoOp, "expected re-run to be a no-op")
	_, err = repo.ApplyKeyRoster(ctx, plan2)
	rtest.OK(t, err)
	rtest.Equals(t, 1, len(listRosterStateFiles(t, repo)))
	rtest.Assert(t, listKeyIDs(t, repo).Has(manual), "manual key must survive repeated rotations")
}

func TestKeyRosterRemovesOnlyDroppedManagedKeys(t *testing.T) {
	ctx := context.TODO()
	repo, _ := TestRepositoryWithBackend(t, nil, 0, Options{})
	current := repo.KeyID()
	k1 := addTestKey(t, repo, "pw-1")
	k2 := addTestKey(t, repo, "pw-2")

	// establish the baseline with all three keys
	plan, err := PlanKeyRoster(ctx, repo, rosterRefs(current, k1, k2))
	rtest.OK(t, err)
	_, err = repo.ApplyKeyRoster(ctx, plan)
	rtest.OK(t, err)

	// a key added manually afterwards must never be removed
	manual := addTestKey(t, repo, "pw-manual")

	// rotate with k2 missing from the roster
	plan, err = PlanKeyRoster(ctx, repo, rosterRefs(current, k1))
	rtest.OK(t, err)
	rtest.Assert(t, !plan.Baseline, "rotation must not be a baseline")
	rtest.Assert(t, plan.Retire.Has(k2), "k2 must be scheduled for removal")
	rtest.Assert(t, !plan.Retire.Has(manual), "manual key must not be scheduled for removal")
	rtest.Assert(t, !plan.Retire.Has(current), "current key must not be scheduled for removal")
	rtest.Assert(t, plan.Untouched.Has(manual), "manual key must remain untouched")

	retired, err := repo.ApplyKeyRoster(ctx, plan)
	rtest.OK(t, err)
	rtest.Equals(t, restic.IDs{k2}, retired)

	keys := listKeyIDs(t, repo)
	rtest.Assert(t, !keys.Has(k2), "k2 must have been removed")
	rtest.Assert(t, keys.Has(current) && keys.Has(k1) && keys.Has(manual),
		"current, retained and manual keys must remain")

	states := listRosterStateFiles(t, repo)
	rtest.Equals(t, 1, len(states))
	// baseline used generation 1; the removing rotation commits intent at
	// generation 2 and the finalized state at generation 3
	rtest.Equals(t, uint64(3), states[0].state.Generation)
	rtest.Equals(t, 0, len(states[0].state.PendingRetire))
	rtest.Assert(t, idSetFromStrings(states[0].state.Managed).Equals(restic.NewIDSet(current, k1)),
		"unexpected managed set: %v", states[0].state.Managed)

	// the repository is still unlockable using the current key's password
	rtest.OK(t, repo.SearchKey(ctx, rtest.TestPassword, 0, current.String()))

	// running the rotation again changes nothing
	plan, err = PlanKeyRoster(ctx, repo, rosterRefs(current, k1))
	rtest.OK(t, err)
	rtest.Assert(t, plan.NoOp, "expected re-run to be a no-op")
	_, err = repo.ApplyKeyRoster(ctx, plan)
	rtest.OK(t, err)
	rtest.Equals(t, 1, len(listRosterStateFiles(t, repo)))
	rtest.Assert(t, listKeyIDs(t, repo).Has(manual), "manual key must survive repeated rotations")
}

func TestKeyRosterAdoptsNewRosterKeys(t *testing.T) {
	ctx := context.TODO()
	repo, _ := TestRepositoryWithBackend(t, nil, 0, Options{})
	current := repo.KeyID()

	plan, err := PlanKeyRoster(ctx, repo, rosterRefs(current))
	rtest.OK(t, err)
	_, err = repo.ApplyKeyRoster(ctx, plan)
	rtest.OK(t, err)

	member := addTestKey(t, repo, "pw-member")
	plan, err = PlanKeyRoster(ctx, repo, rosterRefs(current, member))
	rtest.OK(t, err)
	rtest.Assert(t, plan.Adopt.Has(member), "new roster key must be adopted")
	rtest.Equals(t, 0, len(plan.Retire))
	_, err = repo.ApplyKeyRoster(ctx, plan)
	rtest.OK(t, err)

	// the adopted key is now managed: dropping it from the roster removes it
	plan, err = PlanKeyRoster(ctx, repo, rosterRefs(current))
	rtest.OK(t, err)
	rtest.Assert(t, plan.Retire.Has(member), "adopted key must be removable once dropped from roster")
	_, err = repo.ApplyKeyRoster(ctx, plan)
	rtest.OK(t, err)
	rtest.Assert(t, !listKeyIDs(t, repo).Has(member), "dropped adopted key must be removed")
}

func TestKeyRosterPrefixes(t *testing.T) {
	ctx := context.TODO()
	repo, _ := TestRepositoryWithBackend(t, nil, 0, Options{})
	current := repo.KeyID()

	plan, err := PlanKeyRoster(ctx, repo, []string{current.String()[:8]})
	rtest.OK(t, err)
	rtest.Assert(t, plan.Roster.Has(current), "unique prefix must resolve to the current key")
}

func TestKeyRosterRejectsInvalidRosters(t *testing.T) {
	ctx := context.TODO()
	repo, _ := TestRepositoryWithBackend(t, nil, 0, Options{})
	current := repo.KeyID()
	other := addTestKey(t, repo, "pw-other")

	for i, tc := range []struct {
		refs []string
		msg  string
	}{
		{nil, "at least one"},
		{rosterRefs(other), "current key"},
		{append(rosterRefs(current), "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"), "resolving"},
		{append(rosterRefs(current, other), current.String()), "more than once"},
	} {
		plan, err := PlanKeyRoster(ctx, repo, tc.refs)
		rtest.Assert(t, err != nil, "test case %d expected error", i)
		rtest.Assert(t, plan == nil, "test case %d must not return a plan", i)
		rtest.Assert(t, strings.Contains(err.Error(), tc.msg),
			"test case %d: error %q does not contain %q", i, err, tc.msg)
	}

	// a rejected plan must not create state files
	rtest.Equals(t, 0, len(listRosterStateFiles(t, repo)))
	rtest.Assert(t, listKeyIDs(t, repo).Has(other), "keys must not change on invalid roster")
}

// failRemoveKeyBackend fails to remove one specific key file, simulating a
// write failure in the middle of a rotation.
type failRemoveKeyBackend struct {
	backend.Backend
	failName string
	failed   bool
}

func (b *failRemoveKeyBackend) Remove(ctx context.Context, h backend.Handle) error {
	if !b.failed && h.Type == backend.KeyFile && h.Name == b.failName &&
		!strings.HasPrefix(h.Name, keyRosterStatePrefix) {
		b.failed = true
		return fmt.Errorf("simulated removal failure")
	}
	return b.Backend.Remove(ctx, h)
}

func TestKeyRosterRollsBackWhenKeyRemovalFails(t *testing.T) {
	ctx := context.TODO()
	be := mem.New()
	repo, _ := TestRepositoryWithBackend(t, be, 0, Options{})
	current := repo.KeyID()
	k1 := addTestKey(t, repo, "pw-1")
	k2 := addTestKey(t, repo, "pw-2")

	plan, err := PlanKeyRoster(ctx, repo, rosterRefs(current, k1, k2))
	rtest.OK(t, err)
	_, err = repo.ApplyKeyRoster(ctx, plan)
	rtest.OK(t, err)

	before := listKeyIDs(t, repo)

	failing := &failRemoveKeyBackend{Backend: be, failName: k2.String()}
	repo.be = failing

	plan, err = PlanKeyRoster(ctx, repo, rosterRefs(current, k1))
	rtest.OK(t, err)
	_, err = repo.ApplyKeyRoster(ctx, plan)
	rtest.Assert(t, err != nil, "rotation must fail when a key cannot be removed")
	rtest.Assert(t, strings.Contains(err.Error(), "rolled back"), "unexpected error: %v", err)
	rtest.Assert(t, failing.failed, "failure must have been injected")

	// the repository must be unchanged: all keys remain
	rtest.Assert(t, listKeyIDs(t, repo).Equals(before), "rollback must restore the original key set")

	// the committed state must describe the original roster again. The
	// compensating state (generation 4) wins over the staged intent state
	// (generation 2), even though intent removal succeeds.
	states := listRosterStateFiles(t, repo)
	cur := states[len(states)-1].state
	rtest.Equals(t, uint64(4), cur.Generation, "compensating state must win after rollback")
	rtest.Assert(t, idSetFromStrings(cur.Managed).Equals(restic.NewIDSet(current, k1, k2)),
		"rolled back state must describe the original roster, got %v", cur.Managed)
	rtest.Equals(t, 0, len(cur.PendingRetire))

	// after the backend recovers, the rotation succeeds
	repo.be = be
	plan, err = PlanKeyRoster(ctx, repo, rosterRefs(current, k1))
	rtest.OK(t, err)
	retired, err := repo.ApplyKeyRoster(ctx, plan)
	rtest.OK(t, err)
	rtest.Equals(t, restic.IDs{k2}, retired)
	rtest.Assert(t, !listKeyIDs(t, repo).Has(k2), "k2 must be removed on retry")
}

func TestKeyRosterResumesInterruptedRotation(t *testing.T) {
	ctx := context.TODO()
	repo, _ := TestRepositoryWithBackend(t, nil, 0, Options{})
	current := repo.KeyID()
	k1 := addTestKey(t, repo, "pw-1")
	k2 := addTestKey(t, repo, "pw-2")

	// committed baseline state (generation 1, all keys managed)
	baseline := restic.NewIDSet(current, k1, k2)
	name, data, err := marshalRosterState(1, baseline, nil)
	rtest.OK(t, err)
	rtest.OK(t, repo.be.Save(ctx, backend.Handle{Type: backend.KeyFile, Name: name},
		backend.NewByteReader(data, repo.be.Hasher())))

	// simulate a crash after the intent state was written but before k2 was removed
	intent := restic.NewIDSet(current, k1)
	pending := restic.NewIDSet(k2)
	nameA, dataA, err := marshalRosterState(2, intent, pending)
	rtest.OK(t, err)
	rtest.OK(t, repo.be.Save(ctx, backend.Handle{Type: backend.KeyFile, Name: nameA},
		backend.NewByteReader(dataA, repo.be.Hasher())))
	// old baseline state is still present as well

	plan, err := PlanKeyRoster(ctx, repo, rosterRefs(current, k1))
	rtest.OK(t, err)
	rtest.Assert(t, plan.Retire.Has(k2), "interrupted removal must be resumed")
	_, err = repo.ApplyKeyRoster(ctx, plan)
	rtest.OK(t, err)

	keys := listKeyIDs(t, repo)
	rtest.Assert(t, !keys.Has(k2) && keys.Has(current) && keys.Has(k1), "resumed rotation must converge")

	states := listRosterStateFiles(t, repo)
	rtest.Equals(t, 1, len(states), "stale state files must be collected")
	rtest.Equals(t, 0, len(states[0].state.PendingRetire))
}

func TestKeyRosterFinalizesAfterCompletedButUnfinalizedRemoval(t *testing.T) {
	ctx := context.TODO()
	repo, _ := TestRepositoryWithBackend(t, nil, 0, Options{})
	current := repo.KeyID()
	k1 := addTestKey(t, repo, "pw-1")
	k2 := addTestKey(t, repo, "pw-2")

	// state A: rotation committed intent (generation 2) and removed k2, but
	// the process crashed before the final state was written
	intent := restic.NewIDSet(current, k1)
	pending := restic.NewIDSet(k2)
	nameA, dataA, err := marshalRosterState(2, intent, pending)
	rtest.OK(t, err)
	rtest.OK(t, repo.be.Save(ctx, backend.Handle{Type: backend.KeyFile, Name: nameA},
		backend.NewByteReader(dataA, repo.be.Hasher())))
	rtest.OK(t, repo.be.Remove(ctx, backend.Handle{Type: backend.KeyFile, Name: k2.String()}))

	plan, err := PlanKeyRoster(ctx, repo, rosterRefs(current, k1))
	rtest.OK(t, err)
	rtest.Equals(t, 0, len(plan.Retire), "already removed key must not be removed again")
	rtest.Assert(t, !plan.NoOp, "stale pending state requires finalization")

	retired, err := repo.ApplyKeyRoster(ctx, plan)
	rtest.OK(t, err)
	rtest.Equals(t, 0, len(retired))

	states := listRosterStateFiles(t, repo)
	rtest.Equals(t, 1, len(states), "intent state must be collected")
	rtest.Equals(t, 0, len(states[0].state.PendingRetire), "final state must not keep pending entries")
	rtest.Assert(t, idSetFromStrings(states[0].state.Managed).Equals(restic.NewIDSet(current, k1)),
		"unexpected managed set: %v", states[0].state.Managed)

	// the finalized state makes the next run a no-op
	plan, err = PlanKeyRoster(ctx, repo, rosterRefs(current, k1))
	rtest.OK(t, err)
	rtest.Assert(t, plan.NoOp, "expected re-run to be a no-op")
}

func TestKeyRosterStateFilesAreInvisibleToKeyListing(t *testing.T) {
	ctx := context.TODO()
	repo, _ := TestRepositoryWithBackend(t, nil, 0, Options{})
	current := repo.KeyID()

	plan, err := PlanKeyRoster(ctx, repo, rosterRefs(current))
	rtest.OK(t, err)
	_, err = repo.ApplyKeyRoster(ctx, plan)
	rtest.OK(t, err)

	// key listing and key search must ignore the state file in the keys directory
	ids := restic.NewIDSet()
	rtest.OK(t, repo.List(ctx, restic.KeyFile, func(id restic.ID, _ int64) error {
		ids.Insert(id)
		return nil
	}))
	rtest.Equals(t, 1, len(ids))
	rtest.Assert(t, ids.Has(current), "only the real key must be listed")

	// the raw backend listing does contain the state file
	var stateSeen bool
	rtest.OK(t, repo.be.List(ctx, backend.KeyFile, func(fi backend.FileInfo) error {
		if strings.HasPrefix(fi.Name, keyRosterStatePrefix) {
			stateSeen = true
		}
		return nil
	}))
	rtest.Assert(t, stateSeen, "state file must be stored in the keys directory")
}
