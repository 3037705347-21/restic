package ledger

import (
	"errors"
	"time"

	"github.com/restic/restic/internal/restic"
)

// Standard quarantine reasons recorded by check.
const (
	ReasonDataVerification = "pack data verification failed"
	ReasonMissingPack      = "pack is missing from the repository"
	ReasonTruncatedPack    = "pack file size does not match the index"
)

// CompactThreshold is the segment volume at which a successful run rewrites the
// snapshot. It bounds on-disk growth for repeated partial runs.
const CompactThreshold int64 = 64 * 1024 * 1024

// VerifiedRecord builds a StageVerified record for the given fingerprint.
func VerifiedRecord(id restic.ID, fp Fingerprint) Record {
	return Record{
		PackID:    id,
		BlobHash:  fp.BlobHash,
		Size:      fp.Size,
		Stage:     StageVerified,
		UpdatedAt: time.Now().UTC(),
	}
}

// Selection is the result of reconciling ledger records with the current
// repository index.
type Selection struct {
	// Pending maps packs that must be read in this run to their current
	// fingerprints: packs never verified, verified packs whose index entries
	// changed, and registered packs whose index entries changed (the pack may
	// have been replaced in the meantime).
	Pending map[restic.ID]Fingerprint
	// Stale are ledger records for packs that no longer exist in the index.
	Stale restic.IDSet
}

// SelectPending reconciles records with the fingerprints computed from the
// current repository index.
func SelectPending(records []Record, current map[restic.ID]Fingerprint) Selection {
	sel := Selection{
		Pending: make(map[restic.ID]Fingerprint),
		Stale:   restic.NewIDSet(),
	}
	known := restic.NewIDSet()
	for _, rec := range records {
		known.Insert(rec.PackID)
		cur, ok := current[rec.PackID]
		if !ok {
			sel.Stale.Insert(rec.PackID)
			continue
		}
		switch {
		case rec.Stage == StageVerified && !cur.equal(rec.BlobHash, rec.Size):
			// index entries changed: previous verification no longer applies
			sel.Pending[rec.PackID] = cur
		case rec.Stage == StageVerified:
			// already verified against these index entries: skip
		case rec.Stage == StageRegistered && !cur.equal(rec.BlobHash, rec.Size):
			// the index moved on while the pack was quarantined; re-check it
			sel.Pending[rec.PackID] = cur
		default:
			// pack is in a later repair stage: repair owns it now
		}
	}
	for id, fp := range current {
		if !known.Has(id) {
			sel.Pending[id] = fp
		}
	}
	return sel
}

// verifiedTransition decides whether a successful read should produce a
// verified record. A quarantined pack may only return to verified when it was
// re-read against changed index entries. The second return value is false when
// the existing record must be kept untouched.
func verifiedTransition(id restic.ID, existing Record, exists bool, fp Fingerprint) (Record, bool) {
	if exists && existing.Stage.Quarantine() {
		if existing.Stage != StageRegistered || existing.BlobHash.Equal(fp.BlobHash) {
			return Record{}, false
		}
	}
	return VerifiedRecord(id, fp), true
}

// MarkVerified persists a successful verification of a pack. A quarantined
// pack may only return to verified when it was re-read against changed index
// entries (a different fingerprint than the one recorded at registration).
func (s *Store) MarkVerified(id restic.ID, fp Fingerprint) error {
	s.mu.Lock()
	existing, exists := s.records[id]
	s.mu.Unlock()

	rec, write := verifiedTransition(id, existing, exists, fp)
	if !write {
		return nil
	}
	return s.Put(rec)
}

// Registration is the result of RegisterQuarantine.
type Registration int

const (
	// RegistrationUnchanged means the pack is already in a later repair stage
	// and the report was ignored.
	RegistrationUnchanged Registration = iota
	// RegistrationUpdated means an existing registered record was refreshed.
	RegistrationUpdated
	// RegistrationCreated means a new quarantine record was written.
	RegistrationCreated
)

// RegisterQuarantine records a damaged pack and the blob handles that could
// still be verified. The operation is idempotent: repeatedly registering the
// same pack keeps a single record and never rolls back a repair stage.
func (s *Store) RegisterQuarantine(id restic.ID, fp Fingerprint, reason string, salvageable []restic.BlobHandle) (Registration, error) {
	s.mu.Lock()
	existing, exists := s.records[id]
	s.mu.Unlock()

	if exists && (existing.Stage == StageSalvaged || existing.Stage == StageRewritten) {
		// repair is already past registration; do not clobber its progress
		return RegistrationUnchanged, nil
	}

	result := RegistrationCreated
	stage := StageRegistered
	if exists && existing.Stage == StageRegistered {
		result = RegistrationUpdated
	}

	rec := Record{
		PackID:      id,
		BlobHash:    fp.BlobHash,
		Size:        fp.Size,
		Stage:       stage,
		Reason:      reason,
		Salvageable: salvageable,
		UpdatedAt:   time.Now().UTC(),
	}
	if err := s.Put(rec); err != nil {
		return RegistrationUnchanged, err
	}
	return result, nil
}

// ErrInvalidStageTransition is returned when a repair stage would move
// backwards or skip a stage.
var ErrInvalidStageTransition = errors.New("invalid ledger stage transition")

// AdvanceStage moves a pack to the next repair stage. Transitions are only
// allowed forward in the registered -> salvaged -> rewritten -> removed
// order; removed deletes the record. Every transition is fsynced before the
// call returns.
func (s *Store) AdvanceStage(id restic.ID, to Stage) error {
	s.mu.Lock()
	existing, exists := s.records[id]
	s.mu.Unlock()

	if to == StageRemoved {
		if !exists {
			return nil
		}
		if existing.Stage != StageRewritten {
			return invalidTransition(existing.Stage, to)
		}
		return s.Delete(id)
	}

	var want Stage
	switch {
	case !exists:
		return invalidTransition(StageNone, to)
	case existing.Stage == StageRegistered && to == StageSalvaged:
		want = StageSalvaged
	case existing.Stage == StageSalvaged && to == StageRewritten:
		want = StageRewritten
	default:
		return invalidTransition(existing.Stage, to)
	}

	existing.Stage = want
	existing.UpdatedAt = time.Now().UTC()
	return s.Put(existing)
}

// AdvanceStages moves several packs to the same stage in a single fsynced
// batch. It requires every pack to currently be at the expected predecessor
// stage; missing records are ignored (the pack was already completed).
func (s *Store) AdvanceStages(ids restic.IDSet, to Stage) error {
	s.mu.Lock()
	records := make([]Record, 0, len(ids))
	for id := range ids {
		existing, exists := s.records[id]
		if !exists {
			continue
		}
		valid := (existing.Stage == StageRegistered && to == StageSalvaged) ||
			(existing.Stage == StageSalvaged && to == StageRewritten)
		if !valid {
			s.mu.Unlock()
			return invalidTransition(existing.Stage, to)
		}
		existing.Stage = to
		existing.UpdatedAt = time.Now().UTC()
		records = append(records, existing)
	}
	s.mu.Unlock()
	return s.Apply(records, nil)
}

// RemoveStale deletes records for packs that have vanished from the index.
func (s *Store) RemoveStale(ids restic.IDSet) error {
	if len(ids) == 0 {
		return nil
	}
	return s.Apply(nil, ids.List())
}

// Quarantine returns all records awaiting or undergoing repair, ordered by
// pack ID.
func (s *Store) Quarantine() []Record {
	out := make([]Record, 0)
	for _, rec := range s.Records() {
		if rec.Stage.Quarantine() {
			out = append(out, rec)
		}
	}
	return out
}

func invalidTransition(from, to Stage) error {
	return errors.Join(ErrInvalidStageTransition, errors.New("transition "+from.String()+" -> "+to.String()))
}
