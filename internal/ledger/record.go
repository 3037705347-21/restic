// Package ledger implements the persistent verification quarantine ledger.
//
// The ledger records, per pack file, whether a `restic check --read-data` run
// has actually read and verified the pack (together with a fingerprint of the
// index entries that were used during verification) and the repair stage of
// packs that were found damaged.
//
// State is stored in a local, crash-safe directory: a compacted snapshot plus
// append-only segment files containing CRC protected frames. Mutations are
// batched and only acknowledged after being fsynced, so a crash can at most
// lose the last batch that was in flight.
package ledger

import (
	"fmt"
	"time"

	"github.com/restic/restic/internal/restic"
)

// Stage describes the lifecycle state of a pack record.
type Stage uint8

const (
	// StageNone is the zero value and is never persisted.
	StageNone Stage = iota
	// StageVerified marks a pack that was read and fully verified against the
	// index fingerprint stored in the record.
	StageVerified
	// StageRegistered marks a damaged pack that is quarantined and waiting for
	// repair.
	StageRegistered
	// StageSalvaged marks a quarantined pack whose intact blobs have been
	// re-uploaded.
	StageSalvaged
	// StageRewritten marks a pack that has been excluded from the repository
	// index.
	StageRewritten
	// StageRemoved is the terminal state. Transitions to this state delete the
	// record instead of persisting it.
	StageRemoved
)

func (s Stage) String() string {
	switch s {
	case StageVerified:
		return "verified"
	case StageRegistered:
		return "registered"
	case StageSalvaged:
		return "salvaged"
	case StageRewritten:
		return "rewritten"
	case StageRemoved:
		return "removed"
	default:
		return fmt.Sprintf("<invalid stage %d>", uint8(s))
	}
}

func (s Stage) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

func (s *Stage) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case `"verified"`:
		*s = StageVerified
	case `"registered"`:
		*s = StageRegistered
	case `"salvaged"`:
		*s = StageSalvaged
	case `"rewritten"`:
		*s = StageRewritten
	default:
		return fmt.Errorf("unknown ledger stage %q", string(data))
	}
	return nil
}

// QuarantineStage reports whether the stage belongs to a pack that has been
// quarantined for repair (registered or a later repair stage).
func (s Stage) Quarantine() bool {
	return s == StageRegistered || s == StageSalvaged || s == StageRewritten
}

// Record is the persistent state recorded for a single pack.
type Record struct {
	PackID restic.ID
	// BlobHash is index.PackBlobsHash at the time the record was written.
	BlobHash restic.ID
	// Size is the pack size from the index at the time the record was written.
	Size int64
	// Stage is the current lifecycle state.
	Stage Stage
	// Reason contains a short description of why a pack was quarantined.
	Reason string
	// Salvageable lists the blob handles that cryptographically verified
	// successfully while reading a damaged pack. It is empty for verified
	// packs and for packs that could not be read at all.
	Salvageable []restic.BlobHandle
	// UpdatedAt records when the record was last written.
	UpdatedAt time.Time
}

// Fingerprint identifies the index entries of a pack. A pack must be re-read
// when either value differs from the stored record.
type Fingerprint struct {
	BlobHash restic.ID
	Size     int64
}

func (f Fingerprint) equal(blobHash restic.ID, size int64) bool {
	return f.Size == size && f.BlobHash.Equal(blobHash)
}

// wireRecord is the compact JSON representation of a Record. IDs are encoded
// as lowercase hex strings, blob types as their restic JSON names.
type wireRecord struct {
	PackID      string           `json:"id"`
	BlobHash    string           `json:"bh"`
	Size        int64            `json:"sz"`
	Stage       Stage            `json:"st"`
	Reason      string           `json:"rs,omitempty"`
	Salvageable []wireBlobHandle `json:"sv,omitempty"`
	UpdatedAt   time.Time        `json:"ts"`
}

type wireBlobHandle struct {
	ID   string          `json:"id"`
	Type restic.BlobType `json:"t"`
}

func encodeRecord(rec Record) wireRecord {
	w := wireRecord{
		PackID:    rec.PackID.String(),
		BlobHash:  rec.BlobHash.String(),
		Size:      rec.Size,
		Stage:     rec.Stage,
		Reason:    rec.Reason,
		UpdatedAt: rec.UpdatedAt.UTC(),
	}
	if len(rec.Salvageable) > 0 {
		w.Salvageable = make([]wireBlobHandle, len(rec.Salvageable))
		for i, h := range rec.Salvageable {
			w.Salvageable[i] = wireBlobHandle{ID: h.ID.String(), Type: h.Type}
		}
	}
	return w
}

func decodeRecord(w wireRecord) (Record, error) {
	rec := Record{
		Size:      w.Size,
		Stage:     w.Stage,
		Reason:    w.Reason,
		UpdatedAt: w.UpdatedAt,
	}
	var err error
	rec.PackID, err = restic.ParseID(w.PackID)
	if err != nil {
		return Record{}, fmt.Errorf("pack id: %w", err)
	}
	if w.BlobHash != "" {
		rec.BlobHash, err = restic.ParseID(w.BlobHash)
		if err != nil {
			return Record{}, fmt.Errorf("blob hash: %w", err)
		}
	}
	if len(w.Salvageable) > 0 {
		rec.Salvageable = make([]restic.BlobHandle, len(w.Salvageable))
		for i, wh := range w.Salvageable {
			id, err := restic.ParseID(wh.ID)
			if err != nil {
				return Record{}, fmt.Errorf("salvageable blob id: %w", err)
			}
			rec.Salvageable[i] = restic.BlobHandle{ID: id, Type: wh.Type}
		}
	}
	return rec, nil
}
