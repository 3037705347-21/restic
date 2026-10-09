package restic

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/restic/restic/internal/errors"
)

// RecoveryRecordVersion is the current version of the recovery record format.
const RecoveryRecordVersion = 1

// RecoveryRecord tracks root trees that the "recover" command has already
// recovered. It is stored as a standalone file in the repository so that the
// information survives even if the recovery snapshots themselves are removed.
type RecoveryRecord struct {
	// Version is the format version of the record.
	Version int `json:"version"`
	// Generation is a monotonically increasing counter used to select the
	// newest record when several record files are present, e.g. after a crash
	// while replacing the record.
	Generation uint64 `json:"generation"`
	// Created is the time at which the record was first written.
	Created time.Time `json:"created"`
	// Updated is the time at which this revision of the record was written.
	Updated time.Time `json:"updated"`
	// Roots contains the IDs of root trees that have been recovered. It is
	// sorted and free of duplicates.
	Roots IDs `json:"roots"`
}

// HasRoot returns true if the record contains the given root tree.
func (r *RecoveryRecord) HasRoot(id ID) bool {
	return slices.Contains(r.Roots, id)
}

// NewRecoveryRecord creates a new record with the given generation and roots.
// The roots are sorted and deduplicated.
func NewRecoveryRecord(generation uint64, created, updated time.Time, roots IDs) *RecoveryRecord {
	roots = sortAndDedupe(slices.Clone(roots))
	return &RecoveryRecord{
		Version:    RecoveryRecordVersion,
		Generation: generation,
		Created:    created,
		Updated:    updated,
		Roots:      roots,
	}
}

func sortAndDedupe(ids IDs) IDs {
	sort.Sort(ids)
	return slices.Compact(ids)
}

// ErrUnsupportedRecoveryRecordVersion is returned if a recovery record uses a
// format version newer than the one supported by this restic version.
var ErrUnsupportedRecoveryRecordVersion = errors.New("recovery record uses an unsupported format version")

// SaveRecoveryRecord serializes the record and stores it in the repository.
// It returns the ID of the newly written record file.
func SaveRecoveryRecord(ctx context.Context, repo SaverUnpacked[WriteableFileType], rec *RecoveryRecord) (ID, error) {
	return SaveJSONUnpacked(ctx, repo, WriteableRecoveryFile, rec)
}

// LoadRecoveryRecords loads all recovery records stored in the repository and
// returns them together with the IDs of their backing files. If no record
// exists, nil is returned. Loading fails if any record file is unreadable or
// uses an unsupported format version, as continuing in that state could
// result in duplicate recovery snapshots.
func LoadRecoveryRecords(ctx context.Context, repo ListerLoaderUnpacked) ([]*RecoveryRecord, IDs, error) {
	var records []*RecoveryRecord
	var fileIDs IDs

	err := repo.List(ctx, RecoveryFile, func(id ID, _ int64) error {
		record := &RecoveryRecord{}
		err := LoadJSONUnpacked(ctx, repo, RecoveryFile, id, record)
		if err != nil {
			return err
		}
		if record.Version > RecoveryRecordVersion {
			return ErrUnsupportedRecoveryRecordVersion
		}

		records = append(records, record)
		fileIDs = append(fileIDs, id)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	return records, fileIDs, nil
}
