// Package inventory implements a persistable inventory of the objects
// physically present in a repository backend.
//
// An inventory is taken by listing every relevant file type in the backend.
// Comparing two inventories shows which objects have been removed or
// replaced (same type and ID, but a different size) in the meantime. Objects
// that were added in the meantime are tracked separately: they are expected
// when another backup ran concurrently and must not be reported as
// suspicious changes.
package inventory

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
)

// CurrentVersion is the version of the inventory file format written by this
// package.
const CurrentVersion = 1

// TrackedTypes lists the file types that an inventory records, in the order
// in which they are enumerated.
//
// LockFile is intentionally excluded: locks are short-lived operational files
// that concurrent backups create and remove all the time, so recording them
// would result in spurious removal reports. ConfigFile is excluded as well:
// it is a single fixed-name file that is never identified by an ID and can
// never be replaced by normal repository operations.
var TrackedTypes = []restic.FileType{
	restic.PackFile,
	restic.KeyFile,
	restic.SnapshotFile,
	restic.IndexFile,
}

// Record describes a single object found in the backend.
type Record struct {
	ID   string `json:"id"`
	Size int64  `json:"size"`
}

// Ledger is the persistable inventory of all objects that existed in a
// repository at a single point in time.
type Ledger struct {
	// Version is the format version of the inventory file.
	Version int `json:"version"`
	// RepoID is the ID of the repository the inventory was taken from. It is
	// used to make sure an inventory is never compared against a different
	// repository.
	RepoID string `json:"repo_id"`
	// GeneratedAt records when the enumeration completed.
	GeneratedAt time.Time `json:"generated_at"`
	// Objects maps the file type name ("data", "key", "snapshot", "index") to
	// the sorted list of objects of that type.
	Objects map[string][]Record `json:"objects"`
}

// Enumerate lists every tracked file type in the backend and returns the
// resulting inventory. It either enumerates all file types successfully or
// fails without producing an inventory, so that a partial enumeration can
// never be mistaken for a complete one.
func Enumerate(ctx context.Context, lister restic.Lister, repoID string, now time.Time) (*Ledger, error) {
	ledger := &Ledger{
		Version:     CurrentVersion,
		RepoID:      repoID,
		GeneratedAt: now.UTC(),
		Objects:     make(map[string][]Record, len(TrackedTypes)),
	}

	for _, t := range TrackedTypes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		records := make([]Record, 0)
		err := lister.List(ctx, t, func(id restic.ID, size int64) error {
			records = append(records, Record{ID: id.String(), Size: size})
			return nil
		})
		if err != nil {
			return nil, errors.Wrapf(err, "listing %q objects", t)
		}

		sort.Slice(records, func(i, j int) bool {
			return records[i].ID < records[j].ID
		})
		ledger.Objects[t.String()] = records
	}

	return ledger, nil
}

// Count returns the total number of recorded objects.
func (l *Ledger) Count() int {
	count := 0
	for _, records := range l.Objects {
		count += len(records)
	}
	return count
}

// Ref identifies an object by its type and ID.
type Ref struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// ReplacedObject describes an object that still exists under the same type
// and ID but whose size changed since the previous inventory.
type ReplacedObject struct {
	Ref
	OldSize int64 `json:"old_size"`
	NewSize int64 `json:"new_size"`
}

// Diff describes how the objects in a repository changed between two
// inventories.
type Diff struct {
	// Removed contains objects present in the old inventory but missing from
	// the new one.
	Removed []Ref
	// Added contains objects present in the new inventory but not in the old
	// one. These are expected when another backup ran concurrently and are not
	// considered a suspicious change.
	Added []Ref
	// Replaced contains objects with the same type and ID but a different
	// size.
	Replaced []ReplacedObject
}

// Changed reports whether the diff contains suspicious changes, that is
// removed or replaced objects. Added objects are intentionally ignored.
func (d Diff) Changed() bool {
	return len(d.Removed) > 0 || len(d.Replaced) > 0
}

// Compare computes the difference between an old and a new inventory.
func Compare(old, new *Ledger) Diff {
	diff := Diff{
		Removed:  []Ref{},
		Added:    []Ref{},
		Replaced: []ReplacedObject{},
	}

	for _, t := range TrackedTypes {
		typeName := t.String()
		oldRecords := indexRecords(old.Objects[typeName])
		newRecords := indexRecords(new.Objects[typeName])

		for id, current := range newRecords {
			previous, ok := oldRecords[id]
			switch {
			case !ok:
				diff.Added = append(diff.Added, Ref{Type: typeName, ID: id})
			case previous.Size != current.Size:
				diff.Replaced = append(diff.Replaced, ReplacedObject{
					Ref:     Ref{Type: typeName, ID: id},
					OldSize: previous.Size,
					NewSize: current.Size,
				})
			}
		}

		for id := range oldRecords {
			if _, ok := newRecords[id]; !ok {
				diff.Removed = append(diff.Removed, Ref{Type: typeName, ID: id})
			}
		}
	}

	sortRefs(diff.Removed)
	sortRefs(diff.Added)
	sort.Slice(diff.Replaced, func(i, j int) bool {
		if diff.Replaced[i].Type != diff.Replaced[j].Type {
			return diff.Replaced[i].Type < diff.Replaced[j].Type
		}
		return diff.Replaced[i].ID < diff.Replaced[j].ID
	})

	return diff
}

func indexRecords(records []Record) map[string]Record {
	index := make(map[string]Record, len(records))
	for _, record := range records {
		index[record.ID] = record
	}
	return index
}

func sortRefs(refs []Ref) {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Type != refs[j].Type {
			return refs[i].Type < refs[j].Type
		}
		return refs[i].ID < refs[j].ID
	})
}

// Load reads and validates an inventory from path. It returns an error
// wrapping os.ErrNotExist if the file does not exist.
func Load(path string) (*Ledger, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, "read inventory")
	}

	var ledger Ledger
	if err := json.Unmarshal(buf, &ledger); err != nil {
		return nil, errors.Wrap(err, "parse inventory")
	}

	if ledger.Version != CurrentVersion {
		return nil, errors.Errorf("unsupported inventory format version %d, expected %d", ledger.Version, CurrentVersion)
	}
	if ledger.Objects == nil {
		return nil, errors.New("inventory is missing the objects list")
	}

	return &ledger, nil
}

// Save atomically writes the inventory to path. The data is first written to
// a temporary file in the same directory and only renamed into place after it
// has been written and synced completely. A failure while enumerating or
// writing therefore never leaves a truncated or partially updated inventory
// at path.
func (l *Ledger) Save(path string) (err error) {
	buf, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return errors.Wrap(err, "marshal inventory")
	}
	buf = append(buf, '\n')

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return errors.Wrap(err, "create inventory directory")
	}

	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return errors.Wrap(err, "create temporary inventory file")
	}
	tmpName := tmp.Name()

	defer func() {
		if err != nil {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err = tmp.Write(buf); err != nil {
		_ = tmp.Close()
		return errors.Wrap(err, "write inventory")
	}

	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return errors.Wrap(err, "sync inventory")
	}

	// Close before rename, Windows does not support renaming open files.
	if err = tmp.Close(); err != nil {
		return errors.Wrap(err, "close inventory")
	}

	if err = os.Chmod(tmpName, 0o600); err != nil {
		return errors.Wrap(err, "chmod inventory")
	}

	if err = os.Rename(tmpName, path); err != nil {
		return errors.Wrap(err, "replace inventory")
	}

	return nil
}
