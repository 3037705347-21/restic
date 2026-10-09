package repository

import (
	"context"
	"sort"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/repository/pack"
	"github.com/restic/restic/internal/restic"
)

// QuarantineStage is the repair stage of a quarantined pack, as seen by the
// ledger-driven repair executor.
type QuarantineStage uint8

const (
	// QuarantineRegistered waits for salvage (re-upload of intact blobs).
	QuarantineRegistered QuarantineStage = iota + 1
	// QuarantineSalvaged has its intact blobs re-uploaded.
	QuarantineSalvaged
	// QuarantineRewritten has been excluded from the index and waits for the
	// physical file to be deleted.
	QuarantineRewritten
)

// QuarantineEntry is one damaged pack as recorded by the verification ledger.
type QuarantineEntry struct {
	PackID restic.ID
	Stage  QuarantineStage
	// Salvageable lists the blobs that were still readable when check found
	// the pack damaged; it limits what salvage is allowed to re-upload.
	Salvageable []restic.BlobHandle
}

// QuarantineStore is the ledger surface consumed by RepairPacksFromLedger.
// Every transition must be durable before it is reported as complete.
type QuarantineStore interface {
	// QuarantineRecords returns all records awaiting or undergoing repair.
	QuarantineRecords() []QuarantineEntry
	// MarkSalvaged moves one registered pack to the salvaged stage.
	MarkSalvaged(id restic.ID) error
	// MarkRewritten moves the given salvaged packs to the rewritten stage.
	MarkRewritten(ids restic.IDSet) error
	// Remove deletes the record after its pack file has been removed.
	Remove(id restic.ID) error
}

// RepairPacksFromLedger drives the resumable repair plan recorded by check:
//
//   - packs that vanished from the backend are simply forgotten (their index
//     exclusion was already persisted by an earlier run),
//   - registered packs have their salvageable blobs re-uploaded and move to
//     salvaged,
//   - all salvaged packs are excluded from a freshly rewritten index and move
//     to rewritten,
//   - rewritten pack files are deleted and their records removed.
//
// Each transition is persisted in the ledger before the next stage starts, so
// interrupting and re-running the command never repeats a completed stage.
func RepairPacksFromLedger(ctx context.Context, repo *Repository, store QuarantineStore, printer restic.Printer) error {
	entries := store.QuarantineRecords()
	if len(entries) == 0 {
		printer.S("no quarantined packs in the verification ledger\n")
		return nil
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].PackID.String() < entries[j].PackID.String()
	})

	// packs that need salvage are parsed during the single backend listing;
	// packs at later stages only need presence information
	salvageWanted := restic.NewIDSet()
	for _, e := range entries {
		if e.Stage == QuarantineRegistered {
			salvageWanted.Insert(e.PackID)
		}
	}

	// list the backend exactly once (backends like S3 may serve inconsistent
	// repeated listings): the same listing drives presence checks, size
	// information and the pack header parsing used for salvage
	physical, headerBlobsSalvage, err := listPacksOnce(ctx, repo, salvageWanted)
	if err != nil {
		return err
	}
	physicallyPresent := restic.NewIDSet()
	for id := range physical {
		physicallyPresent.Insert(id)
	}

	// clean up records of packs that vanished entirely
	remaining := make([]QuarantineEntry, 0, len(entries))
	for _, e := range entries {
		if !physicallyPresent.Has(e.PackID) {
			printer.S("pack %v no longer exists, removing its ledger entry\n", e.PackID)
			if err := store.Remove(e.PackID); err != nil {
				return err
			}
			continue
		}
		remaining = append(remaining, e)
	}
	if len(remaining) == 0 {
		return nil
	}

	// Stage 1: salvage registered packs, marking each pack after the uploader
	// flushed
	registered := restic.NewIDSet()
	for _, e := range remaining {
		if e.Stage == QuarantineRegistered {
			registered.Insert(e.PackID)
		}
	}
	if len(registered) > 0 {
		indexBlobs, err := blobsForPacks(ctx, repo, registered)
		if err != nil {
			return err
		}
		// header blobs were parsed during the single backend listing
		headerBlobs := headerBlobsSalvage

		var salvagedIDs []restic.ID
		err = repo.WithBlobUploader(ctx, func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
			bar := printer.NewCounter("salvaged pack files")
			defer bar.Done()
			for _, e := range remaining {
				if e.Stage != QuarantineRegistered {
					continue
				}
				wanted := restic.NewBlobSet(e.Salvageable...)
				blobs := intersectSalvage(wanted, indexBlobs[e.PackID], headerBlobs[e.PackID])
				if len(blobs) == 0 {
					printer.E("pack %v has no salvageable blobs that could be located\n", e.PackID)
				} else {
					printer.P("salvaging %d intact blob(s) from damaged pack %v\n", len(blobs), e.PackID)
					if err := reuploadBlobsFromPack(ctx, repo, e.PackID, blobs, printer, uploader); err != nil {
						return err
					}
				}
				salvagedIDs = append(salvagedIDs, e.PackID)
				bar.Add(1)
			}
			return nil
		})
		if err != nil {
			return err
		}
		// only advance the stage after the uploader flushed everything, so a
		// failed upload can never look like a completed salvage on retry
		for _, id := range salvagedIDs {
			if err := store.MarkSalvaged(id); err != nil {
				return err
			}
		}
	}

	// Stage 2: exclude every pack that is now salvaged from a fresh index
	toRewrite := restic.NewIDSet()
	for _, e := range store.QuarantineRecords() {
		if e.Stage == QuarantineSalvaged && physicallyPresent.Has(e.PackID) {
			toRewrite.Insert(e.PackID)
		}
	}
	if len(toRewrite) > 0 {
		printer.P("rewriting repository index to exclude %d salvaged pack(s)\n", len(toRewrite))
		if err := rewriteIndexFiles(ctx, repo, toRewrite, nil, nil, printer); err != nil {
			return err
		}
		if err := store.MarkRewritten(toRewrite); err != nil {
			return err
		}
	} else {
		debug.Log("ledger repair: no packs waiting for index rewrite")
	}

	// Stage 3: delete the physical files and remove their records
	toDelete := restic.NewIDSet()
	for _, e := range store.QuarantineRecords() {
		if e.Stage == QuarantineRewritten && physicallyPresent.Has(e.PackID) {
			toDelete.Insert(e.PackID)
		}
	}
	if len(toDelete) > 0 {
		printer.S("removing %d salvaged pack file(s)\n", len(toDelete))
		bar := printer.NewCounter("files deleted")
		defer bar.Done()
		deleteFailed := restic.NewIDSet()
		err = restic.ParallelRemove(ctx, &internalRepository{repo}, toDelete, restic.PackFile,
			func(id restic.ID, err error) error {
				if err != nil {
					deleteFailed.Insert(id)
					printer.E("failed to delete pack %v: %v\n", id, err)
				}
				return nil
			}, bar)
		if err != nil {
			return err
		}
		// only clear records of packs that are actually gone; failed deletes
		// keep their rewritten stage and are retried on the next run
		for id := range toDelete {
			if deleteFailed.Has(id) {
				continue
			}
			if err := store.Remove(id); err != nil {
				return err
			}
		}
	}

	return nil
}

// listPacksOnce lists every pack file exactly once and returns its size plus
// the parsed header blobs for the packs in salvageWanted. Broken pack headers
// are skipped here; salvage then falls back to the index blob entries.
func listPacksOnce(ctx context.Context, repo *Repository, salvageWanted restic.IDSet) (map[restic.ID]int64, map[restic.ID]pack.Blobs, error) {
	sizes := make(map[restic.ID]int64)
	headers := make(map[restic.ID]pack.Blobs)

	err := repo.List(ctx, restic.PackFile, func(id restic.ID, size int64) error {
		sizes[id] = size
		if salvageWanted.Has(id) {
			blobs, err := repo.listPack(ctx, id, size)
			if err != nil {
				// broken header: salvage can still use the index entries
				return nil
			}
			headers[id] = blobs
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return sizes, headers, nil
}

// blobsForPacks returns the current index entries for the given packs.
func blobsForPacks(ctx context.Context, repo *Repository, ids restic.IDSet) (map[restic.ID]pack.Blobs, error) {
	out := make(map[restic.ID]pack.Blobs, len(ids))
	for pbs := range repo.listPacksFromIndex(ctx, ids) {
		out[pbs.PackID] = pbs.Blobs
	}
	return out, nil
}

// intersectSalvage restricts salvage to blob handles recorded as salvageable
// by check and that can be located via either index entries or the pack
// header. When check recorded no handles (e.g. an unreadable pack), nothing is
// salvaged.
func intersectSalvage(wanted restic.BlobSet, lists ...pack.Blobs) pack.Blobs {
	if wanted.Len() == 0 {
		return nil
	}
	out := make(pack.Blobs, 0, wanted.Len())
	seen := restic.NewBlobSet()
	for _, list := range lists {
		for _, b := range list {
			if wanted.Has(b.BlobHandle) && !seen.Has(b.BlobHandle) {
				seen.Insert(b.BlobHandle)
				out = append(out, b)
			}
		}
	}
	return out
}
