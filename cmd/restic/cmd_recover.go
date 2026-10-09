package main

import (
	"context"
	"os"
	"slices"
	"sort"
	"time"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/ui/progress"
	"github.com/spf13/cobra"
)

const (
	recoverPath = "/recover"
	recoverTag  = "recovered"
)

func newRecoverCommand(globalOptions *global.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "recover [flags]",
		Short: "Recover data from the repository not referenced by snapshots",
		Long: `
The "recover" command builds a new snapshot from all directories it can find in
the raw data of the repository which are not referenced in an existing snapshot.
It can be used if, for example, a snapshot has been removed by accident with "forget".

A persistent recovery record is stored in the repository. Roots that have
already been recovered are not recovered again and the record is updated when
previously recovered roots are referenced by other snapshots.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRecover(cmd.Context(), *globalOptions, globalOptions.Term)
		},
	}
	return cmd
}

func runRecover(ctx context.Context, gopts global.Options, term ui.Terminal) error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	printer := progress.NewTerminalPrinter(false, gopts.Verbosity, term)
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, gopts, false, printer)
	if err != nil {
		return err
	}
	defer unlock()

	// Load the persistent recovery record before making any changes.
	records, recordFileIDs, err := restic.LoadRecoveryRecords(ctx, repo)
	if err != nil {
		return err
	}
	record, generation, created := selectRecoveryRecord(records)
	recorded := restic.NewIDSet()
	if record != nil {
		for _, id := range record.Roots {
			recorded.Insert(id)
		}
	}

	snapshotLister, err := restic.MemorizeList(ctx, repo, restic.SnapshotFile)
	if err != nil {
		return err
	}

	printer.P("ensuring index is complete\n")
	err = repository.RepairIndex(ctx, repo, repository.RepairIndexOptions{}, printer)
	if err != nil {
		return err
	}

	printer.P("load index files\n")
	if err = repo.LoadIndex(ctx, printer); err != nil {
		return err
	}

	// Classify snapshots: snapshots tagged as recovered are recovery snapshots
	// created by this command, all other snapshots are snapshots that belong
	// to other users and must never be modified.
	realSnapshotRoots := restic.NewIDSet()
	recoveryAggregateRoots := restic.NewIDSet()

	printer.P("load snapshots\n")
	err = data.ForAllSnapshots(ctx, snapshotLister, repo, nil, func(_ restic.ID, sn *data.Snapshot, loadErr error) error {
		if loadErr != nil {
			return loadErr
		}
		if sn.Tree == nil {
			return nil
		}
		if sn.HasTags([]string{recoverTag}) {
			recoveryAggregateRoots.Insert(*sn.Tree)
		} else {
			realSnapshotRoots.Insert(*sn.Tree)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// trees maps a tree ID to whether or not it is referenced by a different
	// tree. If it is not referenced, we have a root tree candidate.
	trees := make(map[restic.ID]bool)
	// children lists the subtrees of each tree, used to determine which trees
	// are reachable from the snapshot roots and which roots are already covered
	// by existing recovery snapshots.
	children := make(map[restic.ID]restic.IDs)

	err = repo.ListBlobs(ctx, func(blob restic.PackBlob) {
		h := blob.Handle()
		if h.Type == restic.TreeBlob {
			trees[h.ID] = false
		}
	})
	if err != nil {
		return err
	}

	printer.P("load %d trees\n", len(trees))
	bar := printer.NewCounter("trees loaded")
	bar.SetMax(uint64(len(trees)))
	for id := range trees {
		tree, err := data.LoadTree(ctx, repo, id)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			printer.E("unable to load tree %v: %v\n", id.Str(), err)
			continue
		}

		for item := range tree {
			if item.Error != nil {
				return item.Error
			}
			node := item.Node
			if node.Type == data.NodeTypeDir && node.Subtree != nil {
				trees[*node.Subtree] = true
				children[id] = append(children[id], *node.Subtree)
			}
		}
		bar.Add(1)
	}
	bar.Done()
	printer.P("done\n")

	// Determine the trees reachable from non-recovery snapshots.
	realReachable := reachableTrees(realSnapshotRoots, children)

	// roots already covered by existing recovery snapshots: the direct
	// children of the recovery snapshot aggregate trees.
	recoveredRoots := restic.NewIDSet()
	for aggregateID := range recoveryAggregateRoots {
		for _, subtreeID := range children[aggregateID] {
			recoveredRoots.Insert(subtreeID)
		}
	}

	// Unreferenced roots: trees not referenced by any other tree, not
	// reachable from a non-recovery snapshot and not the aggregate tree of a
	// recovery snapshot.
	unreferencedRoots := restic.NewIDSet()
	for id, referenced := range trees {
		if referenced || realReachable.Has(id) || recoveryAggregateRoots.Has(id) {
			continue
		}
		printer.V("found root tree %v\n", id.Str())
		unreferencedRoots.Insert(id)
	}
	printer.S("\nfound %d unreferenced roots\n", len(unreferencedRoots))

	// Roots that need a new recovery snapshot: unreferenced roots that are
	// neither recorded nor already covered by a recovery snapshot.
	newRoots := restic.NewIDSet()
	for id := range unreferencedRoots {
		if !recorded.Has(id) && !recoveredRoots.Has(id) {
			newRoots.Insert(id)
		}
	}

	// Build the next record contents.
	// Recorded roots converge (are removed) when they are referenced again by a
	// non-recovery snapshot or when the tree itself no longer exists.
	nextRoots := restic.NewIDSet()
	for id := range recorded {
		_, exists := trees[id]
		if !exists || realReachable.Has(id) {
			continue
		}
		nextRoots.Insert(id)
	}
	// Roots covered by existing recovery snapshots (e.g. recovery snapshots
	// created before the record existed) become tracked as well.
	for id := range recoveredRoots {
		_, exists := trees[id]
		if !exists || realReachable.Has(id) {
			continue
		}
		nextRoots.Insert(id)
	}
	nextRoots.Merge(unreferencedRoots)

	// The record needs to be (re)written when the tracked roots changed, when
	// there is no record yet but roots to track, or when duplicate record
	// files exist and must be consolidated.
	sortedRecorded := recorded.List()
	sort.Sort(sortedRecorded)
	sortedNext := nextRoots.List()
	sort.Sort(sortedNext)
	recordChanged := !slices.Equal(sortedRecorded, sortedNext)
	mustUpdateRecord := recordChanged || len(recordFileIDs) > 1
	if record == nil && !mustUpdateRecord {
		printer.P("no snapshot to write.\n")
		return nil
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Create a recovery snapshot for the new roots. The aggregate tree is built
	// deterministically (sorted names, fixed metadata) and snapshots are only
	// created after the unreferenced roots are known.
	if len(newRoots) > 0 {
		sortedNew := newRoots.List()
		sort.Sort(sortedNew)

		var treeID restic.ID
		err = repo.WithBlobUploader(ctx, func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
			tw := data.NewTreeWriter(uploader)
			for _, id := range sortedNew {
				var subtreeID = id
				node := data.Node{
					Type:    data.NodeTypeDir,
					Name:    id.Str(),
					Mode:    0755,
					Subtree: &subtreeID,
					// Use fixed timestamps so that aggregate trees for the same
					// set of roots are identical.
				}
				err := tw.AddNode(&node)
				if err != nil {
					return err
				}
			}

			var err error
			treeID, err = tw.Finalize(ctx)
			if err != nil {
				return errors.Fatalf("unable to save new tree to the repository: %v", err)
			}
			return nil
		})
		if err != nil {
			return err
		}

		if err = createSnapshot(ctx, printer, recoverPath, hostname, []string{recoverTag}, repo, &treeID); err != nil {
			return err
		}
	}

	// Persist the updated recovery record. The new record is written first and
	// the previous record files are removed afterwards, so the record is always
	// present and readable. Any failure while persisting the record fails the
	// whole command; a subsequent run will adopt existing recovery snapshots
	// and retry. If there are no roots left to track, the obsolete record files
	// are removed instead.
	if !mustUpdateRecord {
		return nil
	}

	if len(nextRoots) == 0 {
		for _, oldID := range recordFileIDs {
			if err = repo.RemoveUnpacked(ctx, restic.WriteableRecoveryFile, oldID); err != nil {
				return errors.Fatalf("unable to remove obsolete recovery record: %v", err)
			}
		}
		return nil
	}

	now := time.Now()
	if record == nil {
		created = now
	}
	newRecord := restic.NewRecoveryRecord(generation+1, created, now, sortedNext)
	_, err = restic.SaveRecoveryRecord(ctx, repo, newRecord)
	if err != nil {
		return errors.Fatalf("unable to save recovery record: %v", err)
	}

	for _, oldID := range recordFileIDs {
		if err = repo.RemoveUnpacked(ctx, restic.WriteableRecoveryFile, oldID); err != nil {
			return errors.Fatalf("unable to remove old recovery record: %v", err)
		}
	}

	return nil
}

// reachableTrees returns all trees reachable from the given root trees via the
// provided parent-to-children mapping. The roots themselves are included.
func reachableTrees(roots restic.IDSet, children map[restic.ID]restic.IDs) restic.IDSet {
	reachable := restic.NewIDSet()
	queue := append(restic.IDs{}, roots.List()...)
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if reachable.Has(id) {
			continue
		}
		reachable.Insert(id)
		queue = append(queue, children[id]...)
	}
	return reachable
}

// selectRecoveryRecord selects the newest record (highest generation, ties
// broken by the update time) from the given records. It also returns the
// selected generation and creation time.
func selectRecoveryRecord(records []*restic.RecoveryRecord) (record *restic.RecoveryRecord, generation uint64, created time.Time) {
	for _, candidate := range records {
		if record == nil || candidate.Generation > generation ||
			(candidate.Generation == generation && candidate.Updated.After(record.Updated)) {
			record = candidate
			generation = candidate.Generation
			created = candidate.Created
		}
	}
	return record, generation, created
}

func createSnapshot(ctx context.Context, printer restic.Printer, name, hostname string, tags []string, repo restic.SaverUnpacked[restic.WriteableFileType], tree *restic.ID) error {
	sn, err := data.NewSnapshot([]string{name}, tags, hostname, time.Now())
	if err != nil {
		return errors.Fatalf("unable to save snapshot: %v", err)
	}

	sn.Tree = tree

	id, err := data.SaveSnapshot(ctx, repo, sn)
	if err != nil {
		return errors.Fatalf("unable to save snapshot: %v", err)
	}

	printer.S("saved new snapshot %v\n", id.Str())
	return nil
}
