package main

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/restic/restic/internal/copier"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/ui/progress"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newCopyCommand(globalOptions *global.Options) *cobra.Command {
	var opts CopyOptions
	cmd := &cobra.Command{
		Use:   "copy [flags] [snapshotID ...]",
		Short: "Copy snapshots from one repository to another",
		Long: `
The "copy" command copies one or more snapshots from one repository to another.

NOTE: This process will have to both download (read) and upload (write) the
entire snapshot(s) due to the different encryption keys used in the source and
destination repositories. This /may incur higher bandwidth usage and costs/ than
expected during normal backup runs.

NOTE: The copying process does not re-chunk files, which may break deduplication
between the files copied and files already stored in the destination repository.
This means that copied files, which existed in both the source and destination
repository, /may occupy up to twice their space/ in the destination repository.
This can be mitigated by the "--copy-chunker-params" option when initializing a
new destination repository using the "init" command.

The copy progress is recorded in the destination repository. Each source
snapshot is only skipped once its copy is complete, and a copy run that is
interrupted (for example by a network failure) only finishes the snapshots that
were not completed yet. Snapshots created manually in the destination
repository are never modified or removed. If the progress records cannot be
written, the command fails instead of copying without bookkeeping.

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
		RunE: func(cmd *cobra.Command, args []string) error {
			finalizeSnapshotFilter(&opts.SnapshotFilter)
			return runCopy(cmd.Context(), opts, *globalOptions, args, globalOptions.Term)
		},
	}

	opts.AddFlags(cmd.Flags())
	return cmd
}

// CopyOptions bundles all options for the copy command.
type CopyOptions struct {
	global.SecondaryRepoOptions
	data.SnapshotFilter
}

func (opts *CopyOptions) AddFlags(f *pflag.FlagSet) {
	opts.SecondaryRepoOptions.AddFlags(f, "destination", "to copy snapshots from")
	initMultiSnapshotFilter(f, &opts.SnapshotFilter, true)
}

var errSentinelEndIteration = errors.New("end iteration")

// snapshotWork is a source snapshot selected for copying together with its
// persistent identity (the Original ID used when it is copied).
type snapshotWork struct {
	sn       *data.Snapshot
	original restic.ID
}

// collectAllSnapshots selects the source snapshots that still need copying.
// The progress ledger stored in the destination repository is authoritative:
//   - snapshots recorded as complete and still present at the destination are
//     skipped without reading or transferring their data;
//   - copies made by older restic versions (recognized via the Original field)
//     are imported into the ledger and skipped as well;
//   - everything else is yielded for copying, including snapshots whose data
//     was already copied before an interrupted run.
func collectAllSnapshots(ctx context.Context, opts CopyOptions,
	srcSnapshotLister restic.Lister, srcRepo restic.Repository,
	dstSnapshotByOriginal map[restic.ID][]*data.Snapshot,
	ledger *copier.Ledger, targetSnapshotIDs restic.IDSet,
	args []string, printer restic.Printer,
) iter.Seq2[snapshotWork, error] {
	targetExists := targetSnapshotIDs.Has
	return func(yield func(snapshotWork, error) bool) {
		err := opts.SnapshotFilter.FindAll(ctx, srcSnapshotLister, srcRepo, args, func(_ string, sn *data.Snapshot, err error) error {
			if err != nil {
				if !yield(snapshotWork{}, err) {
					return errSentinelEndIteration
				}
				return nil
			}

			srcID := *sn.ID()
			originalID := srcID
			if sn.Original != nil {
				originalID = *sn.Original
			}

			// authoritative decision from the destination progress ledger
			if entry, ok := ledger.Get(srcID); ok {
				if entry.Stage == copier.StageComplete && entry.TargetID != nil && targetExists(*entry.TargetID) {
					printer.V("\n%v", sn)
					printer.V("skipping source snapshot %s, copy already recorded as complete (destination snapshot %s)",
						srcID.Str(), entry.TargetID.Str())
					return nil
				}
				if entry.Stage == copier.StageComplete {
					// the copied snapshot vanished from the destination, copy it again
					printer.V("recorded destination snapshot for source snapshot %s no longer exists, copying again",
						srcID.Str())
					ledger.Forget(srcID)
				}
				// stage "data-copied": copyTree below skips blobs already present,
				// only the snapshot metadata still has to be saved
				if !yield(snapshotWork{sn: sn, original: originalID}, nil) {
					return errSentinelEndIteration
				}
				return nil
			}

			// compatibility: recognize copies made by restic versions without
			// a progress ledger and import them as complete
			if originalSns, ok := dstSnapshotByOriginal[originalID]; ok {
				for _, existing := range originalSns {
					if similarSnapshots(existing, sn) {
						printer.V("\n%v", sn)
						printer.V("skipping source snapshot %s, was already copied to snapshot %s; recording it in the progress ledger",
							srcID.Str(), existing.ID().Str())
						if err := ledger.Mark(srcID, originalID, copier.StageComplete, existing.ID()); err != nil {
							return err
						}
						return nil
					}
				}
			}

			if !yield(snapshotWork{sn: sn, original: originalID}, nil) {
				return errSentinelEndIteration
			}
			return nil
		})
		if err != nil && !errors.Is(err, errSentinelEndIteration) {
			yield(snapshotWork{}, err)
		}
	}
}

func runCopy(ctx context.Context, opts CopyOptions, gopts global.Options, args []string, term ui.Terminal) error {
	printer := progress.NewTerminalPrinter(false, gopts.Verbosity, term)
	secondaryGopts, isFromRepo, err := opts.SecondaryRepoOptions.FillGlobalOpts(ctx, gopts, "destination")
	if err != nil {
		return err
	}
	if isFromRepo {
		// swap global options, if the secondary repo was set via from-repo
		gopts, secondaryGopts = secondaryGopts, gopts
	}

	ctx, srcRepo, unlock, err := openWithReadLock(ctx, gopts, gopts.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()

	ctx, dstRepo, unlock, err := openWithAppendLock(ctx, secondaryGopts, false, printer)
	if err != nil {
		return err
	}
	defer unlock()

	srcSnapshotLister, err := restic.MemorizeList(ctx, srcRepo, restic.SnapshotFile)
	if err != nil {
		return err
	}

	dstSnapshotLister, err := restic.MemorizeList(ctx, dstRepo, restic.SnapshotFile)
	if err != nil {
		return err
	}

	debug.Log("Loading source index")
	if err := srcRepo.LoadIndex(ctx, printer); err != nil {
		return err
	}
	debug.Log("Loading destination index")
	if err := dstRepo.LoadIndex(ctx, printer); err != nil {
		return err
	}

	dstSnapshotByOriginal := make(map[restic.ID][]*data.Snapshot)
	dstSnapshotIDs := restic.NewIDSet()
	err = opts.SnapshotFilter.FindAll(ctx, dstSnapshotLister, dstRepo, nil, func(_ string, sn *data.Snapshot, err error) error {
		if err != nil {
			return err
		}
		dstSnapshotIDs.Insert(*sn.ID())
		if sn.Original != nil && !sn.Original.IsNull() {
			dstSnapshotByOriginal[*sn.Original] = append(dstSnapshotByOriginal[*sn.Original], sn)
		}
		// also consider identical snapshot copies
		dstSnapshotByOriginal[*sn.ID()] = append(dstSnapshotByOriginal[*sn.ID()], sn)
		return nil
	})
	if err != nil {
		return err
	}

	debug.Log("Loading copy progress ledger")
	ledger, journalFiles, err := copier.LoadLedger(ctx, dstRepo)
	if err != nil {
		return fmt.Errorf("loading copy progress ledger failed: %w", err)
	}

	selectedSnapshots := collectAllSnapshots(ctx, opts, srcSnapshotLister, srcRepo,
		dstSnapshotByOriginal, ledger, dstSnapshotIDs, args, printer)

	// persistLedger writes the current in-memory ledger to the destination. A
	// failure is fatal: copying must not continue when progress cannot be
	// recorded.
	persistLedger := func() error {
		if ledger.Empty() {
			return nil
		}
		id, err := ledger.Save(ctx, dstRepo)
		if err != nil {
			return fmt.Errorf("failed to persist copy progress to destination repository: %w", err)
		}
		journalFiles.Insert(id)
		return nil
	}

	if err := copyTreeBatched(ctx, srcRepo, dstRepo, selectedSnapshots, ledger,
		dstSnapshotIDs, dstSnapshotByOriginal, persistLedger, printer); err != nil {
		return err
	}

	// consolidate the append-only journal into a single state file. Failing to
	// write it is fatal; failures removing obsolete files only produce warnings
	// because their entries remain safely recorded.
	removeWarnings, err := ledger.Compact(ctx, dstRepo, journalFiles)
	if err != nil {
		return err
	}
	for _, warning := range removeWarnings {
		printer.E("%v", warning)
	}

	return ctx.Err()
}

func similarSnapshots(sna *data.Snapshot, snb *data.Snapshot) bool {
	// everything except Parent and Original must match
	if !sna.Time.Equal(snb.Time) || !sna.Tree.Equal(*snb.Tree) || sna.Hostname != snb.Hostname ||
		sna.Username != snb.Username || sna.UID != snb.UID || sna.GID != snb.GID ||
		len(sna.Paths) != len(snb.Paths) || len(sna.Excludes) != len(snb.Excludes) ||
		len(sna.Tags) != len(snb.Tags) {
		return false
	}
	if !sna.HasPaths(snb.Paths) || !sna.HasTags(snb.Tags) {
		return false
	}
	for i, a := range sna.Excludes {
		if a != snb.Excludes[i] {
			return false
		}
	}
	return true
}

// savedSnapshot records a snapshot that this run saved at the destination, so
// it can be adopted without saving it again.
type savedSnapshot struct {
	targetID restic.ID
	sn       *data.Snapshot
}

// copyTreeBatched copies multiple snapshots in one go. Snapshots are written after
// data equivalent to at least 10 packfiles was written.
func copyTreeBatched(ctx context.Context, srcRepo *repository.Repository, dstRepo restic.Repository,
	selectedSnapshots iter.Seq2[snapshotWork, error],
	ledger *copier.Ledger, dstSnapshotIDs restic.IDSet,
	dstSnapshotByOriginal map[restic.ID][]*data.Snapshot,
	persistLedger func() error, printer restic.Printer) error {

	// remember already processed trees across all snapshots
	visitedTrees := srcRepo.NewAssociatedBlobSet()

	targetSize := uint64(dstRepo.PackSize()) * 100
	minDuration := 1 * time.Minute

	// snapshots saved earlier in this run, indexed by the persistent Original ID
	savedInRun := make(map[restic.ID][]savedSnapshot)

	// use pull-based iterator to allow iteration in multiple steps
	next, stop := iter.Pull2(selectedSnapshots)
	defer stop()

	for {
		var batch []snapshotWork
		batchSize := uint64(0)
		startTime := time.Now()

		// call WithBlobUploader() once and then loop over all selectedSnapshots
		err := dstRepo.WithBlobUploader(ctx, func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
			for batchSize < targetSize || time.Since(startTime) < minDuration {
				w, err, ok := next()
				if err != nil {
					return err
				}
				if !ok {
					break
				}

				batch = append(batch, w)

				printer.P("\n%v", w.sn)
				printer.P("  copy started, this may take a while...")
				sizeBlobs, err := copyTree(ctx, srcRepo, dstRepo, visitedTrees, *w.sn.Tree, printer, uploader)
				if err != nil {
					return err
				}
				debug.Log("tree copied")
				batchSize += sizeBlobs
			}

			return nil
		})
		if err != nil {
			return err
		}

		// if no snapshots were processed in this batch, we're done
		if len(batch) == 0 {
			break
		}

		// durable boundary 1: packs and the destination index are flushed now,
		// so the snapshot data will not have to be transferred again. Record
		// this before writing any snapshot metadata.
		for _, w := range batch {
			if err := ledger.Mark(*w.sn.ID(), w.original, copier.StageDataCopied, nil); err != nil {
				return err
			}
		}
		if err := persistLedger(); err != nil {
			return err
		}

		// add a newline to separate saved snapshot messages from the other messages
		if len(batch) > 1 {
			printer.P("")
		}
		// save all the snapshots and mark them complete
		for _, w := range batch {
			targetID, err := copySaveSnapshot(ctx, w, dstRepo, ledger, dstSnapshotIDs, dstSnapshotByOriginal,
				savedInRun, printer)
			if err != nil {
				return err
			}
			if err := ledger.Mark(*w.sn.ID(), w.original, copier.StageComplete, &targetID); err != nil {
				return err
			}
		}
		// durable boundary 2: the snapshots are present at the destination
		if err := persistLedger(); err != nil {
			return err
		}
	}

	return nil
}

func copyTree(ctx context.Context, srcRepo *repository.Repository, dstRepo restic.Repository,
	visitedTrees restic.AssociatedBlobSet, rootTreeID restic.ID, printer restic.Printer, uploader restic.BlobSaverWithAsync) (uint64, error) {

	copyBlobs := srcRepo.NewAssociatedBlobSet()
	packList := restic.NewIDSet()
	var lock sync.Mutex

	enqueue := func(h restic.BlobHandle) {
		lock.Lock()
		defer lock.Unlock()
		if _, ok := dstRepo.LookupBlobSize(h); !ok {
			pb := srcRepo.LookupBlob(h)
			copyBlobs.Insert(h)
			for _, p := range pb {
				packList.Insert(p.PackID())
			}
		}
	}

	err := data.StreamTrees(ctx, srcRepo, restic.IDs{rootTreeID}, restic.NoopCounter, func(treeID restic.ID) bool {
		handle := restic.BlobHandle{ID: treeID, Type: restic.TreeBlob}
		visited := visitedTrees.Has(handle)
		visitedTrees.Insert(handle)
		return visited
	}, func(treeID restic.ID, err error, nodes data.TreeNodeIterator) error {
		if err != nil {
			return fmt.Errorf("LoadTree(%v) returned error %v", treeID.Str(), err)
		}

		// copy raw tree bytes to avoid problems if the serialization changes
		enqueue(restic.BlobHandle{ID: treeID, Type: restic.TreeBlob})

		for item := range nodes {
			if item.Error != nil {
				return item.Error
			}
			// Recursion into directories is handled by StreamTrees
			// Copy the blobs for this file.
			for _, blobID := range item.Node.Content {
				enqueue(restic.BlobHandle{Type: restic.DataBlob, ID: blobID})
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	sizeBlobs := copyStats(srcRepo, copyBlobs, packList, printer)
	bar := printer.NewCounter("packs copied")
	err = repository.CopyBlobs(ctx, srcRepo, dstRepo, uploader, packList, copyBlobs, bar, printer.P)
	if err != nil {
		return 0, errors.Fatalf("%s", err)
	}
	return sizeBlobs, nil
}

// copyStats: print statistics for the blobs to be copied
func copyStats(srcRepo restic.Repository, copyBlobs restic.AssociatedBlobSet, packList restic.IDSet, printer restic.Printer) uint64 {
	// count and size
	countBlobs := 0
	sizeBlobs := uint64(0)
	for blob := range copyBlobs.Keys() {
		for _, pb := range srcRepo.LookupBlob(blob) {
			countBlobs++
			sizeBlobs += uint64(pb.CiphertextLength())
			break
		}
	}

	printer.V("  copy %d blobs with disk size %s in %d packfiles\n",
		countBlobs, ui.FormatBytes(uint64(sizeBlobs)), len(packList))
	return sizeBlobs
}

// copySaveSnapshot saves the snapshot metadata at the destination. Before
// writing a new snapshot file it checks, in order, whether the snapshot is
// already recorded as complete, was saved earlier in this run, or exists at the
// destination (for example written by an older restic version or by a previous
// interrupted run that failed between snapshot saves). An existing snapshot is
// adopted, so snapshots are never duplicated.
func copySaveSnapshot(ctx context.Context, w snapshotWork, dstRepo restic.Repository,
	ledger *copier.Ledger, dstSnapshotIDs restic.IDSet,
	dstSnapshotByOriginal map[restic.ID][]*data.Snapshot,
	savedInRun map[restic.ID][]savedSnapshot, printer restic.Printer,
) (restic.ID, error) {
	sn := w.sn
	srcID := *sn.ID()

	if entry, ok := ledger.Get(srcID); ok && entry.Stage == copier.StageComplete &&
		entry.TargetID != nil && dstSnapshotIDs.Has(*entry.TargetID) {
		printer.V("snapshot %s already saved at destination as %s", srcID.Str(), entry.TargetID.Str())
		return *entry.TargetID, nil
	}

	for _, saved := range savedInRun[w.original] {
		if similarSnapshots(saved.sn, sn) {
			printer.V("snapshot %s already saved at destination as %s", srcID.Str(), saved.targetID.Str())
			return saved.targetID, nil
		}
	}

	for _, existing := range dstSnapshotByOriginal[w.original] {
		if similarSnapshots(existing, sn) {
			printer.V("snapshot %s already present at destination as %s, adopting existing snapshot",
				srcID.Str(), existing.ID().Str())
			dstSnapshotIDs.Insert(*existing.ID())
			return *existing.ID(), nil
		}
	}

	sn.Parent = nil // Parent does not have relevance in the new repo.
	// Use Original as a persistent snapshot ID
	if sn.Original == nil {
		sn.Original = sn.ID()
	}
	newID, err := data.SaveSnapshot(ctx, dstRepo, sn)
	if err != nil {
		return restic.ID{}, err
	}
	printer.P("snapshot %s saved, copied from source snapshot %s", newID.Str(), srcID.Str())
	dstSnapshotIDs.Insert(newID)
	savedInRun[w.original] = append(savedInRun[w.original], savedSnapshot{targetID: newID, sn: sn})
	return newID, nil
}
