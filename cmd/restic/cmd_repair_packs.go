package main

import (
	"context"
	"os"

	"github.com/spf13/cobra"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/ui/progress"
)

// RepairPacksOptions are the options for the "repair packs" command.
type RepairPacksOptions struct {
	FromLedger bool
	LedgerDir  string
}

func newRepairPacksCommand(globalOptions *global.Options) *cobra.Command {
	var opts RepairPacksOptions

	cmd := &cobra.Command{
		Use:   "packs [packIDs...]",
		Short: "Salvage damaged pack files",
		Long: `
The "repair packs" command extracts intact blobs from the specified pack files,
rebuilds the index to remove the damaged pack files and removes the pack files
from the repository.

With --from-ledger (experimental, needs
--feature-flags=verification-ledger=true) it instead repairs exactly the packs
that a "check --resume --read-data" run recorded in the verification ledger.
The repair is staged in the ledger, so an interrupted run continues without
repeating completed stages. Run "repair snapshots --forget" afterwards to
remove snapshots that reference blobs which could not be salvaged.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRepairPacks(cmd.Context(), *globalOptions, opts, globalOptions.Term, args)
		},
	}

	cmd.Flags().BoolVar(&opts.FromLedger, "from-ledger", false, "repair the packs recorded by `check --resume --read-data` in the verification ledger, continuing an interrupted repair instead of taking pack IDs")
	cmd.Flags().StringVar(&opts.LedgerDir, "ledger-dir", "", "use `dir` as the verification ledger location instead of the persistent cache directory")
	return cmd
}

func runRepairPacks(ctx context.Context, gopts global.Options, opts RepairPacksOptions, term ui.Terminal, args []string) error {
	if opts.FromLedger {
		if !feature.Flag.Enabled(feature.VerificationLedger) {
			return errors.Fatal("the verification ledger is experimental: enable it with --feature-flags=verification-ledger=true")
		}
		if len(args) > 0 {
			return errors.Fatal("--from-ledger cannot be combined with explicit pack IDs")
		}
		return runRepairPacksFromLedger(ctx, gopts, opts, term)
	}
	if opts.LedgerDir != "" {
		return errors.Fatal("--ledger-dir only applies together with --from-ledger")
	}
	return runRepairPacksManual(ctx, gopts, term, args)
}

func runRepairPacksManual(ctx context.Context, gopts global.Options, term ui.Terminal, args []string) error {
	printer := progress.NewTerminalPrinter(false, gopts.Verbosity, term)

	ids := restic.NewIDSet()
	for _, arg := range args {
		id, err := restic.ParseID(arg)
		if err != nil {
			return errors.Fatal("invalid id: " + arg)
		}
		ids.Insert(id)
	}
	if len(ids) == 0 {
		return errors.Fatal("no ids specified")
	}

	ctx, repo, unlock, err := openWithExclusiveLock(ctx, gopts, false, printer)
	if err != nil {
		return err
	}
	defer unlock()

	printer.P("saving backup copies of pack files to current folder")
	for id := range ids {
		buf, err := repo.LoadRaw(ctx, restic.PackFile, id)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// only skip creating a copy if no data at all could be loaded
		if buf == nil && err != nil {
			printer.E("will remove packfile %v due to failed download: %v", id, err)
			continue
		}
		f, err := os.OpenFile("pack-"+id.String(), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o666)
		if err != nil {
			return err
		}
		if _, err := f.Write(buf); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}

	err = repository.RepairPacks(ctx, repo, ids, printer)
	if err != nil {
		return errors.Fatalf("%s", err)
	}

	printer.E("\nUse `restic repair snapshots --forget` to remove the corrupted data blobs from all snapshots\n")
	return nil
}

func runRepairPacksFromLedger(ctx context.Context, gopts global.Options, opts RepairPacksOptions, term ui.Terminal) error {
	printer := progress.NewTerminalPrinter(false, gopts.Verbosity, term)

	ctx, repo, unlock, err := openWithExclusiveLock(ctx, gopts, false, printer)
	if err != nil {
		return err
	}
	defer unlock()

	dir, err := resolveLedgerDir(opts.LedgerDir, persistentCacheBase(gopts), repo.Config().ID)
	if err != nil {
		return err
	}
	store, err := openLedgerStore(dir)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	before := len(store.Quarantine())
	if before == 0 {
		printer.S("no quarantined packs in the verification ledger\n")
		return nil
	}
	printer.S("repairing %d quarantined pack(s) using ledger %v\n", before, dir)
	err = repository.RepairPacksFromLedger(ctx, repo, &ledgerQuarantineStore{store: store}, printer)
	if err != nil {
		return errors.Fatalf("%s", err)
	}

	remaining := len(store.Quarantine())
	printer.S("ledger repair finished: %d pack(s) repaired, %d remaining\n", before-remaining, remaining)
	if remaining > 0 {
		// packs that could not be deleted are retried on the next run
		return errors.Fatalf("%d quarantined pack(s) remain; re-run the command to retry", remaining)
	}
	printer.E("\nUse `restic repair snapshots --forget` to remove the corrupted data blobs from all snapshots\n")
	return nil
}
