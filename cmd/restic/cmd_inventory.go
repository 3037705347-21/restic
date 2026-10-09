package main

import (
	"context"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/inventory"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/ui/progress"
)

// ErrInventoryDrift is returned when the current backend objects differ from
// the previous inventory due to removed or replaced objects.
var ErrInventoryDrift = errors.New("repository objects were removed or replaced since the previous inventory")

func newInventoryCommand(globalOptions *global.Options) *cobra.Command {
	var opts InventoryOptions

	cmd := &cobra.Command{
		Use:   "inventory [flags]",
		Short: "Record and reconcile an inventory of backend objects",
		Long: `
The "inventory" command records the objects that physically exist in the
repository backend and compares them against the inventory recorded during
the previous run. It detects:

* objects that have been removed from the backend
* objects that have been replaced, that is objects with the same type and ID
  but a different size

Objects that were newly added in the meantime (for example by a backup running
concurrently) are counted separately and do not count as suspicious changes.
Lock files are not tracked because concurrent backups create and remove them
during normal operation.

The inventory is stored in a local file outside the repository, specified
with --file or the RESTIC_INVENTORY_FILE environment variable. The file must
be kept in a location that cannot be modified together with the repository
backend, otherwise an attacker could tamper with the inventory as well.

The inventory file is only replaced after every object type has been listed
successfully. If the enumeration fails halfway through, the previous inventory
file is left untouched.

Replacement detection is based on the object size reported by the backend;
use "check --read-data" to also verify the actual contents of the objects.

EXIT STATUS
===========

Exit status is 0 if no objects were removed or replaced.
Exit status is 1 if objects were removed or replaced, or if an error occurred.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInventory(cmd.Context(), opts, *globalOptions, globalOptions.Term)
		},
	}

	opts.AddFlags(cmd.Flags())
	return cmd
}

// InventoryOptions collects all options for the 'inventory' command.
type InventoryOptions struct {
	File string
}

func (opts *InventoryOptions) AddFlags(f *pflag.FlagSet) {
	f.StringVarP(&opts.File, "file", "f", "", "`file` in which to store the object inventory (default: $RESTIC_INVENTORY_FILE)")
}

// inventoryStatus is the JSON output of the command in --json mode.
type inventoryStatus struct {
	MessageType         string                     `json:"message_type"` // "inventory"
	RepoID              string                     `json:"repo_id"`
	Created             bool                       `json:"created"` // true when no previous inventory existed
	BaselineGeneratedAt *time.Time                 `json:"baseline_generated_at,omitempty"`
	GeneratedAt         time.Time                  `json:"generated_at"`
	ObjectCount         int                        `json:"object_count"`
	Removed             []inventory.Ref            `json:"removed"`
	Added               []inventory.Ref            `json:"added"`
	Replaced            []inventory.ReplacedObject `json:"replaced"`
}

func runInventory(ctx context.Context, opts InventoryOptions, gopts global.Options, term ui.Terminal) error {
	file := opts.File
	if file == "" {
		file = os.Getenv("RESTIC_INVENTORY_FILE")
	}
	if file == "" {
		return errors.Fatal("please specify an inventory file with --file or the RESTIC_INVENTORY_FILE environment variable")
	}

	printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, term)

	ctx, repo, unlock, err := openWithReadLock(ctx, gopts, gopts.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()

	// Enumerate the complete backend first. Only if this succeeds is the
	// previous inventory even loaded, so a partial listing can never replace
	// a valid baseline.
	current, err := inventory.Enumerate(ctx, repo, repo.Config().ID, time.Now())
	if err != nil {
		return err
	}

	previous, err := inventory.Load(file)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}

		// First run: establish a baseline and exit.
		if gopts.JSON {
			term.Print(ui.ToJSONString(newInventoryStatus(nil, current, inventory.Diff{
				Removed: []inventory.Ref{}, Added: []inventory.Ref{}, Replaced: []inventory.ReplacedObject{},
			}, true)))
		} else {
			printer.P("no previous inventory found, creating baseline in %v with %d objects", file, current.Count())
		}
		return current.Save(file)
	}

	if previous.RepoID != "" && previous.RepoID != current.RepoID {
		return errors.Fatalf("refusing to update inventory: the file %v was taken from repository %v, but the opened repository is %v",
			file, previous.RepoID, current.RepoID)
	}

	diff := inventory.Compare(previous, current)

	if gopts.JSON {
		term.Print(ui.ToJSONString(newInventoryStatus(previous, current, diff, false)))
	} else {
		printInventoryDiff(printer, previous, diff)
	}

	if err := current.Save(file); err != nil {
		return err
	}
	printer.P("updated inventory file %v", file)

	if diff.Changed() {
		return ErrInventoryDrift
	}
	return nil
}

func newInventoryStatus(previous, current *inventory.Ledger, diff inventory.Diff, created bool) inventoryStatus {
	status := inventoryStatus{
		MessageType: "inventory",
		RepoID:      current.RepoID,
		Created:     created,
		GeneratedAt: current.GeneratedAt,
		ObjectCount: current.Count(),
		Removed:     diff.Removed,
		Added:       diff.Added,
		Replaced:    diff.Replaced,
	}
	if previous != nil {
		baseline := previous.GeneratedAt
		status.BaselineGeneratedAt = &baseline
	}
	return status
}

func printInventoryDiff(printer restic.Printer, previous *inventory.Ledger, diff inventory.Diff) {
	printer.P("comparing backend objects against inventory from %v", previous.GeneratedAt.Format(global.TimeFormat))

	for _, ref := range diff.Removed {
		printer.S("removed object:  %-8s %v", ref.Type, ref.ID)
	}
	for _, replaced := range diff.Replaced {
		printer.S("replaced object: %-8s %v (size %d -> %d)", replaced.Type, replaced.ID, replaced.OldSize, replaced.NewSize)
	}
	for _, ref := range diff.Added {
		printer.V("new object:      %-8s %v", ref.Type, ref.ID)
	}

	switch {
	case diff.Changed():
		printer.S("found %d removed and %d replaced objects; %d objects were added (new objects are expected during concurrent backups)",
			len(diff.Removed), len(diff.Replaced), len(diff.Added))
	case len(diff.Added) == 0:
		printer.P("no changes found")
	default:
		printer.P("no objects removed or replaced (%d new objects)", len(diff.Added))
	}
}
