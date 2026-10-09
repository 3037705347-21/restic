package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/ui/progress"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newKeyRotateCommand(globalOptions *global.Options) *cobra.Command {
	var opts KeyRotateOptions

	cmd := &cobra.Command{
		Use:   "rotate --roster-file file",
		Short: "Reconcile repository keys with a roster of current member keys",
		Long: `
The "key rotate" command reconciles the keys (passwords) of a repository with a
roster of current member key IDs.

Keys listed in the roster are retained. Keys recorded by a previous rotation
that are no longer listed are removed. Keys added manually, which never
appeared in a roster, are never removed. The first rotation (when no previous
rotation state exists) records the roster as a baseline and removes no keys.

The roster file contains one key ID (or unique ID prefix) per line. Empty
lines and lines starting with "#" are ignored; comments may also follow an
entry after whitespace. The key used to access the repository must be part of
the roster.

The rotation is applied atomically: an invalid roster or any write failure
rolls back all changes, so the repository is never left without a known
working key. Re-running the command with the same roster does not modify the
repository. Use the --dry-run flag to preview changes.

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
			return runKeyRotate(cmd.Context(), *globalOptions, opts, args, globalOptions.Term)
		},
	}

	opts.AddFlags(cmd.Flags())
	return cmd
}

// KeyRotateOptions collects the options for the key rotate command.
type KeyRotateOptions struct {
	RosterFile string
	DryRun     bool
}

// AddFlags registers the options for the key rotate command.
func (opts *KeyRotateOptions) AddFlags(flags *pflag.FlagSet) {
	flags.StringVar(&opts.RosterFile, "roster-file", "", "`file` containing the current member key roster, one ID per line (\"-\" for stdin)")
	flags.BoolVarP(&opts.DryRun, "dry-run", "n", false, "show what would be done without modifying the repository")
}

// parseKeyRoster reads a roster file: one hexadecimal key ID (or ID prefix)
// per line. Blank lines and comments are ignored.
func parseKeyRoster(r io.Reader) ([]string, error) {
	var refs []string
	seen := make(map[string]int)

	scanner := bufio.NewScanner(r)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if i := strings.IndexByte(line, '#'); i >= 0 && (i == 0 || line[i-1] == ' ' || line[i-1] == '\t') {
			line = strings.TrimSpace(line[:i])
		}

		fields := strings.Fields(line)
		if len(fields) != 1 {
			return nil, fmt.Errorf("roster line %d: expected one key ID per line, got %q", lineNo, line)
		}

		ref := fields[0]
		if _, err := hex.DecodeString(ref); err != nil {
			return nil, fmt.Errorf("roster line %d: %q is not a hexadecimal key ID", lineNo, ref)
		}

		if prev, ok := seen[ref]; ok {
			return nil, fmt.Errorf("roster lists key %q more than once (lines %d and %d)", ref, prev, lineNo)
		}
		seen[ref] = lineNo

		refs = append(refs, ref)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return refs, nil
}

func readKeyRoster(path string) ([]string, error) {
	if path == "-" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("reading roster from stdin: %w", err)
		}
		return parseKeyRoster(strings.NewReader(string(data)))
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening roster file: %w", err)
	}
	defer f.Close()

	return parseKeyRoster(f)
}

// keyRotateReport is the JSON output of the key rotate command.
type keyRotateReport struct {
	DryRun    bool     `json:"dry_run"`
	Changed   bool     `json:"changed"`
	Baseline  bool     `json:"baseline"`
	Current   string   `json:"current"`
	Roster    []string `json:"roster"`
	Adopt     []string `json:"adopt"`
	Retire    []string `json:"retire"`
	Removed   []string `json:"removed,omitempty"`
	Untouched []string `json:"untouched"`
}

func runKeyRotate(ctx context.Context, gopts global.Options, opts KeyRotateOptions, args []string, term ui.Terminal) error {
	if len(args) > 0 {
		return errors.Fatalf("the key rotate command expects no arguments, only options - please see `restic help key rotate` for usage and flags")
	}
	if opts.RosterFile == "" {
		return errors.Fatalf("the key rotate command requires a --roster-file")
	}

	refs, err := readKeyRoster(opts.RosterFile)
	if err != nil {
		return errors.Fatalf("%v", err)
	}

	printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, term)
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, gopts, opts.DryRun, printer)
	if err != nil {
		return err
	}
	defer unlock()

	plan, err := repository.PlanKeyRoster(ctx, repo, refs)
	if err != nil {
		return errors.Fatalf("%v", err)
	}

	report := keyRotateReport{
		DryRun:    opts.DryRun,
		Baseline:  plan.Baseline,
		Current:   plan.Current.String(),
		Roster:    idsToStrings(plan.Roster.List()),
		Adopt:     idsToStrings(plan.Adopt.List()),
		Retire:    idsToStrings(plan.Retire.List()),
		Untouched: idsToStrings(plan.Untouched.List()),
	}

	if !gopts.JSON {
		printKeyRosterPlan(printer, plan, opts.DryRun)
	}

	if opts.DryRun || plan.NoOp {
		if gopts.JSON {
			return json.NewEncoder(term.OutputWriter()).Encode(report)
		}
		return nil
	}

	removed, err := repo.ApplyKeyRoster(ctx, plan)
	if err != nil {
		return errors.Fatalf("%v", err)
	}

	report.Removed = idsToStrings(removed)
	report.Changed = plan.Baseline || len(plan.Adopt) > 0 || len(removed) > 0

	if !gopts.JSON {
		for _, id := range removed {
			printer.P("removed key %v", id.Str())
		}
		printer.S("key rotation complete: %d key(s) retained, %d key(s) removed; the repository remains unlockable with the current key %v",
			len(plan.Roster), len(removed), plan.Current.Str())
	}

	if gopts.JSON {
		return json.NewEncoder(term.OutputWriter()).Encode(report)
	}
	return nil
}

func printKeyRosterPlan(printer restic.Printer, plan *repository.KeyRosterPlan, dryRun bool) {
	verb := "would "
	if !dryRun {
		verb = ""
	}

	if plan.NoOp {
		printer.P("key roster is already up to date, no changes needed")
		return
	}

	if plan.Baseline {
		printer.P("no previous roster state found; %srecord the roster with %d key(s) as the baseline; no keys are removed",
			verb, len(plan.Roster))
	}

	for _, id := range plan.Adopt.List() {
		printer.P("%sadopt key %v into roster management", verb, id.Str())
	}
	for _, id := range plan.Retire.List() {
		printer.P("%sremove key %v (managed by a previous rotation but no longer in the roster)", verb, id.Str())
	}
	for _, id := range plan.Untouched.List() {
		printer.P("%sleave key %v untouched (not managed by roster rotation)", verb, id.Str())
	}
}

func idsToStrings(ids restic.IDs) []string {
	strs := make([]string, 0, len(ids))
	for _, id := range ids {
		strs = append(strs, id.String())
	}
	return strs
}
