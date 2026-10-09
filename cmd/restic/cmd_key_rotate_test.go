package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

func TestParseKeyRoster(t *testing.T) {
	for _, tc := range []struct {
		name string
		text string
		want []string
		err  string
	}{
		{
			name: "simple",
			text: "# header comment\n\n0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n",
			want: []string{"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		},
		{
			name: "prefixes-comments-and-whitespace",
			text: "  ab12 # alice\n\tcd34\t# bob\n# full line comment\n",
			want: []string{"ab12", "cd34"},
		},
		{
			name: "empty",
			text: "# only comments\n\n",
			want: nil,
		},
		{
			name: "two tokens",
			text: "abcd efgh\n",
			err:  "one key ID per line",
		},
		{
			name: "non-hex",
			text: "xyz\n",
			err:  "not a hexadecimal key ID",
		},
		{
			name: "duplicate",
			text: "abcd\nabcd\n",
			err:  "more than once",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseKeyRoster(strings.NewReader(tc.text))
			if tc.err != "" {
				rtest.Assert(t, err != nil && strings.Contains(err.Error(), tc.err),
					"expected error containing %q, got %v", tc.err, err)
				return
			}
			rtest.OK(t, err)
			rtest.Equals(t, tc.want, got)
		})
	}
}

func writeTestRoster(t *testing.T, ids []string) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), "roster.txt")
	rtest.OK(t, os.WriteFile(f, []byte(strings.Join(ids, "\n")+"\n"), 0o600))
	return f
}

func testRunKeyIDs(t testing.TB, gopts global.Options) (ids []string, current string) {
	err := withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		repo, err := global.OpenRepository(ctx, gopts, restic.NewNoopPrinter())
		if err != nil {
			return err
		}
		current = repo.KeyID().String()
		return repo.List(ctx, restic.KeyFile, func(id restic.ID, _ int64) error {
			ids = append(ids, id.String())
			return nil
		})
	})
	rtest.OK(t, err)
	sort.Strings(ids)
	return ids, current
}

func testKeyIDForPassword(t testing.TB, gopts global.Options, password string) string {
	var id string
	err := withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		gopts.Password = password
		repo, err := global.OpenRepository(ctx, gopts, restic.NewNoopPrinter())
		if err != nil {
			return err
		}
		id = repo.KeyID().String()
		return nil
	})
	rtest.OK(t, err)
	return id
}

func testPasswordOpensRepo(t testing.TB, gopts global.Options, password string) bool {
	err := withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		gopts.Password = password
		_, err := global.OpenRepository(ctx, gopts, restic.NewNoopPrinter())
		return err
	})
	return err == nil
}

func testRunKeyRotate(t testing.TB, gopts global.Options, roster string) error {
	return withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runKeyRotate(ctx, gopts, KeyRotateOptions{RosterFile: roster}, nil, gopts.Term)
	})
}

func runKeyRotateCapture(t testing.TB, gopts global.Options, opts KeyRotateOptions) (string, error) {
	// make printer.P output visible to the test
	gopts.Quiet = false
	gopts.Verbosity = 1
	buf, err := withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runKeyRotate(ctx, gopts, opts, nil, gopts.Term)
	})
	return buf.String(), err
}

func runKeyRotateCaptureJSON(t testing.TB, gopts global.Options, opts KeyRotateOptions) (string, error) {
	gopts.JSON = true
	buf, err := withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runKeyRotate(ctx, gopts, opts, nil, gopts.Term)
	})
	return strings.TrimSpace(buf.String()), err
}

func TestKeyRotateRosterFlow(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	// key files are listed more than once during rotation
	env.gopts.BackendTestHook = nil
	defer cleanup()

	testRunInit(t, env.gopts)
	testRunKeyAddNewKey(t, "member-password", env.gopts)
	testRunKeyAddNewKey(t, "leaving-password", env.gopts)

	_, current := testRunKeyIDs(t, env.gopts)
	memberID := testKeyIDForPassword(t, env.gopts, "member-password")
	leavingID := testKeyIDForPassword(t, env.gopts, "leaving-password")
	rtest.Equals(t, 3, len(testKeyIDsList(t, env.gopts)))

	// 1. baseline adopts the full roster and removes nothing
	all := []string{current, memberID, leavingID}
	rtest.OK(t, testRunKeyRotate(t, env.gopts, writeTestRoster(t, all)))
	rtest.Equals(t, 3, len(testKeyIDsList(t, env.gopts)))

	// 2. dry run previews the departure without changing anything
	dryRunOut, err := runKeyRotateCapture(t, env.gopts, KeyRotateOptions{
		RosterFile: writeTestRoster(t, []string{current, memberID}),
		DryRun:     true,
	})
	rtest.OK(t, err)
	rtest.Assert(t, strings.Contains(dryRunOut, "would remove"), "dry run output: %v", dryRunOut)
	rtest.Assert(t, strings.Contains(dryRunOut, leavingID[:4]), "dry run must name the leaving key: %v", dryRunOut)
	rtest.Equals(t, 3, len(testKeyIDsList(t, env.gopts)))

	// 3. actual rotation removes only the managed key dropped from the roster
	rtest.OK(t, testRunKeyRotate(t, env.gopts, writeTestRoster(t, []string{current, memberID})))
	remaining, _ := testRunKeyIDs(t, env.gopts)
	rtest.Equals(t, 2, len(remaining))
	rtest.Assert(t, !containsString(remaining, leavingID), "leaving key must be removed")
	rtest.Assert(t, testPasswordOpensRepo(t, env.gopts, rtest.TestPassword), "current password must still open the repo")
	rtest.Assert(t, testPasswordOpensRepo(t, env.gopts, "member-password"), "retained password must still open the repo")
	rtest.Assert(t, !testPasswordOpensRepo(t, env.gopts, "leaving-password"), "retired password must not open the repo anymore")

	// 4. re-running with the same roster is a no-op
	out, err := runKeyRotateCapture(t, env.gopts, KeyRotateOptions{
		RosterFile: writeTestRoster(t, []string{current, memberID}),
	})
	rtest.OK(t, err)
	rtest.Assert(t, strings.Contains(out, "already up to date"), "expected no-op, got: %v", out)
	rtest.Equals(t, 2, len(testKeyIDsList(t, env.gopts)))

	// 5. a key added manually afterwards is not removed even though it is not listed
	testRunKeyAddNewKey(t, "manual-password", env.gopts)
	manualID := testKeyIDForPassword(t, env.gopts, "manual-password")
	rtest.OK(t, testRunKeyRotate(t, env.gopts, writeTestRoster(t, []string{current, memberID})))
	rtest.Equals(t, 3, len(testKeyIDsList(t, env.gopts)))
	rtest.Assert(t, testPasswordOpensRepo(t, env.gopts, "manual-password"), "manual password must keep working")

	// 6. JSON output reports a removing rotation; the manual key stays
	jsonOut, err := runKeyRotateCaptureJSON(t, env.gopts, KeyRotateOptions{
		RosterFile: writeTestRoster(t, []string{current}),
	})
	rtest.OK(t, err)
	var report keyRotateReport
	rtest.OK(t, json.Unmarshal([]byte(jsonOut), &report))
	rtest.Assert(t, report.Changed, "expected changes, got: %+v", report)
	rtest.Equals(t, []string{memberID}, report.Removed)
	rtest.Assert(t, containsString(report.Untouched, manualID), "manual key must be reported untouched: %+v", report)
	rtest.Equals(t, 2, len(testKeyIDsList(t, env.gopts)))
	rtest.Assert(t, testPasswordOpensRepo(t, env.gopts, "manual-password"), "manual password must keep working")
	rtest.Assert(t, !testPasswordOpensRepo(t, env.gopts, "member-password"), "removed member password must not open the repo")

	// 7. invalid rosters fail without modifying anything
	rtest.Assert(t, testRunKeyRotate(t, env.gopts, writeTestRoster(t, []string{
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	})) != nil, "unknown roster key must fail")

	rtest.Assert(t, testRunKeyRotate(t, env.gopts, writeTestRoster(t, []string{manualID})) != nil,
		"roster without the current key must fail")
	rtest.Equals(t, 2, len(testKeyIDsList(t, env.gopts)))
}

func testKeyIDsList(t testing.TB, gopts global.Options) []string {
	ids, _ := testRunKeyIDs(t, gopts)
	return ids
}

func containsString(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
