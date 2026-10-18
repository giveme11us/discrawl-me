package memory

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func journalMode(t *testing.T, path string) string {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var mode string
	if err := db.QueryRow(`pragma journal_mode`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	return mode
}

// R1-01: a DB without the selective marker in journal DELETE mode must be
// refused WITHOUT any mutation (bytes, journal mode, sidecars), for both the
// writable and the dry-run path.
func TestPruneRefusalDoesNotMutateUnmarkedDB(t *testing.T) {
	f, _ := buildFixture(t)
	unmarked := filepath.Join(f.dir, "unmarked.db")
	copyFile(t, f.source, unmarked)
	db, err := sql.Open("sqlite", "file:"+unmarked+"?mode=rw")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`pragma journal_mode=DELETE`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if got := journalMode(t, unmarked); got != "delete" {
		t.Fatalf("setup journal mode %q", got)
	}
	before, err := fileSHA(unmarked)
	if err != nil {
		t.Fatal(err)
	}
	for _, dry := range []bool{false, true} {
		_, err := Prune(context.Background(), unmarked, f.policy, fixtureNow, dry, false)
		if !errors.Is(err, ErrNotSelective) {
			t.Fatalf("dry=%v: want ErrNotSelective, got %v", dry, err)
		}
		after, err := fileSHA(unmarked)
		if err != nil {
			t.Fatal(err)
		}
		if after != before {
			t.Fatalf("dry=%v: unmarked DB mutated %s -> %s", dry, before, after)
		}
		for _, sfx := range []string{"-wal", "-shm", "-journal"} {
			if _, err := os.Stat(unmarked + sfx); !os.IsNotExist(err) {
				t.Fatalf("dry=%v: sidecar %s created", dry, sfx)
			}
		}
	}
	if got := journalMode(t, unmarked); got != "delete" {
		t.Fatalf("journal mode changed to %q", got)
	}
}

// R1-05: missing/null/mistyped processed is never a revocation of pins.
func TestPinStateShapeFailClosed(t *testing.T) {
	f, _ := buildFixture(t)
	srcBefore, _ := fileSHA(f.source)
	cases := map[string]string{
		"empty object":       `{}`,
		"processed null":     `{"processed": null}`,
		"processed array":    `{"processed": []}`,
		"processed number":   `{"processed": 7}`,
		"value not string":   `{"processed": {"400000000000000008": 1}}`,
		"value empty string": `{"processed": {"400000000000000008": ""}}`,
		"top-level null":     `null`,
		"top-level array":    `[]`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			state := filepath.Join(f.dir, "state-"+strings.ReplaceAll(name, " ", "-")+".json")
			if err := os.WriteFile(state, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			pol := defaultPolicy(f)
			pol["pins"] = map[string]any{"registry_path": f.registry, "state_path": state}
			polPath := state + ".policy.json"
			writeJSON(t, polPath, pol)
			stBefore, _ := fileSHA(state)
			out := filepath.Join(f.dir, "o-"+filepath.Base(state)+".db")
			_, err := Export(context.Background(), ExportOptions{Source: f.source, Output: out, PolicyPath: polPath, Now: fixtureNow})
			if err == nil || !strings.Contains(err.Error(), "pin state") {
				t.Fatalf("want pin state error, got %v", err)
			}
			if _, err := os.Stat(out); !os.IsNotExist(err) {
				t.Fatal("target exists after refused export")
			}
			if stAfter, _ := fileSHA(state); stAfter != stBefore {
				t.Fatal("state mutated")
			}
		})
	}
	if after, _ := fileSHA(f.source); after != srcBefore {
		t.Fatal("source mutated")
	}
}

// R1-05: a registered processed ID absent from the source must fail the
// export (no partially covered target published).
func TestExportRefusesIncompleteProcessedCoverage(t *testing.T) {
	f, x := buildFixture(t)
	writeJSON(t, f.state, map[string]any{"processed": map[string]string{x.processedOld: "h1", x.pinAfter: "h2", "499999999999999999": "h3"}, "signal_fingerprints": []string{}})
	out := filepath.Join(f.dir, "missing.db")
	_, err := export(t, f, out, fixtureNow)
	if err == nil || !strings.Contains(err.Error(), "coverage incomplete (2/3") {
		t.Fatalf("want coverage error 2/3, got %v", err)
	}
	entries, _ := os.ReadDir(f.dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "missing.db") {
			t.Fatalf("leftover %s", e.Name())
		}
	}
}

// R1-02: the retention pass is bounded per call (oldest first) and reports
// the remaining backlog; a second pass finishes it. Cursors stay identical.
func TestPruneBoundedPass(t *testing.T) {
	f, _ := buildFixture(t)
	out := filepath.Join(f.dir, "sel.db")
	if _, err := export(t, f, out, fixtureNow); err != nil {
		t.Fatal(err)
	}
	later := fixtureNow.Add(60 * 24 * time.Hour)
	dry, err := Prune(context.Background(), out, f.policy, later, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if dry.Expired < 2 {
		t.Fatalf("fixture needs >=2 expired at +60d, got %d", dry.Expired)
	}
	cursors := digestOf(t, out, "sync_state", "scope not like 'memory:%'")
	rep, err := PruneWithLimit(context.Background(), out, f.policy, later, false, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Deleted["messages"] != 1 || rep.Remaining != dry.Expired-1 {
		t.Fatalf("bounded pass: deleted=%d remaining=%d expired=%d", rep.Deleted["messages"], rep.Remaining, dry.Expired)
	}
	rep2, err := PruneWithLimit(context.Background(), out, f.policy, later, false, false, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if rep2.Deleted["messages"] != dry.Expired-1 || rep2.Remaining != 0 {
		t.Fatalf("second pass: %+v", rep2)
	}
	if got := digestOf(t, out, "sync_state", "scope not like 'memory:%'"); got != cursors {
		t.Fatal("cursor rows changed by bounded prune")
	}
}
