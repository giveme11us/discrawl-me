package memory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// skillFingerprint re-implements, independently from the Python harness, the
// copy-learning skill formula (step 3): sha256 of the canonical JSON
// {"content":..., "embeds":[...]} with sorted keys and compact separators.
func skillFingerprint(t *testing.T, content string, embeds []map[string]any) string {
	t.Helper()
	if embeds == nil {
		embeds = []map[string]any{}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(map[string]any{"content": content, "embeds": embeds}); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(bytes.TrimRight(buf.Bytes(), "\n"))
	return hex.EncodeToString(sum[:])
}

type harnessReport struct {
	OK        bool           `json:"ok"`
	Error     string         `json:"error"`
	Signals   int            `json:"signals_emitted"`
	RawTotals map[string]int `json:"totals"`
	StateBef  string         `json:"state_sha256_before"`
	StateAft  string         `json:"state_sha256_after"`
	Selective bool           `json:"selective_memory"`
}

// R1-08(b): copy-learning verification on the DB PRODUCED by `memory export`
// from a fixture with 2 releases / 2 channels, 7 processed IDs (two before
// since, several older than the 30-day window) and 6 drafts: baseline
// 7 unchanged / 0 signal; new / edit are reported as novelties; replay is
// identical; errors exit 2; state.json never changes.
func TestCopyLearningHarnessOnExportedDB(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	harness, err := filepath.Abs(filepath.Join("..", "..", "tools", "verify_copy_learning.py"))
	if err != nil {
		t.Fatal(err)
	}
	f, x := buildFixture(t)
	const chPin2 = "300000000000000008"
	s, err := sql.Open("sqlite", "file:"+f.source+"?mode=rw")
	if err != nil {
		t.Fatal(err)
	}
	addChannel(t, s, chPin2, gEasy, catGen, "text", false, fixtureNow.Add(-time.Hour))
	processed := map[string]string{
		x.processedOld: skillFingerprint(t, "processed old", nil),
		x.pinAfter:     skillFingerprint(t, "pin after since", nil),
	}
	add := func(ch string, at time.Time, content string, embeds []map[string]any) string {
		raw, _ := json.Marshal(map[string]any{"content": content, "embeds": embeds})
		id := addMessage(t, s, gEasy, ch, at, content, string(raw))
		processed[id] = skillFingerprint(t, content, embeds)
		return id
	}
	embed := []map[string]any{{"title": "Release", "fields": []map[string]any{{"name": "Retailer", "value": "synthetic"}}}}
	add(chPin, time.Date(2026, 9, 10, 9, 0, 0, 0, time.UTC), "pin copy one", nil)
	add(chPin, time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC), "pin copy two", embed)
	add(chPin2, time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC), "pin2 processed before since", nil)
	add(chPin2, time.Date(2026, 9, 3, 9, 0, 0, 0, time.UTC), "pin2 copy old", embed)
	editTarget := add(chPin2, time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC), "pin2 copy recent", nil)
	_ = s.Close()
	if len(processed) != 7 {
		t.Fatalf("fixture must have 7 processed, got %d", len(processed))
	}
	var drafts []string
	for i := 0; i < 6; i++ {
		p := filepath.Join(f.dir, "draft-"+itoa(i)+".md")
		if err := os.WriteFile(p, []byte("synthetic draft"), 0o600); err != nil {
			t.Fatal(err)
		}
		drafts = append(drafts, p)
	}
	writeJSON(t, f.registry, map[string]any{"releases": []map[string]any{
		{"slug": "synthetic-a", "guild_id": gEasy, "channel_id": chPin, "since": "2026-09-01T00:00:00Z", "report_dir": f.dir, "draft_files": drafts[:3]},
		{"slug": "synthetic-b", "guild_id": gEasy, "channel_id": chPin2, "since": "2026-09-01T00:00:00Z", "report_dir": f.dir, "draft_files": drafts[3:]},
	}})
	writeJSON(t, f.state, map[string]any{"processed": processed, "signal_fingerprints": []string{}})
	skill := filepath.Join(f.dir, "SKILL.md")
	if err := os.WriteFile(skill, []byte("synthetic skill copy\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	skillSHA, _ := fileSHA(skill)
	stateSHA, _ := fileSHA(f.state)

	out := filepath.Join(f.dir, "selective.db")
	rep, err := Export(context.Background(), ExportOptions{Source: f.source, Output: out, PolicyPath: f.policy, Now: fixtureNow})
	if err != nil {
		t.Fatal(err)
	}
	if rep.ProcessedKept != 7 || rep.ProcessedTotal != 7 {
		t.Fatalf("export processed coverage %d/%d", rep.ProcessedKept, rep.ProcessedTotal)
	}
	run := func(db, sha string) (harnessReport, int, string) {
		cmd := exec.Command(py, harness, "--db", db, "--registry", f.registry, "--state", f.state, "--skill", skill, "--skill-sha256", sha)
		var stdout bytes.Buffer
		cmd.Stdout = &stdout
		err := cmd.Run()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		var r harnessReport
		if err := json.Unmarshal(stdout.Bytes(), &r); err != nil {
			t.Fatalf("harness output: %v %q", err, stdout.String())
		}
		if after, _ := fileSHA(f.state); after != stateSHA {
			t.Fatal("state.json changed")
		}
		return r, code, stdout.String()
	}
	base, code, raw1 := run(out, skillSHA)
	tot := base.RawTotals
	if code != 0 || !base.OK || !base.Selective || tot["unchanged"] != 7 || tot["new"] != 0 || tot["changed"] != 0 || tot["processed_missing"] != 0 || tot["drafts_reachable"] != 6 || base.Signals != 0 {
		t.Fatalf("baseline on exported DB: code=%d %s", code, raw1)
	}
	if _, _, raw2 := run(out, skillSHA); raw2 != raw1 {
		t.Fatal("replay is not identical")
	}
	// Mutations on COPIES of the exported DB.
	mutate := func(name, q string, args ...any) string {
		p := filepath.Join(f.dir, name)
		copyFile(t, out, p)
		db, err := sql.Open("sqlite", "file:"+p+"?mode=rw")
		if err != nil {
			t.Fatal(err)
		}
		mustExec(t, db, q, args...)
		_, _ = db.Exec(`pragma wal_checkpoint(TRUNCATE)`)
		_ = db.Close()
		return p
	}
	newDB := mutate("new.db", `insert into messages(id, guild_id, channel_id, author_id, message_type, created_at, content, normalized_content, raw_json, updated_at) values('499999999999999990', ?, ?, '1', 0, '2026-10-14T00:00:00Z', 'brand new', 'brand new', '{}', '2026-10-14T00:00:00Z')`, gEasy, chPin2)
	if r, code, raw := run(newDB, skillSHA); code != 1 || r.RawTotals["new"] != 1 || r.RawTotals["unchanged"] != 7 {
		t.Fatalf("new: code=%d %s", code, raw)
	}
	editDB := mutate("edit.db", `update messages set content = 'pin2 copy recent EDITED' where id = ?`, editTarget)
	if r, code, raw := run(editDB, skillSHA); code != 1 || r.RawTotals["changed"] != 1 || r.RawTotals["unchanged"] != 6 {
		t.Fatalf("edit: code=%d %s", code, raw)
	}
	if r, code, _ := run(out, strings.Repeat("0", 64)); code != 2 || r.OK {
		t.Fatalf("wrong skill fingerprint must exit 2, got %d", code)
	}
	if _, code, _ := run(filepath.Join(f.dir, "absent.db"), skillSHA); code != 2 {
		t.Fatalf("missing DB must exit 2, got %d", code)
	}
}
