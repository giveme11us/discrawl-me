package memory

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/giveme11us/discrawl-me/internal/store"
)

const chSD = "300000000000000008"

// finalPolicy is the owner's final shape: two guilds, strict rolling 30
// days, no info, no pins.
func finalPolicy(t *testing.T, dir string) string {
	t.Helper()
	p := filepath.Join(dir, "policy-final.json")
	writeJSON(t, p, map[string]any{"version": 1, "guild_ids": []string{gSD, gDust}, "rolling_days": 30})
	return p
}

func TestPolicyInfoAndPinsOptional(t *testing.T) {
	dir := t.TempDir()
	r, err := LoadPolicy(finalPolicy(t, dir), fixtureNow)
	if err != nil {
		t.Fatalf("policy without info/pins: %v", err)
	}
	if r.InfoEnabled() || len(r.InfoCats) != 0 || len(r.Pins) != 0 || len(r.ProcessedIDs) != 0 || r.RegistrySHA != "" || r.StateSHA != "" {
		t.Fatalf("info/pins must be disabled: %+v", r)
	}
	if !r.Cutoff.Equal(fixtureNow.AddDate(0, 0, -30)) {
		t.Fatalf("cutoff %v", r.Cutoff)
	}
	nullP := filepath.Join(dir, "null.json")
	writeJSON(t, nullP, map[string]any{"version": 1, "guild_ids": []string{gSD}, "rolling_days": 30, "info": nil, "pins": nil})
	if r, err := LoadPolicy(nullP, fixtureNow); err != nil || r.InfoEnabled() {
		t.Fatalf("null info/pins = disabled: %v", err)
	}
	// Present sections keep the strict validation.
	for name, extra := range map[string]map[string]any{
		"info empty categories": {"info": map[string]any{"guild_id": gSD, "category_ids": []string{}, "post_transition_days": 90, "catalog_stale_after_hours": 72}},
		"info guild outside":    {"info": map[string]any{"guild_id": gEasy, "category_ids": []string{catInfo}, "post_transition_days": 90, "catalog_stale_after_hours": 72}},
		"pins without paths":    {"pins": map[string]any{}},
		"pins missing registry": {"pins": map[string]any{"registry_path": filepath.Join(dir, "nope.json"), "state_path": filepath.Join(dir, "nope2.json")}},
		"unknown key":           {"exceptions": true},
	} {
		pol := map[string]any{"version": 1, "guild_ids": []string{gSD, gDust}, "rolling_days": 30}
		for k, v := range extra {
			pol[k] = v
		}
		p := filepath.Join(dir, strings.ReplaceAll(name, " ", "_")+".json")
		writeJSON(t, p, pol)
		if _, err := LoadPolicy(p, fixtureNow); err == nil {
			t.Errorf("%s: want validation error", name)
		}
	}
}

type finalIDs struct{ dustRecent, sdRecentNoEmb, sd25, sdOld string }

// buildFinalSource extends the shared fixture with Sneaker Development data:
// the EASYCOP-like guild (gEasy) holds most of the old payload and is OUT of
// the final policy.
func buildFinalSource(t *testing.T) (*fixture, finalIDs) {
	t.Helper()
	f, _ := buildFixture(t)
	s, err := store.Open(context.Background(), f.source)
	if err != nil {
		t.Fatal(err)
	}
	db := s.DB()
	addChannel(t, db, chSD, gSD, "", "text", false, fixtureNow.Add(-time.Hour))
	mustExec(t, db, `insert into sync_state(scope, cursor, updated_at) values(?,?,?)`, "channel:"+chSD+":latest", "499999999999999999", ts(fixtureNow))
	var x finalIDs
	x.dustRecent = addMessage(t, db, gDust, chDust, fixtureNow.AddDate(0, 0, -2), "dust recent", "")
	x.sdRecentNoEmb = addMessage(t, db, gSD, chSD, fixtureNow.AddDate(0, 0, -5), "sd recent", "")
	mustExec(t, db, `delete from message_embeddings where message_id = ?`, x.sdRecentNoEmb)
	mustExec(t, db, `delete from embedding_jobs where message_id = ?`, x.sdRecentNoEmb)
	x.sd25 = addMessage(t, db, gSD, chSD, fixtureNow.AddDate(0, 0, -25), "sd twentyfive", "")
	mustExec(t, db, `insert into message_edits(message_id, edited_at, content, raw_json) values(?,?,?,?)`, x.sd25, ts(fixtureNow.AddDate(0, 0, -24)), "v1", "{}")
	mustExec(t, db, `insert into reaction_events(message_id, channel_id, guild_id, user_id, emoji_name, action, event_at) values(?,?,?,?,?,?,?)`, x.sd25, chSD, gSD, "500000000000000001", "x", "add", ts(fixtureNow.AddDate(0, 0, -24)))
	mustExec(t, db, `insert into mention_events(message_id, guild_id, channel_id, author_id, target_type, target_id, event_at) values(?,?,?,?,?,?,?)`, x.sd25, gSD, chSD, "500000000000000001", "user", "500000000000000002", ts(fixtureNow.AddDate(0, 0, -25)))
	x.sdOld = addMessage(t, db, gSD, chSD, fixtureNow.AddDate(0, -3, 0), "sd old", "")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return f, x
}

func countFor(t *testing.T, db *sql.DB, table, key, id string) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(`select count(*) from "`+table+`" where "`+key+`" = ?`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Final policy end to end: export keeps only the two guilds' last 30 days
// with their embeddings/jobs, opens no rollback window; prune (as run at every
// collector cycle) removes expired and out-of-policy messages together with
// embeddings, jobs, FTS, attachments and every child row.
func TestFinalPolicyExportPruneEmbeddings(t *testing.T) {
	ctx := context.Background()
	f, x := buildFinalSource(t)
	pol := finalPolicy(t, f.dir)
	out := filepath.Join(f.dir, "final.db")
	srcEasy := scalar(t, f.source, `select count(*) from messages where guild_id = ?`, gEasy)
	rep, err := Export(ctx, ExportOptions{Source: f.source, Output: out, PolicyPath: pol, Now: fixtureNow, NoRollbackWindow: true, EnqueueMissingEmbeddings: true})
	if err != nil {
		t.Fatal(err)
	}
	got := messageIDs(t, out)
	want := map[string]bool{x.dustRecent: true, x.sdRecentNoEmb: true, x.sd25: true}
	if len(got) != len(want) {
		t.Fatalf("retained %v, want %v", got, want)
	}
	for id := range want {
		if !got[id] {
			t.Errorf("missing %s", id)
		}
	}
	if n := scalar(t, out, `select count(*) from messages where guild_id = ?`, gEasy); n != 0 || rep.OutOfPolicy[gEasy] != srcEasy || rep.Reasons[ReasonOutOfScope] != srcEasy {
		t.Fatalf("out-of-policy guild kept=%d dropped=%v reasons=%v (source %d)", n, rep.OutOfPolicy, rep.Reasons, srcEasy)
	}
	if n := scalar(t, out, `select count(*) from message_embeddings`); n != 2 {
		t.Errorf("embeddings %d, want 2 (retained with a vector)", n)
	}
	if n := scalar(t, out, `select count(*) from embedding_jobs where state = 'pending'`); n != 1 || rep.EnqueuedJobs != 1 {
		t.Errorf("pending jobs %d enqueued %d, want 1 (retained without vector)", n, rep.EnqueuedJobs)
	}
	if n := scalar(t, out, `select count(*) from embedding_jobs`); n != 3 {
		t.Errorf("jobs %d, want 3", n)
	}
	if markerField(t, out, "rollback_window") != WindowClosed || rep.RollbackWindow != WindowClosed {
		t.Fatal("final export must not open a rollback window")
	}
	// Catalog and cursors of every guild (including the dropped one) intact.
	if digestOf(t, out, "channels", "") != digestOf(t, f.source, "channels", "") {
		t.Error("channel catalog changed")
	}
	if digestOf(t, out, "sync_state", "scope not like 'memory:%' and scope not like 'schema:%'") != digestOf(t, f.source, "sync_state", "scope not like 'memory:%' and scope not like 'schema:%'") {
		t.Error("cursors changed")
	}

	// A legacy/out-of-policy row that got in anyway (e.g. written before the
	// policy changed) is removed by the next retention pass.
	s, err := store.Open(ctx, out)
	if err != nil {
		t.Fatal(err)
	}
	stray := addMessage(t, s.DB(), gEasy, chGen, fixtureNow.AddDate(0, 0, -1), "stray easycop", "")
	_ = s.Close()

	later := fixtureNow.AddDate(0, 0, 10) // sd25 is now 35 days old
	pr, err := Prune(ctx, out, pol, later, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if pr.Deleted["messages"] != 2 || pr.RollbackWindow == nil || pr.RollbackWindow.HeldMessages != 0 || pr.Remaining != 0 {
		t.Fatalf("prune report %+v / %+v", pr.Deleted, pr.RollbackWindow)
	}
	if pr.Deleted["message_embeddings"] != 2 || pr.Deleted["embedding_jobs"] != 2 || pr.Deleted["message_attachments"] != 2 {
		t.Errorf("dependent rows deleted %+v", pr.Deleted)
	}
	ro, err := store.OpenReadOnly(ctx, out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	db := ro.DB()
	for _, id := range []string{x.sd25, stray} {
		for _, tk := range [][2]string{{"messages", "id"}, {"message_embeddings", "message_id"}, {"embedding_jobs", "message_id"}, {"message_fts", "message_id"},
			{"message_attachments", "message_id"}, {"message_edits", "message_id"}, {"message_events", "message_id"}, {"reaction_events", "message_id"}, {"mention_events", "message_id"}} {
			if n := countFor(t, db, tk[0], tk[1], id); n != 0 {
				t.Errorf("%s still has %d rows for expired %s", tk[0], n, id)
			}
		}
	}
	for _, id := range []string{x.dustRecent, x.sdRecentNoEmb} {
		if countFor(t, db, "messages", "id", id) != 1 || countFor(t, db, "message_fts", "message_id", id) != 1 || countFor(t, db, "embedding_jobs", "message_id", id) != 1 {
			t.Errorf("retained %s lost rows", id)
		}
	}
	var old int64
	_ = db.QueryRow(`select count(*) from messages where created_at < ?`, ts(later.AddDate(0, 0, -30))).Scan(&old)
	if old != 0 || pr.SyncStateAft != pr.SyncStateBef {
		t.Fatalf("old=%d cursors %d->%d", old, pr.SyncStateBef, pr.SyncStateAft)
	}
}

// The native writer on a final-policy DB: out-of-policy guild skipped,
// retained messages enqueued for embedding, floor = cutoff everywhere (no
// INFO/pin exceptions).
func TestFinalPolicyWriter(t *testing.T) {
	ctx := context.Background()
	f, _ := buildFinalSource(t)
	pol := finalPolicy(t, f.dir)
	out := filepath.Join(f.dir, "final.db")
	if _, err := Export(ctx, ExportOptions{Source: f.source, Output: out, PolicyPath: pol, Now: fixtureNow, NoRollbackWindow: true}); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(ctx, out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	w, err := InstallWriterRetention(ctx, s, pol, fixtureNow)
	if err != nil {
		t.Fatal(err)
	}
	mk := func(id, g, ch string, at time.Time) store.MessageMutation {
		return store.MessageMutation{Record: store.MessageRecord{ID: id, GuildID: g, ChannelID: ch, CreatedAt: ts(at), Content: "w", NormalizedContent: "w", RawJSON: "{}"}, Options: store.WriteOptions{EnqueueEmbedding: true}}
	}
	if err := s.UpsertMessages(ctx, []store.MessageMutation{
		mk("420000000000000001", gEasy, chGen, fixtureNow.Add(-time.Hour)),
		mk("420000000000000002", gSD, chSD, fixtureNow.Add(-time.Hour)),
		mk("420000000000000003", gSD, chSD, fixtureNow.AddDate(0, 0, -40)),
	}); err != nil {
		t.Fatal(err)
	}
	var n int
	_ = s.DB().QueryRow(`select count(*) from messages where id like '42%'`).Scan(&n)
	if n != 1 || s.RetentionSkipped() != 2 {
		t.Fatalf("written %d skipped %d", n, s.RetentionSkipped())
	}
	_ = s.DB().QueryRow(`select count(*) from embedding_jobs where message_id = '420000000000000002' and state = 'pending'`).Scan(&n)
	if n != 1 {
		t.Fatal("retained write not enqueued for embedding")
	}
	cut := fixtureNow.AddDate(0, 0, -30)
	for _, ch := range []string{chSD, chDust, chInfo, chPin} {
		if !w.SinceFloor(ch).Equal(cut) {
			t.Errorf("floor %s = %v, want cutoff", ch, w.SinceFloor(ch))
		}
	}
}
