package memory

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/giveme11us/discrawl-me/internal/store"
)

// Synthetic IDs only. No real guild/channel/message data.
const (
	gEasy   = "100000000000000001"
	gDust   = "100000000000000002"
	gSD     = "100000000000000003"
	gOther  = "100000000000000009"
	catInfo = "200000000000000001"
	catArch = "200000000000000002"
	catGen  = "200000000000000003"
	chInfo  = "300000000000000001" // text room under INFO
	thInfo  = "300000000000000002" // public thread under chInfo
	thPriv  = "300000000000000003" // private thread under chInfo
	chGen   = "300000000000000004" // generic channel
	chPin   = "300000000000000005" // copy-learning registered channel
	chDust  = "300000000000000006"
	chMoved = "300000000000000007" // INFO room later moved to ARCHIVE
)

var fixtureNow = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

type fixture struct {
	dir, source, policy, registry, state string
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func addChannel(t *testing.T, db *sql.DB, id, guild, parent, kind string, private bool, observed time.Time) {
	t.Helper()
	priv := 0
	if private {
		priv = 1
	}
	mustExec(t, db, `insert or replace into channels(id, guild_id, parent_id, kind, name, position, is_private_thread, raw_json, updated_at) values(?,?,?,?,?,0,?,?,?)`,
		id, guild, parent, kind, "c-"+id[len(id)-3:], priv, `{"id":"`+id+`"}`, ts(observed))
}

var msgSeq = 400000000000000000

func addMessage(t *testing.T, db *sql.DB, guild, channel string, created time.Time, content, raw string) string {
	t.Helper()
	msgSeq++
	id := itoa(msgSeq)
	if raw == "" {
		raw = `{"id":"` + id + `","content":` + jsonString(content) + `}`
	}
	mustExec(t, db, `insert into messages(id, guild_id, channel_id, author_id, message_type, created_at, content, normalized_content, raw_json, updated_at) values(?,?,?,?,0,?,?,?,?,?)`,
		id, guild, channel, "500000000000000001", ts(created), content, content, raw, ts(created))
	rowID, _ := store.MessageFTSRowID(id)
	mustExec(t, db, `insert into message_fts(rowid, message_id, guild_id, channel_id, author_id, author_name, channel_name, content) values(?,?,?,?,?,?,?,?)`, rowID, id, guild, channel, "500000000000000001", "a", "c", content)
	mustExec(t, db, `insert into embedding_jobs(message_id, state, attempts, updated_at) values(?, 'done', 0, ?)`, id, ts(created))
	mustExec(t, db, `insert into message_embeddings(message_id, model, dim, vec, created_at) values(?, 'm', 1, x'00', ?)`, id, ts(created))
	mustExec(t, db, `insert into message_attachments(attachment_id, message_id, guild_id, channel_id, filename, updated_at) values(?,?,?,?,?,?)`, "a"+id, id, guild, channel, "f.png", ts(created))
	mustExec(t, db, `insert into message_events(guild_id, channel_id, message_id, event_type, event_at, payload_json) values(?,?,?,'upsert',?,'{}')`, guild, channel, id, ts(created))
	return id
}

func itoa(n int) string { return strings.TrimSpace(strings.Replace(jsonString(n), `"`, "", -1)) }

func jsonString(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

type ids struct {
	ancientGen, recentGen, oldInfo, oldInfoThread, oldPrivThread, pinBefore, pinAfter, processedOld, embedOnly, snapshot, oldDust, movedOld, edited string
}

func buildFixture(t *testing.T) (*fixture, ids) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	f := &fixture{dir: dir, source: filepath.Join(dir, "source.db"), policy: filepath.Join(dir, "policy.json"), registry: filepath.Join(dir, "releases.json"), state: filepath.Join(dir, "state.json")}
	s, err := store.Open(ctx, f.source)
	if err != nil {
		t.Fatal(err)
	}
	db := s.DB()
	for _, g := range []string{gEasy, gDust, gSD} {
		mustExec(t, db, `insert into guilds(id, name, raw_json, updated_at) values(?,?,?,?)`, g, "g"+g[len(g)-1:], `{"roles":[{"id":"1","name":"r"}]}`, ts(fixtureNow))
	}
	fresh := fixtureNow.Add(-time.Hour)
	addChannel(t, db, catInfo, gEasy, "", "category", false, fresh)
	addChannel(t, db, catArch, gEasy, "", "category", false, fresh)
	addChannel(t, db, catGen, gEasy, "", "category", false, fresh)
	addChannel(t, db, chInfo, gEasy, catInfo, "text", false, fresh)
	addChannel(t, db, thInfo, gEasy, chInfo, "thread_public", false, fresh)
	addChannel(t, db, thPriv, gEasy, chInfo, "thread_private", true, fresh)
	addChannel(t, db, chGen, gEasy, catGen, "text", false, fresh)
	addChannel(t, db, chPin, gEasy, catGen, "text", false, fresh)
	addChannel(t, db, chDust, gDust, "", "text", false, fresh)
	addChannel(t, db, chMoved, gEasy, catInfo, "text", false, fresh)
	var x ids
	old := fixtureNow.AddDate(-3, 0, 0)
	x.ancientGen = addMessage(t, db, gEasy, chGen, old, "ancient general", "")
	x.recentGen = addMessage(t, db, gEasy, chGen, fixtureNow.AddDate(0, 0, -2), "recent general", "")
	x.oldInfo = addMessage(t, db, gEasy, chInfo, fixtureNow.AddDate(0, -8, 0), "info intro old", "")
	x.oldInfoThread = addMessage(t, db, gEasy, thInfo, fixtureNow.AddDate(0, -7, 0), "info thread old", "")
	x.oldPrivThread = addMessage(t, db, gEasy, thPriv, fixtureNow.AddDate(0, -7, 0), "private thread old", "")
	x.pinBefore = addMessage(t, db, gEasy, chPin, time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), "pin before since", "")
	x.pinAfter = addMessage(t, db, gEasy, chPin, time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC), "pin after since", "")
	x.processedOld = addMessage(t, db, gEasy, chPin, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), "processed old", "")
	x.embedOnly = addMessage(t, db, gEasy, chGen, fixtureNow.AddDate(0, 0, -1), "", `{"id":"x","content":"","embeds":[{"title":"T","fields":[{"name":"Retailer","value":"zebrafieldvalue"}],"footer":{"text":"footnote"}}]}`)
	x.snapshot = addMessage(t, db, gEasy, chGen, fixtureNow.AddDate(0, 0, -1), "", `{"id":"y","content":"","message_snapshots":[{"message":{"content":"forwardedsnapshotword","embeds":[{"title":"snaptitle"}]}}]}`)
	x.oldDust = addMessage(t, db, gDust, chDust, fixtureNow.AddDate(-1, 0, 0), "dust old", "")
	x.movedOld = addMessage(t, db, gEasy, chMoved, fixtureNow.AddDate(0, -5, 0), "moved room intro", "")
	x.edited = x.recentGen
	mustExec(t, db, `insert into message_edits(message_id, edited_at, content, raw_json) values(?,?,?,?)`, x.recentGen, ts(fixtureNow.AddDate(0, 0, -1)), "recent general v1", "{}")
	mustExec(t, db, `insert into message_edits(message_id, edited_at, content, raw_json) values(?,?,?,?)`, x.ancientGen, ts(old), "ancient v1", "{}")
	// Cursors/checkpoints for every channel, including fully expired ones.
	for _, ch := range []string{chGen, chInfo, thInfo, chPin, chDust, chMoved} {
		mustExec(t, db, `insert into sync_state(scope, cursor, updated_at) values(?,?,?)`, "channel:"+ch+":latest", "499999999999999999", ts(fresh))
		mustExec(t, db, `insert into sync_state(scope, cursor, updated_at) values(?,?,?)`, "channel:"+ch+":history_complete", "1", ts(fresh))
	}
	mustExec(t, db, `insert into sync_state(scope, cursor, updated_at) values('channel:`+chGen+`:backfill','400000000000000001',?)`, ts(fresh))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, f.registry, map[string]any{"releases": []map[string]any{{"slug": "synthetic-release", "guild_id": gEasy, "channel_id": chPin, "since": "2026-09-01T00:00:00Z", "report_dir": "/x", "draft_files": []string{"a"}}}})
	writeJSON(t, f.state, map[string]any{"processed": map[string]string{x.processedOld: "h1", x.pinAfter: "h2"}, "signal_fingerprints": []string{}})
	writeJSON(t, f.policy, defaultPolicy(f))
	return f, x
}

func defaultPolicy(f *fixture) map[string]any {
	return map[string]any{
		"version": 1, "guild_ids": []string{gEasy, gDust, gSD}, "rolling_days": 30,
		"info": map[string]any{"guild_id": gEasy, "category_ids": []string{catInfo}, "post_transition_days": 90, "catalog_stale_after_hours": 72},
		"pins": map[string]any{"registry_path": f.registry, "state_path": f.state},
	}
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func messageIDs(t *testing.T, path string) map[string]bool {
	t.Helper()
	s, err := store.OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	rows, err := s.DB().Query(`select id from messages`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]bool{}
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		out[id] = true
	}
	return out
}

func scalar(t *testing.T, path, q string, args ...any) int64 {
	t.Helper()
	s, err := store.OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	var n int64
	if err := s.DB().QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func digestOf(t *testing.T, path, table, where string) string {
	t.Helper()
	s, err := store.OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if where == "" {
		d, err := TableDigest(context.Background(), s.DB(), table)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	// filtered digest through a temp view-less query
	rows, err := s.DB().Query(`select scope, coalesce(cursor,''), updated_at from sync_state where ` + where + ` order by scope`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var b strings.Builder
	for rows.Next() {
		var a, c, u string
		_ = rows.Scan(&a, &c, &u)
		b.WriteString(a + "|" + c + "|" + u + "\n")
	}
	return b.String()
}
