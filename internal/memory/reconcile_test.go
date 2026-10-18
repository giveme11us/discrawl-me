package memory

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func q1(t *testing.T, path, q string, args ...any) string {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var v sql.NullString
	if err := db.QueryRow(q, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return v.String
}

func execOn(t *testing.T, path, q string, args ...any) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=rw")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mustExec(t, db, q, args...)
}

// R1-04: rollback reconciliation by ID. Reproduces the Verify scenario (two
// PRE-EXISTING IDs edited / soft-deleted after cutover, which a
// created_at > cutover filter misses) plus a new message, child rows, catalog
// change and cursor advance; checks idempotency, newer-archive-wins, no
// propagation of prune, refusal cases with the archive unchanged.
func TestReconcileByIDCoversEditsDeletesChildrenCatalogCursors(t *testing.T) {
	f, x := buildFixture(t)
	ctx := context.Background()
	sel := filepath.Join(f.dir, "sel.db")
	if _, err := export(t, f, sel, fixtureNow); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(f.dir, "parked-archive.db")
	copyFile(t, f.source, archive)
	cut := fixtureNow.Add(24 * time.Hour)
	post := ts(cut.Add(time.Hour))

	// Post-cutover activity in the selective memory.
	execOn(t, sel, `update messages set content='info intro EDITED', normalized_content='info intro edited', edited_at=?, updated_at=? where id=?`, post, post, x.oldInfo)
	execOn(t, sel, `insert into message_edits(message_id, edited_at, content, raw_json) values(?,?,?,?)`, x.oldInfo, post, "info intro old", "{}")
	execOn(t, sel, `update messages set deleted_at=?, updated_at=? where id=?`, post, post, x.recentGen)
	execOn(t, sel, `insert into message_events(guild_id, channel_id, message_id, event_type, event_at, payload_json) values(?,?,?,'delete',?,'{}')`, gEasy, chGen, x.recentGen, post)
	execOn(t, sel, `insert into messages(id, guild_id, channel_id, author_id, message_type, created_at, content, normalized_content, raw_json, updated_at) values('499999999999999901',?,?,'1',0,?,'brand new','brand new','{}',?)`, gEasy, chGen, post, post)
	execOn(t, sel, `insert into message_attachments(attachment_id, message_id, guild_id, channel_id, filename, updated_at) values('a499999999999999901','499999999999999901',?,?,'n.png',?)`, gEasy, chGen, post)
	execOn(t, sel, `update channels set name='renamed', updated_at=? where id=?`, post, chGen)
	execOn(t, sel, `insert into sync_state(scope, cursor, updated_at) values(?, '499999999999999901', ?) on conflict(scope) do update set cursor=excluded.cursor, updated_at=excluded.updated_at`, "channel:"+chGen+":latest_message_id", post)
	// A message expiring right after cutover is pruned from the selective
	// memory; reconcile must NOT delete it from the archive.
	old := ts(fixtureNow.Add(-29*24*time.Hour - 12*time.Hour))
	for _, p := range []string{sel, archive} {
		execOn(t, p, `insert into messages(id, guild_id, channel_id, author_id, message_type, created_at, content, normalized_content, raw_json, updated_at) values('499999999999999950',?,?,'1',0,?,'expiring','expiring','{}',?)`, gEasy, chGen, old, old)
	}
	pr, err := Prune(ctx, sel, f.policy, cut.Add(2*time.Hour), false, false)
	if err != nil {
		t.Fatal(err)
	}
	if pr.Deleted["messages"] < 1 || q1(t, sel, `select count(*) from messages where id='499999999999999950'`) != "0" {
		t.Fatalf("setup: expiring message not pruned: %+v", pr.Deleted)
	}
	// The created_at>cutover filter of the R1 runbook selects only the new row.
	if got := q1(t, sel, `select count(*) from messages where created_at > ?`, ts(cut)); got != "1" {
		t.Fatalf("old filter baseline: %s", got)
	}

	// Refusals leave the archive byte-identical.
	before, _ := fileSHA(archive)
	for name, o := range map[string]ReconcileOptions{
		"selective as archive": {Selective: sel, Archive: sel, Since: cut},
		"unmarked selective":   {Selective: f.source, Archive: archive, Since: cut},
		"marked archive":       {Selective: sel, Archive: filepath.Join(f.dir, "sel2.db"), Since: cut},
		"missing archive":      {Selective: sel, Archive: filepath.Join(f.dir, "nope.db"), Since: cut},
	} {
		if name == "marked archive" {
			copyFile(t, sel, o.Archive)
		}
		if _, err := Reconcile(ctx, o); err == nil {
			t.Fatalf("%s: expected refusal", name)
		}
	}
	dry, err := Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: archive, Since: cut, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if after, _ := fileSHA(archive); after != before {
		t.Fatal("refusal or dry-run modified the archive")
	}
	if dry.TouchedMsgs != 3 {
		t.Fatalf("dry-run touched %d, want 3", dry.TouchedMsgs)
	}

	rep, err := Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: archive, Since: cut})
	if err != nil {
		t.Fatal(err)
	}
	if rep.TouchedMsgs != 3 || rep.Applied["messages"] != 3 {
		t.Fatalf("reconcile: %+v", rep)
	}
	checks := map[string][2]string{
		"edit applied":       {q1(t, archive, `select content from messages where id=?`, x.oldInfo), "info intro EDITED"},
		"edit history":       {q1(t, archive, `select count(*) from message_edits where message_id=?`, x.oldInfo), "1"},
		"tombstone applied":  {q1(t, archive, `select deleted_at from messages where id=?`, x.recentGen), post},
		"delete event":       {q1(t, archive, `select count(*) from message_events where message_id=? and event_type='delete'`, x.recentGen), "1"},
		"new message":        {q1(t, archive, `select count(*) from messages where id='499999999999999901'`), "1"},
		"new attachment":     {q1(t, archive, `select count(*) from message_attachments where message_id='499999999999999901'`), "1"},
		"catalog":            {q1(t, archive, `select name from channels where id=?`, chGen), "renamed"},
		"cursor":             {q1(t, archive, `select cursor from sync_state where scope=?`, "channel:"+chGen+":latest_message_id"), "499999999999999901"},
		"fts edit":           {q1(t, archive, `select count(*) from message_fts where message_fts match 'edited' and message_id=?`, x.oldInfo), "1"},
		"prune not mirrored": {q1(t, archive, `select count(*) from messages where id = '499999999999999950'`), "1"},
		"no memory scopes":   {q1(t, archive, `select count(*) from sync_state where scope like 'memory:%'`), "0"},
	}
	for name, c := range checks {
		if c[0] != c[1] {
			t.Errorf("%s: got %q want %q", name, c[0], c[1])
		}
	}
	// Idempotent second run.
	rep2, err := Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: archive, Since: cut})
	if err != nil {
		t.Fatal(err)
	}
	for table, n := range rep2.Applied {
		if n != 0 {
			t.Errorf("second run applied %d rows in %s", n, table)
		}
	}
	if q1(t, archive, `select count(*) from message_events where message_id=? and event_type='delete'`, x.recentGen) != "1" {
		t.Error("events duplicated")
	}
	// Archive newer than selective is kept.
	newer := ts(cut.Add(48 * time.Hour))
	execOn(t, archive, `update messages set content='archive newer', updated_at=? where id=?`, newer, x.oldInfo)
	if _, err := Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: archive, Since: cut}); err != nil {
		t.Fatal(err)
	}
	if got := q1(t, archive, `select content from messages where id=?`, x.oldInfo); got != "archive newer" {
		t.Fatalf("newer archive row overwritten: %q", got)
	}
	if !strings.Contains(rep.Since, "T") {
		t.Fatal("since not reported")
	}
}
