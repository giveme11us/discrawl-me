package memory

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/giveme11us/discrawl-me/internal/store"
)

const fixtureBoundary = "2026-10-15T12:00:00Z"

func markerField(t *testing.T, path, field string) string {
	t.Helper()
	return q1(t, path, `select coalesce(json_extract(cursor,'$.`+field+`'),'') from sync_state where scope='memory:selective'`)
}

// Decision A, R2-01 lossless: the SAME pre-existing IDs edited/deleted after
// the boundary, plus a message created after it, all aged past retention
// while the archive is parked. Retention holds them (window open), expels
// the unchanged ones, and reconcile transfers everything without loss; replay
// is idempotent. Then the explicit close makes retention expel them and
// reconcile refuses over the loss.
func TestRollbackWindowHoldsPostBoundaryChangesLossless(t *testing.T) {
	f, x := buildFixture(t)
	ctx := context.Background()
	sel := filepath.Join(f.dir, "sel.db")
	if _, err := export(t, f, sel, fixtureNow); err != nil {
		t.Fatal(err)
	}
	if got := markerField(t, sel, "rollback_window"); got != WindowOpen {
		t.Fatalf("export must open the rollback window, got %q", got)
	}
	archive := filepath.Join(f.dir, "parked-archive.db")
	copyFile(t, f.source, archive)
	post := ts(fixtureNow.Add(24*time.Hour + 500*time.Millisecond)) // fractional on purpose
	newID := "499999999999999901"

	// Post-boundary activity on the SAME pre-existing IDs.
	execOn(t, sel, `update messages set content='edited after cutover', normalized_content='edited after cutover', edited_at=?, updated_at=? where id=?`, post, post, x.recentGen)
	execOn(t, sel, `insert into message_edits(message_id, edited_at, content, raw_json) values(?,?,?,?)`, x.recentGen, post, "recent general", "{}")
	execOn(t, sel, `insert into reaction_events(guild_id, channel_id, message_id, user_id, emoji_name, action, event_at) values(?,?,?,?,?,?,?)`, gEasy, chGen, x.recentGen, "u1", "e", "add", post)
	execOn(t, sel, `update messages set deleted_at=?, updated_at=? where id=?`, post, post, x.embedOnly)
	execOn(t, sel, `insert into message_events(guild_id, channel_id, message_id, event_type, event_at, payload_json) values(?,?,?,'delete',?,'{}')`, gEasy, chGen, x.embedOnly, post)
	// A message born after the boundary, with children.
	execOn(t, sel, `insert into messages(id, guild_id, channel_id, author_id, message_type, created_at, content, normalized_content, raw_json, updated_at) values(?,?,?,'1',0,?,'born after cutover','born after cutover','{}',?)`, newID, gEasy, chGen, post, post)
	execOn(t, sel, `insert into message_attachments(attachment_id, message_id, guild_id, channel_id, filename, updated_at) values(?,?,?,?,'n.png',?)`, "a"+newID, newID, gEasy, chGen, post)
	execOn(t, sel, `insert into message_events(guild_id, channel_id, message_id, event_type, event_at, payload_json) values(?,?,?,'upsert',?,'{}')`, gEasy, chGen, newID, post)
	execOn(t, sel, `insert into sync_state(scope, cursor, updated_at) values(?,?,?) on conflict(scope) do update set cursor=excluded.cursor, updated_at=excluded.updated_at`, "channel:"+chGen+":latest", newID, post)

	// 40 days later everything above is out of the rolling window.
	later := fixtureNow.AddDate(0, 0, 40)
	held := []string{x.recentGen, x.embedOnly, newID}
	pr, err := Prune(ctx, sel, f.policy, later, false, false)
	if err != nil {
		t.Fatal(err)
	}
	w := pr.RollbackWindow
	if w == nil || w.State != WindowOpen || w.HeldMessages != 3 || w.HeldRows["messages"] != 3 || w.HeldRows["message_attachments"] < 3 || w.HeldPayloadBytes <= 0 {
		t.Fatalf("held report: %+v", w)
	}
	if pr.ExpelledPostBoundary != 0 || w.ExpelledPostBoundaryTotal != 0 {
		t.Fatalf("post-boundary change expelled with window open: %+v", pr)
	}
	if !strings.Contains(strings.Join(w.Warnings, "|"), "OPEN") || w.Boundary != fixtureBoundary {
		t.Fatalf("window warnings/boundary: %+v", w)
	}
	for _, id := range held {
		if q1(t, sel, `select count(*) from messages where id=?`, id) != "1" {
			t.Fatalf("held message %s expelled", id)
		}
	}
	// Unchanged messages that the archive holds identically ARE expelled.
	if q1(t, sel, `select count(*) from messages where id=?`, x.snapshot) != "0" || pr.Deleted["messages"] < 1 {
		t.Fatalf("unchanged expired message not pruned: %+v", pr.Deleted)
	}
	if pr.Remaining != 0 || pr.Expired != pr.Deleted["messages"]+w.HeldMessages {
		t.Fatalf("accounting: expired %d deleted %d held %d remaining %d", pr.Expired, pr.Deleted["messages"], w.HeldMessages, pr.Remaining)
	}
	if markerField(t, sel, "rollback_window") != WindowOpen {
		t.Fatal("prune changed the window state")
	}
	// Replay: same holds, nothing more deleted.
	pr2, err := Prune(ctx, sel, f.policy, later, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if pr2.Deleted["messages"] != 0 || pr2.RollbackWindow.HeldMessages != 3 {
		t.Fatalf("prune replay: %+v %+v", pr2.Deleted, pr2.RollbackWindow)
	}
	st, err := Status(ctx, sel, f.policy, later)
	if err != nil {
		t.Fatal(err)
	}
	if st.WouldPrune != 0 || st.RollbackWindow == nil || st.RollbackWindow.HeldMessages != 3 || !st.RollbackWindow.Evaluated {
		t.Fatalf("status: would_prune %d window %+v", st.WouldPrune, st.RollbackWindow)
	}

	// Reconcile into the parked archive: lossless on the same IDs.
	rep, err := Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: archive})
	if err != nil {
		t.Fatal(err)
	}
	if rep.TouchedMsgs != 3 || rep.Applied["messages"] != 3 {
		t.Fatalf("reconcile: %+v", rep)
	}
	checks := map[string][2]string{
		"edit text":          {q1(t, archive, `select content from messages where id=?`, x.recentGen), "edited after cutover"},
		"edit history":       {q1(t, archive, `select count(*) from message_edits where message_id=? and edited_at=?`, x.recentGen, post), "1"},
		"reaction":           {q1(t, archive, `select count(*) from reaction_events where message_id=?`, x.recentGen), "1"},
		"tombstone":          {q1(t, archive, `select deleted_at from messages where id=?`, x.embedOnly), post},
		"delete event":       {q1(t, archive, `select count(*) from message_events where message_id=? and event_type='delete'`, x.embedOnly), "1"},
		"new message":        {q1(t, archive, `select count(*) from messages where id=?`, newID), "1"},
		"new attachment":     {q1(t, archive, `select count(*) from message_attachments where message_id=?`, newID), "1"},
		"new event":          {q1(t, archive, `select count(*) from message_events where message_id=?`, newID), "1"},
		"cursor":             {q1(t, archive, `select cursor from sync_state where scope=?`, "channel:"+chGen+":latest"), newID},
		"prune not mirrored": {q1(t, archive, `select count(*) from messages where id=?`, x.snapshot), "1"},
	}
	for name, c := range checks {
		if c[0] != c[1] {
			t.Errorf("%s: got %q want %q", name, c[0], c[1])
		}
	}
	rep2, err := Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: archive})
	if err != nil {
		t.Fatal(err)
	}
	for table, n := range rep2.Applied {
		if n != 0 {
			t.Errorf("reconcile replay applied %d rows in %s", n, table)
		}
	}

	// Explicit close -> next retention pass expels the held delta -> a new
	// reconcile over that boundary refuses (no flag can bypass it).
	if _, err := CloseRollbackWindow(ctx, sel, fixtureBoundary, later); err != nil {
		t.Fatal(err)
	}
	pr3, err := Prune(ctx, sel, f.policy, later, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if pr3.Deleted["messages"] != 3 || pr3.ExpelledPostBoundary != 3 || pr3.RollbackWindow.State != WindowClosed || pr3.RollbackWindow.HeldMessages != 0 {
		t.Fatalf("after close: %+v %+v", pr3.Deleted, pr3.RollbackWindow)
	}
	fresh := filepath.Join(f.dir, "fresh-archive.db")
	copyFile(t, f.source, fresh)
	before, _ := fileSHA(fresh)
	if _, err := Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: fresh}); err == nil || !strings.Contains(err.Error(), "expelled") {
		t.Fatalf("reconcile after close+expulsion must refuse, got %v", err)
	}
	if _, err := Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: fresh, DryRun: true}); err == nil {
		t.Fatal("dry-run must refuse too")
	}
	if after, _ := fileSHA(fresh); after != before {
		t.Fatal("refused reconcile modified the archive")
	}
}

// The explicit close: refusals leave the DB untouched, an interruption before
// commit leaves the window open, replay is a no-op, and nothing else closes it.
func TestCloseRollbackWindowExplicitOnly(t *testing.T) {
	f, _ := buildFixture(t)
	ctx := context.Background()
	sel := filepath.Join(f.dir, "sel.db")
	if _, err := export(t, f, sel, fixtureNow); err != nil {
		t.Fatal(err)
	}
	now := fixtureNow.AddDate(0, 0, 5)
	srcBefore, _ := fileSHA(f.source)
	if _, err := CloseRollbackWindow(ctx, f.source, fixtureBoundary, now); !errors.Is(err, ErrNotSelective) {
		t.Fatalf("non-selective: %v", err)
	}
	if after, _ := fileSHA(f.source); after != srcBefore {
		t.Fatal("refused close modified a non-selective DB")
	}
	for name, b := range map[string]string{"mismatch": "2026-10-15T12:00:01Z", "empty": "", "garbage": "yesterday"} {
		if _, err := CloseRollbackWindow(ctx, sel, b, now); err == nil {
			t.Fatalf("%s boundary accepted", name)
		}
	}
	if _, err := CloseRollbackWindow(ctx, filepath.Join(f.dir, "nope.db"), fixtureBoundary, now); err == nil {
		t.Fatal("missing DB accepted")
	}
	// Interruption before commit: window stays open.
	closeWindowFailpoint = func(string) error { return errors.New("injected interruption") }
	_, err := CloseRollbackWindow(ctx, sel, fixtureBoundary, now)
	closeWindowFailpoint = nil
	if err == nil || markerField(t, sel, "rollback_window") != WindowOpen {
		t.Fatalf("interrupted close: err=%v state=%s", err, markerField(t, sel, "rollback_window"))
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := CloseRollbackWindow(cctx, sel, fixtureBoundary, now); err == nil || markerField(t, sel, "rollback_window") != WindowOpen {
		t.Fatalf("cancelled close: %v", err)
	}
	// Ordinary prune / status never close it.
	if _, err := Prune(ctx, sel, f.policy, now, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := Status(ctx, sel, f.policy, now); err != nil {
		t.Fatal(err)
	}
	if markerField(t, sel, "rollback_window") != WindowOpen {
		t.Fatal("window closed implicitly")
	}
	// Same instant with another precision is accepted.
	rep, err := CloseRollbackWindow(ctx, sel, "2026-10-15T14:00:00.000+02:00", now)
	if err != nil || rep.AlreadyClosed {
		t.Fatalf("close: %+v %v", rep, err)
	}
	if markerField(t, sel, "rollback_window") != WindowClosed || markerField(t, sel, "rollback_window_closed_at") != ts(now) {
		t.Fatal("window not closed")
	}
	marker := q1(t, sel, `select cursor from sync_state where scope='memory:selective'`)
	rep2, err := CloseRollbackWindow(ctx, sel, fixtureBoundary, now.Add(time.Hour))
	if err != nil || !rep2.AlreadyClosed || rep2.ClosedAt != ts(now) {
		t.Fatalf("replay: %+v %v", rep2, err)
	}
	if q1(t, sel, `select cursor from sync_state where scope='memory:selective'`) != marker {
		t.Fatal("replay rewrote the marker")
	}
	// Prune keeps the closed state.
	if _, err := Prune(ctx, sel, f.policy, now, false, false); err != nil {
		t.Fatal(err)
	}
	if markerField(t, sel, "rollback_window") != WindowClosed {
		t.Fatal("prune reopened the window")
	}
}

// Missing/unreadable window information never authorizes an irreversible
// expulsion: legacy marker (no field), unknown state or "closed" without a
// valid closed_at -> treated OPEN with a warning; unusable boundary with the
// window open -> every expired message is held.
func TestRollbackWindowConservativeOnMissingOrUnreadable(t *testing.T) {
	for _, tc := range []struct {
		name, patch, warn string
		holdAll           bool
	}{
		{"legacy marker", `json_remove(cursor,'$.rollback_window')`, "predates", false},
		{"unknown state", `json_set(cursor,'$.rollback_window','maybe')`, "unknown", false},
		{"closed without closed_at", `json_set(cursor,'$.rollback_window','closed')`, "without a valid closed_at", false},
		{"unusable boundary", `json_set(cursor,'$.created_at','not-a-time')`, "unusable", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, x := buildFixture(t)
			ctx := context.Background()
			sel := filepath.Join(f.dir, "sel.db")
			if _, err := export(t, f, sel, fixtureNow); err != nil {
				t.Fatal(err)
			}
			post := ts(fixtureNow.Add(24 * time.Hour))
			execOn(t, sel, `update messages set edited_at=?, updated_at=? where id=?`, post, post, x.recentGen)
			execOn(t, sel, `update sync_state set cursor=`+tc.patch+` where scope='memory:selective'`)
			later := fixtureNow.AddDate(0, 0, 40)
			pr, err := Prune(ctx, sel, f.policy, later, false, false)
			if err != nil {
				t.Fatal(err)
			}
			w := pr.RollbackWindow
			if w.State != WindowOpen || !strings.Contains(strings.Join(w.Warnings, "|"), tc.warn) {
				t.Fatalf("state/warnings: %+v", w)
			}
			if q1(t, sel, `select count(*) from messages where id=?`, x.recentGen) != "1" {
				t.Fatal("post-boundary change expelled")
			}
			if tc.holdAll {
				if pr.Deleted["messages"] != 0 || w.HeldMessages != pr.Expired || pr.Expired == 0 {
					t.Fatalf("unusable boundary must hold every expired message: %+v held=%d", pr.Deleted, w.HeldMessages)
				}
			} else if pr.Deleted["messages"] == 0 || w.HeldMessages != 1 {
				t.Fatalf("unchanged messages must still be pruned: %+v held=%d", pr.Deleted, w.HeldMessages)
			}
			if pr.ExpelledPostBoundary != 0 {
				t.Fatal("loss recorded while conservative")
			}
			if tc.holdAll {
				if _, err := CloseRollbackWindow(ctx, sel, fixtureBoundary, later); err == nil {
					t.Fatal("close accepted with an unusable boundary")
				}
			}
		})
	}
}

// Writer side: while the window is open, records created/edited/deleted
// at/after the boundary are stored even when the rolling policy would skip
// them; closed window or pre-boundary/out-of-scope records keep the policy.
func TestWriterRetentionHoldsPostBoundaryRecordsWhileOpen(t *testing.T) {
	f, _ := buildFixture(t)
	ctx := context.Background()
	sel := filepath.Join(f.dir, "sel.db")
	if _, err := export(t, f, sel, fixtureNow); err != nil {
		t.Fatal(err)
	}
	later := fixtureNow.AddDate(0, 0, 40)
	old := ts(fixtureNow.AddDate(0, -6, 0))
	post := ts(fixtureNow.Add(time.Hour))
	recs := map[string]store.MessageRecord{
		"old edited after boundary":  {ID: "1", GuildID: gEasy, ChannelID: chGen, CreatedAt: old, EditedAt: post},
		"old deleted after boundary": {ID: "2", GuildID: gEasy, ChannelID: chGen, CreatedAt: old, DeletedAt: post},
		"born after boundary, aged":  {ID: "3", GuildID: gEasy, ChannelID: chGen, CreatedAt: post},
		"old unchanged":              {ID: "4", GuildID: gEasy, ChannelID: chGen, CreatedAt: old},
		"old edited before boundary": {ID: "5", GuildID: gEasy, ChannelID: chGen, CreatedAt: old, EditedAt: ts(fixtureNow.Add(-time.Hour))},
		"out of scope edited":        {ID: "6", GuildID: gOther, ChannelID: chGen, CreatedAt: old, EditedAt: post},
	}
	wantOpen := map[string]bool{"old edited after boundary": true, "old deleted after boundary": true, "born after boundary, aged": true}
	check := func(open bool) {
		s, err := store.Open(ctx, sel)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close() }()
		w, err := InstallWriterRetention(ctx, s, f.policy, later)
		if err != nil {
			t.Fatal(err)
		}
		for name, r := range recs {
			want := open && wantOpen[name]
			if got := w.RetainMessage(r); got != want {
				t.Errorf("open=%v %s: retain=%v want %v", open, name, got, want)
			}
		}
	}
	check(true)
	if _, err := CloseRollbackWindow(ctx, sel, fixtureBoundary, later); err != nil {
		t.Fatal(err)
	}
	check(false)
}

// R4-01: a VALID explicit close followed by corruption of ONLY the boundary
// (marker created_at) must not authorize irreversible expulsion. status, prune
// and the writer treat the window as OPEN with a warning and hold every
// expired message; data, children and cursors are preserved and a further
// close is refused without rewriting the marker.
func TestClosedWindowWithUnusableBoundaryHoldsEverything(t *testing.T) {
	f, x := buildFixture(t)
	ctx := context.Background()
	sel := filepath.Join(f.dir, "sel.db")
	if _, err := export(t, f, sel, fixtureNow); err != nil {
		t.Fatal(err)
	}
	post := ts(fixtureNow.Add(24 * time.Hour))
	execOn(t, sel, `update messages set edited_at=?, updated_at=? where id=?`, post, post, x.recentGen)
	execOn(t, sel, `insert into message_edits(message_id, edited_at, content, raw_json) values(?,?,?,?)`, x.recentGen, post, "recent general", "{}")
	later := fixtureNow.AddDate(0, 0, 40)
	if _, err := CloseRollbackWindow(ctx, sel, fixtureBoundary, later); err != nil {
		t.Fatalf("valid explicit close: %v", err)
	}
	if markerField(t, sel, "rollback_window") != WindowClosed || markerField(t, sel, "rollback_window_closed_at") == "" {
		t.Fatal("window not closed with a valid closed_at")
	}
	execOn(t, sel, `update sync_state set cursor=json_set(cursor,'$.created_at','not-a-time') where scope='memory:selective'`)

	msgsBefore := scalar(t, sel, `select count(*) from messages`)
	childBefore := map[string]string{}
	for _, table := range append([]string{"messages"}, messageChildTables...) {
		childBefore[table] = digestOf(t, sel, table, "")
	}
	cursorsBefore := digestOf(t, sel, "sync_state", "scope not like 'memory:%'")
	assertConservative := func(stage string, w *RollbackWindowReport, expired int64) {
		t.Helper()
		if w == nil || w.State != WindowOpen || !strings.Contains(strings.Join(w.Warnings, "|"), "boundary") {
			t.Fatalf("%s: closed window with unusable boundary must be reported OPEN with a boundary warning: %+v", stage, w)
		}
		if expired == 0 || w.HeldMessages != expired {
			t.Fatalf("%s: every expired message must be held: held=%d expired=%d", stage, w.HeldMessages, expired)
		}
	}

	st, err := Status(ctx, sel, f.policy, later)
	if err != nil {
		t.Fatal(err)
	}
	if st.WouldPrune != 0 {
		t.Fatalf("status would prune %d with an unusable boundary", st.WouldPrune)
	}
	assertConservative("status", st.RollbackWindow, st.RollbackWindow.HeldMessages+st.WouldPrune)

	pr, err := Prune(ctx, sel, f.policy, later, false, false)
	if err != nil {
		t.Fatal(err)
	}
	assertConservative("prune", pr.RollbackWindow, pr.Expired)
	if pr.Deleted["messages"] != 0 || pr.ExpelledPostBoundary != 0 {
		t.Fatalf("prune expelled with an unusable boundary: deleted=%+v lost=%d", pr.Deleted, pr.ExpelledPostBoundary)
	}
	if scalar(t, sel, `select count(*) from messages`) != msgsBefore {
		t.Fatal("messages lost")
	}
	for table, d := range childBefore {
		if digestOf(t, sel, table, "") != d {
			t.Fatalf("%s changed", table)
		}
	}
	if digestOf(t, sel, "sync_state", "scope not like 'memory:%'") != cursorsBefore {
		t.Fatal("cursors changed")
	}

	// Native writer side (sync --memory-policy): conservative hold too.
	s, err := store.Open(ctx, sel)
	if err != nil {
		t.Fatal(err)
	}
	w, err := InstallWriterRetention(ctx, s, f.policy, later)
	if err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	old := ts(fixtureNow.AddDate(0, -6, 0))
	if !w.RetainMessage(store.MessageRecord{ID: "1", GuildID: gEasy, ChannelID: chGen, CreatedAt: old, EditedAt: post}) {
		t.Error("writer dropped an edited record with an unusable boundary")
	}
	_ = s.Close()

	// Prune only stamps last_prune_at: it neither reopens nor "repairs" the
	// window/boundary. A further close is refused and does not rewrite it.
	if markerField(t, sel, "rollback_window") != WindowClosed || markerField(t, sel, "created_at") != "not-a-time" {
		t.Fatal("prune rewrote window state or boundary")
	}
	markerBefore := q1(t, sel, `select cursor from sync_state where scope='memory:selective'`)
	if _, err := CloseRollbackWindow(ctx, sel, fixtureBoundary, later.Add(time.Hour)); err == nil {
		t.Fatal("close accepted with an unusable boundary")
	}
	if q1(t, sel, `select cursor from sync_state where scope='memory:selective'`) != markerBefore {
		t.Fatal("refused close rewrote the marker")
	}
}
