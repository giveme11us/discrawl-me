package memory

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/giveme11us/discrawl-me/internal/store"
)

func export(t *testing.T, f *fixture, out string, now time.Time) (*ExportReport, error) {
	t.Helper()
	return Export(context.Background(), ExportOptions{Source: f.source, Output: out, PolicyPath: f.policy, Now: now, HashSource: true})
}

func TestExportSelectsPolicyAndPreservesCatalogAndCursors(t *testing.T) {
	f, x := buildFixture(t)
	srcSHA, _ := fileSHA(f.source)
	srcSync := digestOf(t, f.source, "sync_state", "")
	srcChannels := digestOf(t, f.source, "channels", "")
	srcGuilds := digestOf(t, f.source, "guilds", "")
	out := filepath.Join(f.dir, "selective.db")
	rep, err := export(t, f, out, fixtureNow)
	if err != nil {
		t.Fatal(err)
	}
	got := messageIDs(t, out)
	want := map[string]bool{x.recentGen: true, x.oldInfo: true, x.oldInfoThread: true, x.pinAfter: true, x.processedOld: true, x.embedOnly: true, x.snapshot: true, x.movedOld: true}
	for id := range want {
		if !got[id] {
			t.Errorf("expected retained %s", id)
		}
	}
	for _, id := range []string{x.ancientGen, x.oldPrivThread, x.pinBefore, x.oldDust} {
		if got[id] {
			t.Errorf("expected expired %s", id)
		}
	}
	if len(got) != len(want) {
		t.Errorf("retained %d, want %d (no extra payload/duplicates)", len(got), len(want))
	}
	// Catalog + every cursor preserved byte-for-byte (plus memory:* bookkeeping only).
	if d := digestOf(t, out, "channels", ""); d != srcChannels {
		t.Error("channel catalog digest changed")
	}
	if d := digestOf(t, out, "guilds", ""); d != srcGuilds {
		t.Error("guild catalog digest changed")
	}
	srcCursors := digestOf(t, f.source, "sync_state", "scope not like 'memory:%'")
	if c := digestOf(t, out, "sync_state", "scope not like 'memory:%' and scope not like 'schema:%'"); c != digestOf(t, f.source, "sync_state", "scope not like 'memory:%' and scope not like 'schema:%'") || srcCursors == "" {
		t.Error("cursors not preserved")
	}
	// Final policy: embeddings and jobs of retained messages are kept, none
	// of expelled messages.
	if n := scalar(t, out, `select count(*) from message_embeddings`); n != int64(len(want)) {
		t.Errorf("embeddings copied: %d, want %d (retained only)", n, len(want))
	}
	if n := scalar(t, out, `select count(*) from embedding_jobs`); n != int64(len(want)) {
		t.Errorf("embedding jobs copied: %d, want %d (retained only)", n, len(want))
	}
	if n := scalar(t, out, `select count(*) from message_embeddings e where not exists (select 1 from messages m where m.id = e.message_id)`); n != 0 {
		t.Errorf("orphan embeddings: %d", n)
	}
	if n := scalar(t, out, `select count(*) from message_attachments`); n != int64(len(want)) {
		t.Errorf("attachments %d, want %d (only retained)", n, len(want))
	}
	if n := scalar(t, out, `select count(*) from message_edits`); n != 1 {
		t.Errorf("edits %d, want 1 (only retained message)", n)
	}
	if n := scalar(t, out, `select count(*) from message_events where message_id not in (select id from messages)`); n != 0 {
		t.Errorf("orphan events %d", n)
	}
	if n := scalar(t, out, `select count(*) from message_fts where message_fts match 'zebrafieldvalue'`); n != 1 {
		t.Errorf("embed field not searchable: %d", n)
	}
	if n := scalar(t, out, `select count(*) from message_fts where message_fts match 'forwardedsnapshotword'`); n != 1 {
		t.Errorf("snapshot not searchable: %d", n)
	}
	if n := scalar(t, out, `select count(*) from messages where raw_json like '%zebrafieldvalue%'`); n != 1 {
		t.Error("raw embed payload lost")
	}
	if rep.ProcessedKept != 2 || rep.ProcessedTotal != 2 {
		t.Errorf("processed kept %d/%d", rep.ProcessedKept, rep.ProcessedTotal)
	}
	// Source unchanged.
	after, _ := fileSHA(f.source)
	if after != srcSHA || digestOf(t, f.source, "sync_state", "") != srcSync {
		t.Error("source modified by export")
	}
	if rep.OutputStats.PageCount == 0 || rep.OutputStats.PageSize == 0 || rep.OutputStats.Bytes == 0 {
		t.Errorf("output stats not measured: %+v", rep.OutputStats)
	}
	if rep.OutputStats.WALBytes != 0 {
		t.Errorf("published output has WAL bytes %d", rep.OutputStats.WALBytes)
	}
	if _, err := os.Stat(out + ".partial-" + itoa(os.Getpid())); !os.IsNotExist(err) {
		t.Error("partial file left behind")
	}
}

func TestExportIsDeterministicAcrossRuns(t *testing.T) {
	f, _ := buildFixture(t)
	a, err := export(t, f, filepath.Join(f.dir, "a.db"), fixtureNow)
	if err != nil {
		t.Fatal(err)
	}
	b, err := export(t, f, filepath.Join(f.dir, "b.db"), fixtureNow)
	if err != nil {
		t.Fatal(err)
	}
	for table, d := range a.OutputDigests {
		if table == "sync_state" { // marker/bookkeeping updated_at identical with same --now
			if b.OutputDigests[table] != d {
				t.Errorf("sync_state digest differs across runs")
			}
			continue
		}
		if b.OutputDigests[table] != d {
			t.Errorf("table %s digest differs across runs", table)
		}
	}
	if _, err := export(t, f, filepath.Join(f.dir, "a.db"), fixtureNow); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("second run onto existing output must be refused, got %v", err)
	}
}

func TestReadOnlyOpenRejectsMutation(t *testing.T) {
	f, _ := buildFixture(t)
	before, _ := fileSHA(f.source)
	s, err := store.OpenReadOnly(context.Background(), f.source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`delete from messages`); err == nil {
		t.Error("mutation on read-only store succeeded")
	}
	_ = s.Close()
	src, err := openSourceRO(context.Background(), f.source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Exec(`insert into sync_state(scope, cursor, updated_at) values('x','y','z')`); err == nil {
		t.Error("mutation on export source succeeded")
	}
	_ = src.Close()
	after, _ := fileSHA(f.source)
	if before != after {
		t.Error("source hash changed")
	}
	if _, err := store.OpenReadOnly(context.Background(), filepath.Join(f.dir, "missing.db")); err == nil {
		t.Error("read-only open created/accepted a missing DB")
	}
	if _, err := os.Stat(filepath.Join(f.dir, "missing.db")); !os.IsNotExist(err) {
		t.Error("missing DB was created")
	}
}

func TestExportFailClosedCases(t *testing.T) {
	f, _ := buildFixture(t)
	out := func(name string) string { return filepath.Join(f.dir, name) }
	link := out("link.db")
	if err := os.Symlink(f.source, link); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func() ExportOptions
		want   string
	}{
		{"source equals dest", func() ExportOptions {
			return ExportOptions{Source: f.source, Output: f.source, PolicyPath: f.policy, Now: fixtureNow}
		}, "differ"},
		{"nonexistent source", func() ExportOptions {
			return ExportOptions{Source: out("nope.db"), Output: out("o1.db"), PolicyPath: f.policy, Now: fixtureNow}
		}, "source"},
		{"symlink source", func() ExportOptions {
			return ExportOptions{Source: link, Output: out("o2.db"), PolicyPath: f.policy, Now: fixtureNow}
		}, "symlink"},
		{"invalid policy json", func() ExportOptions {
			p := out("bad.json")
			_ = os.WriteFile(p, []byte(`{"version":1,`), 0o600)
			return ExportOptions{Source: f.source, Output: out("o3.db"), PolicyPath: p, Now: fixtureNow}
		}, "invalid policy"},
		{"unknown policy field", func() ExportOptions {
			p := out("unk.json")
			pol := defaultPolicy(f)
			pol["retain_everything"] = true
			writeJSON(t, p, pol)
			return ExportOptions{Source: f.source, Output: out("o4.db"), PolicyPath: p, Now: fixtureNow}
		}, "unknown field"},
		{"cross guild pin", func() ExportOptions {
			r := out("reg-cross.json")
			writeJSON(t, r, map[string]any{"releases": []map[string]any{{"slug": "s", "guild_id": gDust, "channel_id": chPin, "since": "2026-09-01T00:00:00Z"}}})
			p := out("pol-cross.json")
			pol := defaultPolicy(f)
			pol["pins"] = map[string]any{"registry_path": r, "state_path": f.state}
			writeJSON(t, p, pol)
			return ExportOptions{Source: f.source, Output: out("o5.db"), PolicyPath: p, Now: fixtureNow}
		}, "registry guild"},
		{"pin guild outside policy", func() ExportOptions {
			r := out("reg-out.json")
			writeJSON(t, r, map[string]any{"releases": []map[string]any{{"slug": "s", "guild_id": gOther, "channel_id": chPin, "since": "2026-09-01T00:00:00Z"}}})
			p := out("pol-out.json")
			pol := defaultPolicy(f)
			pol["pins"] = map[string]any{"registry_path": r, "state_path": f.state}
			writeJSON(t, p, pol)
			return ExportOptions{Source: f.source, Output: out("o6.db"), PolicyPath: p, Now: fixtureNow}
		}, "not in policy"},
		{"missing pin registry", func() ExportOptions {
			p := out("pol-miss.json")
			pol := defaultPolicy(f)
			pol["pins"] = map[string]any{"registry_path": out("absent.json"), "state_path": f.state}
			writeJSON(t, p, pol)
			return ExportOptions{Source: f.source, Output: out("o7.db"), PolicyPath: p, Now: fixtureNow}
		}, "pin registry"},
		{"non-UTC since", func() ExportOptions {
			r := out("reg-tz.json")
			writeJSON(t, r, map[string]any{"releases": []map[string]any{{"slug": "s", "guild_id": gEasy, "channel_id": chPin, "since": "2026-09-01T00:00:00+02:00"}}})
			p := out("pol-tz.json")
			pol := defaultPolicy(f)
			pol["pins"] = map[string]any{"registry_path": r, "state_path": f.state}
			writeJSON(t, p, pol)
			return ExportOptions{Source: f.source, Output: out("o8.db"), PolicyPath: p, Now: fixtureNow}
		}, "not UTC"},
		{"day-month since", func() ExportOptions {
			r := out("reg-dm.json")
			writeJSON(t, r, map[string]any{"releases": []map[string]any{{"slug": "s", "guild_id": gEasy, "channel_id": chPin, "since": "01-09"}}})
			p := out("pol-dm.json")
			pol := defaultPolicy(f)
			pol["pins"] = map[string]any{"registry_path": r, "state_path": f.state}
			writeJSON(t, p, pol)
			return ExportOptions{Source: f.source, Output: out("o9.db"), PolicyPath: p, Now: fixtureNow}
		}, "RFC3339"},
		{"info category in wrong guild", func() ExportOptions {
			p := out("pol-info.json")
			pol := defaultPolicy(f)
			pol["info"] = map[string]any{"guild_id": gDust, "category_ids": []string{catInfo}, "post_transition_days": 90, "catalog_stale_after_hours": 72}
			writeJSON(t, p, pol)
			return ExportOptions{Source: f.source, Output: out("o10.db"), PolicyPath: p, Now: fixtureNow}
		}, "info category"},
		{"existing output", func() ExportOptions {
			_ = os.WriteFile(out("exists.db"), []byte("keep"), 0o600)
			return ExportOptions{Source: f.source, Output: out("exists.db"), PolicyPath: f.policy, Now: fixtureNow}
		}, "already exists"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := tc.mutate()
			_, err := Export(context.Background(), opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
			if tc.name == "existing output" {
				b, _ := os.ReadFile(opts.Output)
				if string(b) != "keep" {
					t.Fatal("existing output overwritten")
				}
				return
			}
			if opts.Output != f.source {
				if _, err := os.Stat(opts.Output); !os.IsNotExist(err) {
					t.Fatalf("output exists after failure")
				}
			}
		})
	}
}

func TestExportMidFailureLeavesNoTarget(t *testing.T) {
	f, _ := buildFixture(t)
	for _, point := range []string{"channels", "messages", "message_events", "commit", "final"} {
		out := filepath.Join(f.dir, "fail-"+point+".db")
		_, err := Export(context.Background(), ExportOptions{Source: f.source, Output: out, PolicyPath: f.policy, Now: fixtureNow, failAfter: point})
		if err == nil {
			t.Fatalf("%s: expected failure", point)
		}
		entries, _ := os.ReadDir(f.dir)
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "fail-"+point) {
				t.Errorf("%s: leftover %s", point, e.Name())
			}
		}
	}
}

func TestPruneOnlySelectiveAndKeepsCursors(t *testing.T) {
	f, x := buildFixture(t)
	ctx := context.Background()
	if _, err := Prune(ctx, f.source, f.policy, fixtureNow, false, false); err == nil || !strings.Contains(err.Error(), "marker") {
		t.Fatalf("prune on source must be refused, got %v", err)
	}
	out := filepath.Join(f.dir, "sel.db")
	if _, err := export(t, f, out, fixtureNow); err != nil {
		t.Fatal(err)
	}
	cursorsBefore := digestOf(t, out, "sync_state", "scope not like 'memory:%'")
	// Same day: nothing to prune; second prune idempotent.
	rep, err := Prune(ctx, out, f.policy, fixtureNow, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Deleted["messages"] != 0 {
		t.Fatalf("prune at export time deleted %d", rep.Deleted["messages"])
	}
	// 40 days later the two recent general messages expire, INFO/pin/processed stay.
	later := fixtureNow.AddDate(0, 0, 40)
	// Refresh catalog observation so it is not stale at `later`.
	touchCatalog(t, out, later.Add(-time.Hour), "")
	rep, err = Prune(ctx, out, f.policy, later, false, true)
	if err != nil {
		t.Fatal(err)
	}
	got := messageIDs(t, out)
	for _, id := range []string{x.recentGen, x.embedOnly, x.snapshot} {
		if got[id] {
			t.Errorf("%s should have expired", id)
		}
	}
	for _, id := range []string{x.oldInfo, x.oldInfoThread, x.pinAfter, x.processedOld, x.movedOld} {
		if !got[id] {
			t.Errorf("%s should be retained", id)
		}
	}
	if c := digestOf(t, out, "sync_state", "scope not like 'memory:%'"); c != cursorsBefore {
		t.Error("prune changed cursors/checkpoints")
	}
	if rep.SyncStateAft != rep.SyncStateBef {
		t.Error("cursor row count changed")
	}
	if n := scalar(t, out, `select count(*) from message_fts where message_id not in (select id from messages)`); n != 0 {
		t.Errorf("orphan fts rows %d", n)
	}
	rep2, err := Prune(ctx, out, f.policy, later, false, false)
	if err != nil || rep2.Deleted["messages"] != 0 {
		t.Fatalf("second prune not idempotent: %v %+v", err, rep2)
	}
}

// touchCatalog sets updated_at (observation) and optionally moves chMoved to ARCHIVE.
func touchCatalog(t *testing.T, path string, observed time.Time, moveTo string) {
	t.Helper()
	s, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustExec(t, s.DB(), `update channels set updated_at = ?`, ts(observed))
	if moveTo != "" {
		mustExec(t, s.DB(), `update channels set parent_id = ? where id = ?`, moveTo, chMoved)
	}
}

func TestInfoTransitionNinetyDaysAndStaleCatalog(t *testing.T) {
	f, x := buildFixture(t)
	ctx := context.Background()
	out := filepath.Join(f.dir, "sel.db")
	if _, err := export(t, f, out, fixtureNow); err != nil {
		t.Fatal(err)
	}
	// Room moved to ARCHIVE but the catalog observation is STALE: no expulsion.
	t1 := fixtureNow.AddDate(0, 0, 10)
	touchCatalog(t, out, fixtureNow.AddDate(0, 0, 1), catArch)
	if _, err := Prune(ctx, out, f.policy, t1, false, false); err != nil {
		t.Fatal(err)
	}
	if !messageIDs(t, out)[x.movedOld] {
		t.Fatal("stale catalog allowed expulsion")
	}
	if n := scalar(t, out, `select count(*) from sync_state where scope like 'memory:info_left:%'`); n != 0 {
		t.Fatal("stale observation confirmed a transition")
	}
	// Fresh observation confirms the move: kept for 90 days from confirmation.
	confirmed := t1.Add(-time.Hour)
	touchCatalog(t, out, confirmed, catArch)
	if _, err := Prune(ctx, out, f.policy, t1, false, false); err != nil {
		t.Fatal(err)
	}
	if !messageIDs(t, out)[x.movedOld] {
		t.Fatal("expelled right after transition")
	}
	t2 := confirmed.AddDate(0, 0, 89)
	touchCatalog(t, out, t2.Add(-time.Hour), catArch)
	if _, err := Prune(ctx, out, f.policy, t2, false, false); err != nil {
		t.Fatal(err)
	}
	if !messageIDs(t, out)[x.movedOld] {
		t.Fatal("expelled before 90 days")
	}
	t3 := confirmed.AddDate(0, 0, 91)
	touchCatalog(t, out, t3.Add(-time.Hour), catArch)
	if _, err := Prune(ctx, out, f.policy, t3, false, false); err != nil {
		t.Fatal(err)
	}
	if messageIDs(t, out)[x.movedOld] {
		t.Fatal("kept after 90 days post-transition")
	}
	if !messageIDs(t, out)[x.oldInfo] {
		t.Fatal("active INFO room message expelled")
	}
	// Cursor for the moved channel still present: no history reimport.
	if n := scalar(t, out, `select count(*) from sync_state where scope = ?`, "channel:"+chMoved+":history_complete"); n != 1 {
		t.Fatal("moved channel cursor lost")
	}
}

func TestStatusReportsSelectiveAndDoesNotCreate(t *testing.T) {
	f, x := buildFixture(t)
	ctx := context.Background()
	out := filepath.Join(f.dir, "sel.db")
	if _, err := export(t, f, out, fixtureNow); err != nil {
		t.Fatal(err)
	}
	ro, err := store.OpenReadOnly(ctx, out)
	if err != nil {
		t.Fatal(err)
	}
	conv, err := ro.GetConversation(ctx, x.oldInfo, 2, 2)
	_ = ro.Close()
	if err != nil || len(conv) != 1 || conv[0].MessageID != x.oldInfo {
		t.Fatalf("conversation on a retained message in a sparse channel: %v %+v", err, conv)
	}
	rep, err := Status(ctx, out, f.policy, fixtureNow)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Selective || rep.Marker == nil || !strings.Contains(rep.Notice, "absence") || rep.Embeddings != rep.Messages || rep.WouldPrune != 0 {
		t.Fatalf("unexpected status %+v", rep)
	}
	src, err := Status(ctx, f.source, "", fixtureNow)
	if err != nil {
		t.Fatal(err)
	}
	if src.Selective {
		t.Fatal("source reported as selective")
	}
	if _, err := Status(ctx, filepath.Join(f.dir, "none.db"), "", fixtureNow); err == nil {
		t.Fatal("status accepted missing DB")
	}
	if _, err := os.Stat(filepath.Join(f.dir, "none.db")); !os.IsNotExist(err) {
		t.Fatal("status created a DB")
	}
}

func TestWriterRetentionSkipsOldKeepsCursorsAndEnqueuesRetained(t *testing.T) {
	f, _ := buildFixture(t)
	ctx := context.Background()
	out := filepath.Join(f.dir, "sel.db")
	if _, err := export(t, f, out, fixtureNow); err != nil {
		t.Fatal(err)
	}
	// Legacy store on the source: policy refused (no marker), default writer unchanged.
	legacy, err := store.Open(ctx, f.source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InstallWriterRetention(ctx, legacy, f.policy, fixtureNow); err == nil {
		t.Fatal("writer policy installed on a non-selective DB")
	}
	if legacy.RetentionActive() {
		t.Fatal("legacy store has retention")
	}
	_ = legacy.Close()

	s, err := store.Open(ctx, out)
	if err != nil {
		t.Fatal(err)
	}
	w, err := InstallWriterRetention(ctx, s, f.policy, fixtureNow)
	if err != nil {
		t.Fatal(err)
	}
	mk := func(id, ch string, at time.Time) store.MessageMutation {
		return store.MessageMutation{Record: store.MessageRecord{ID: id, GuildID: gEasy, ChannelID: ch, CreatedAt: ts(at), Content: "w " + id, NormalizedContent: "w " + id, RawJSON: `{"embeds":[{"fields":[{"name":"n","value":"writerfieldword"}]}]}`}, Options: store.WriteOptions{EnqueueEmbedding: true}}
	}
	muts := []store.MessageMutation{
		mk("410000000000000001", chGen, fixtureNow.AddDate(-2, 0, 0)),  // ancient general: skipped
		mk("410000000000000002", chGen, fixtureNow.Add(-time.Hour)),    // recent: kept
		mk("410000000000000003", chInfo, fixtureNow.AddDate(-1, 0, 0)), // INFO: kept
	}
	if err := s.UpsertMessages(ctx, muts); err != nil {
		t.Fatal(err)
	}
	if s.RetentionSkipped() != 1 {
		t.Fatalf("skipped %d, want 1", s.RetentionSkipped())
	}
	var n int
	_ = s.DB().QueryRow(`select count(*) from messages where id like '41%'`).Scan(&n)
	if n != 2 {
		t.Fatalf("written %d, want 2", n)
	}
	// Retained messages are enqueued exactly like on a full archive when the
	// caller asks for it; the skipped one is not.
	_ = s.DB().QueryRow(`select count(*) from embedding_jobs where message_id like '41%'`).Scan(&n)
	if n != 2 {
		t.Fatalf("embedding jobs enqueued for retained writes: %d, want 2", n)
	}
	_ = s.DB().QueryRow(`select count(*) from message_fts where message_fts match 'writerfieldword'`).Scan(&n)
	if n != 2 {
		t.Fatalf("writer FTS embed fields: %d", n)
	}
	if !w.SinceFloor(chGen).Equal(fixtureNow.AddDate(0, 0, -30)) {
		t.Errorf("general floor %v", w.SinceFloor(chGen))
	}
	if !w.SinceFloor(chInfo).IsZero() {
		t.Errorf("INFO floor must be unbounded")
	}
	if !w.SinceFloor(chPin).Equal(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("pin floor %v", w.SinceFloor(chPin))
	}
	// Edits/reactions/deletes of a non-retained (expelled/never stored) message are dropped.
	if err := s.AppendMessageEdit(ctx, store.MessageEditRecord{MessageID: "410000000000000001", Content: "x", RawJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendReactionEvent(ctx, store.ReactionEventRecord{MessageID: "410000000000000001", ChannelID: chGen, GuildID: gEasy, UserID: "u", EmojiName: "e", Action: "add"}); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkMessageDeleted(ctx, gEasy, chGen, "410000000000000001", map[string]string{}); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`select count(*) from message_edits where message_id = '410000000000000001'`, `select count(*) from reaction_events`, `select count(*) from message_events where message_id = '410000000000000001'`} {
		_ = s.DB().QueryRow(q).Scan(&n)
		if n != 0 {
			t.Errorf("orphan row for non-retained message: %s -> %d", q, n)
		}
	}
	if err := s.AppendMessageEdit(ctx, store.MessageEditRecord{MessageID: "410000000000000002", Content: "x", RawJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	_ = s.DB().QueryRow(`select count(*) from message_edits where message_id = '410000000000000002'`).Scan(&n)
	if n != 1 {
		t.Errorf("edit of retained message dropped")
	}
	_ = s.Close()
}
