package memory

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// R2-02: the delta boundary and newest-wins compare INSTANTS, not TEXT.
// Marker boundary 12:00:00Z; an edit at 12:00:00.500Z is AFTER it although
// '...00.500Z' < '...00Z' lexicographically. Mixed precisions/offsets on the
// archive side must not make an older version win, and equal instants
// written with different precision must not re-apply (idempotency).
func TestReconcileComparesInstantsNotText(t *testing.T) {
	f, x := buildFixture(t)
	ctx := context.Background()
	sel := filepath.Join(f.dir, "sel.db")
	if _, err := export(t, f, sel, fixtureNow); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(f.dir, "parked-archive.db")
	copyFile(t, f.source, archive)

	// Default boundary = marker created_at (whole seconds).
	if got := q1(t, sel, `select json_extract(cursor,'$.created_at') from sync_state where scope='memory:selective'`); got != "2026-10-15T12:00:00Z" {
		t.Fatalf("marker created_at precondition: %q", got)
	}
	frac := "2026-10-15T12:00:00.5Z"
	execOn(t, sel, `update messages set content='edited half second later', normalized_content='edited half second later', edited_at=?, updated_at=? where id=?`, frac, frac, x.recentGen)
	execOn(t, sel, `insert into message_edits(message_id, edited_at, content, raw_json) values(?,?,?,?)`, x.recentGen, frac, "recent general", "{}")

	rep, err := Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: archive})
	if err != nil {
		t.Fatal(err)
	}
	if rep.TouchedMsgs != 1 || rep.Applied["messages"] != 1 || rep.Applied["message_edits"] != 1 {
		t.Fatalf("fractional post-boundary edit missed: %+v", rep)
	}
	if got := q1(t, archive, `select content from messages where id=?`, x.recentGen); got != "edited half second later" {
		t.Fatalf("archive content %q", got)
	}

	// Idempotent: same instant, other precision on the archive side.
	execOn(t, archive, `update messages set updated_at='2026-10-15T12:00:00.500000+00:00' where id=?`, x.recentGen)
	rep2, err := Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: archive})
	if err != nil {
		t.Fatal(err)
	}
	if rep2.Applied["messages"] != 0 || rep2.KeptNewer["messages"] != 1 {
		t.Fatalf("equal instant re-applied: %+v", rep2)
	}

	// Archive-newer by 0.2s, written with offset and fewer digits: kept.
	execOn(t, archive, `update messages set content='archive newer', updated_at='2026-10-15T14:00:00.7+02:00' where id=?`, x.recentGen)
	rep3, err := Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: archive})
	if err != nil {
		t.Fatal(err)
	}
	if got := q1(t, archive, `select content from messages where id=?`, x.recentGen); got != "archive newer" || rep3.KeptNewer["messages"] != 1 {
		t.Fatalf("archive-newer lost: %q %+v", got, rep3)
	}

	// Selective newer by 1ms although TEXT order says otherwise
	// ('...00.501Z' vs archive '...00.5009999+00:00' and lexically '+' < 'Z').
	execOn(t, archive, `update messages set content='archive older', updated_at='2026-10-15T12:00:00.5009999+00:00' where id=?`, x.recentGen)
	execOn(t, sel, `update messages set updated_at='2026-10-15T12:00:00.501Z' where id=?`, x.recentGen)
	if _, err := Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: archive}); err != nil {
		t.Fatal(err)
	}
	if got := q1(t, archive, `select content from messages where id=?`, x.recentGen); got != "edited half second later" {
		t.Fatalf("newer selective version not applied: %q", got)
	}

	// Pre-boundary activity stays out of the delta (boundary is exclusive of
	// the past, inclusive of the instant itself).
	execOn(t, sel, `update messages set updated_at='2026-10-15T11:59:59.999999+00:00' where id=?`, x.oldInfo)
	rep4, err := Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: archive, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep4.TouchedMsgs != 1 {
		t.Fatalf("pre-boundary row selected: %+v", rep4)
	}
}

// R2-01 (fail-closed part, design-neutral): when retention expels a message
// that changed after the rollback boundary (edit/delete/children or created
// after it), the expelled delta can no longer be reconciled. Prune records it
// and reconcile REFUSES instead of exiting 0 and advancing cursors with the
// archive still holding the old version.
func TestReconcileRefusesWhenRetentionExpelledPostBoundaryChanges(t *testing.T) {
	f, x := buildFixture(t)
	ctx := context.Background()
	sel := filepath.Join(f.dir, "sel.db")
	if _, err := export(t, f, sel, fixtureNow); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(f.dir, "parked-archive.db")
	copyFile(t, f.source, archive)
	post := ts(fixtureNow.Add(24 * time.Hour))
	execOn(t, sel, `update messages set content='edited after cutover', edited_at=?, updated_at=? where id=?`, post, post, x.recentGen)
	execOn(t, sel, `insert into message_edits(message_id, edited_at, content, raw_json) values(?,?,?,?)`, x.recentGen, post, "recent general", "{}")
	execOn(t, sel, `update sync_state set cursor='499999999999999998', updated_at=? where scope like 'channel:%:latest_message_id'`, post)

	// Decision A: with the window OPEN the change would be held (see
	// rollback_window_test.go). This test covers the state AFTER the explicit
	// close: retention expels it and reconcile must refuse.
	if _, err := CloseRollbackWindow(ctx, sel, "2026-10-15T12:00:00Z", fixtureNow.Add(25*time.Hour)); err != nil {
		t.Fatal(err)
	}
	pr, err := Prune(ctx, sel, f.policy, fixtureNow.AddDate(0, 1, 0), false, false)
	if err != nil {
		t.Fatal(err)
	}
	if q1(t, sel, `select count(*) from messages where id=?`, x.recentGen) != "0" {
		t.Fatal("setup: edited message not expelled")
	}
	if pr.ExpelledPostBoundary < 1 {
		t.Fatalf("prune did not count the expelled post-boundary change: %+v", pr)
	}
	before, _ := fileSHA(archive)
	_, err = Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: archive})
	if err == nil || !strings.Contains(err.Error(), "expelled") {
		t.Fatalf("reconcile must refuse, got %v", err)
	}
	if after, _ := fileSHA(archive); after != before {
		t.Fatal("refused reconcile modified the archive (cursors advanced?)")
	}
	// A boundary after the lost change does not hit the guard.
	if _, err := Reconcile(ctx, ReconcileOptions{Selective: sel, Archive: archive, Since: fixtureNow.Add(48 * time.Hour), DryRun: true}); err != nil {
		t.Fatalf("later boundary: %v", err)
	}
}
