package memory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/giveme11us/discrawl-me/internal/store"
)

// Rollback window (owner decision A, R2-01 lossless part).
//
// While the full archive is parked (export -> cutover -> final yes), the
// selective memory is the only place where post-boundary activity exists. The
// window is OPEN from export until an explicit, separate close command: while
// open, retention (memory prune and the in-cycle pass of sync --memory-policy)
// does NOT expel a message whose latest change (creation, update, edit,
// delete, attachment, events) is at/after the boundary (marker created_at);
// unchanged messages, which the archive holds identically, are expelled
// normally. The native writer also accepts records created/edited/deleted
// at/after the boundary while the window is open.
//
// Nothing closes the window implicitly: not sync, not prune, not a target size
// or time. Growth is proportional to post-boundary activity for the duration
// of the window; there is no size cap. A marker without the field, with an
// unknown value, a "closed" without a valid closed_at or a "closed" whose
// boundary is unusable is treated as OPEN (conservative) and reported as a
// warning. An unusable boundary with the window open holds EVERY expired
// message (no irreversible expulsion).
const (
	WindowOpen   = "open"
	WindowClosed = "closed"
)

// RollbackWindowReport is reported by status and prune. Counts/bytes only.
type RollbackWindowReport struct {
	State    string `json:"state"`
	Boundary string `json:"boundary_utc,omitempty"`
	ClosedAt string `json:"closed_at,omitempty"`
	// HeldMessages are policy-expired messages kept ONLY because they changed
	// at/after the boundary while the window is open.
	HeldMessages int64            `json:"held_for_rollback_messages"`
	HeldRows     map[string]int64 `json:"held_for_rollback_rows,omitempty"`
	// HeldPayloadBytes sums the stored column lengths of held messages and
	// their child rows (excludes FTS, indexes and page overhead); file size
	// is measured separately in file/file_after.
	HeldPayloadBytes int64 `json:"held_for_rollback_payload_bytes"`
	// ExpelledPostBoundaryTotal is the persisted count of messages expelled
	// after changing at/after the boundary (only possible once closed, or
	// recorded by an earlier build): reconcile refuses over that loss.
	ExpelledPostBoundaryTotal int64    `json:"expelled_post_boundary_total"`
	LatestExpelledChangeAt    string   `json:"latest_expelled_change_at,omitempty"`
	Warnings                  []string `json:"warnings,omitempty"`
	Evaluated                 bool     `json:"held_counts_evaluated"`
}

// windowState returns whether the window is open (conservatively) plus
// warnings describing why.
func windowState(m *Marker) (bool, []string) {
	var warns []string
	open := true
	switch m.RollbackWindow {
	case WindowClosed:
		_, closedOK, _ := parseInstant(m.RollbackClosedAt)
		_, berr := markerBoundary(m)
		switch {
		case !closedOK:
			warns = append(warns, "rollback window marked closed without a valid closed_at: treated as OPEN (conservative)")
		case berr != nil:
			// R4-01: a closed window is only honoured with a usable
			// boundary; otherwise nothing proves which changes are safe
			// to expel irreversibly.
			warns = append(warns, "rollback window marked closed but the boundary (marker created_at) is unusable: treated as OPEN (conservative)")
		default:
			open = false
		}
	case WindowOpen:
	case "":
		warns = append(warns, "marker predates the rollback window field: treated as OPEN (conservative)")
	default:
		warns = append(warns, fmt.Sprintf("unknown rollback window state %q: treated as OPEN (conservative)", m.RollbackWindow))
	}
	if open {
		warns = append(warns, "rollback window OPEN: changes at/after the boundary are held beyond retention until an explicit `memory close-rollback-window`; growth is proportional to post-boundary activity, no size cap, no automatic close")
		if _, err := markerBoundary(m); err != nil {
			warns = append(warns, "rollback boundary (marker created_at) unusable: every expired message is held, nothing is expelled")
		}
	}
	return open, warns
}

type expireDecision struct {
	id   string
	post bool // latest change at/after boundary (or boundary unusable)
	lc   time.Time
}

// classifyExpired splits policy-expired messages into those to expel and
// those held for rollback (window open and post-boundary change).
func classifyExpired(ctx context.Context, q queryer, m *Marker, drop []string, now time.Time) (expel []expireDecision, hold []string, err error) {
	boundary, berr := markerBoundary(m)
	open, _ := windowState(m)
	hold = []string{} // non-nil: "evaluated, nothing held" differs from "not evaluated"
	for _, id := range drop {
		lc, err := latestChange(ctx, q, id, now)
		if err != nil {
			return nil, nil, err
		}
		post := berr != nil || !lc.Before(boundary)
		if post && open {
			hold = append(hold, id)
			continue
		}
		expel = append(expel, expireDecision{id: id, post: post, lc: lc})
	}
	return expel, hold, nil
}

// heldWeight measures rows and stored payload bytes of the given messages and
// their children.
func heldWeight(ctx context.Context, q queryer, ids []string) (map[string]int64, int64, error) {
	rowsBy := map[string]int64{}
	var bytes int64
	tables := append([]string{"messages"}, messageChildTables...)
	for _, table := range tables {
		cols, err := tableColumns(ctx, q, table)
		if err != nil {
			return nil, 0, err
		}
		expr := "0"
		for _, c := range cols {
			expr += fmt.Sprintf(` + coalesce(length(cast("%s" as blob)),0)`, c)
		}
		key := "message_id"
		if table == "messages" {
			key = "id"
		}
		stmt := fmt.Sprintf(`select count(*), coalesce(sum(%s),0) from "%s" where "%s" = ?`, expr, table, key)
		for _, id := range ids {
			rows, err := q.QueryContext(ctx, stmt, id)
			if err != nil {
				return nil, 0, err
			}
			vals, err := scanRows(rows, 2)
			if err != nil {
				return nil, 0, err
			}
			for _, v := range vals {
				n, _ := v[0].(int64)
				b, _ := v[1].(int64)
				rowsBy[table] += n
				bytes += b
			}
		}
	}
	return rowsBy, bytes, nil
}

// windowReport builds the report; when held is non-nil the held counts are
// measured on q.
func windowReport(ctx context.Context, q queryer, m *Marker, held []string) (*RollbackWindowReport, error) {
	open, warns := windowState(m)
	rep := &RollbackWindowReport{State: WindowClosed, Boundary: m.CreatedAt, Warnings: warns}
	if open {
		rep.State = WindowOpen
	} else {
		rep.ClosedAt = m.RollbackClosedAt
	}
	lost, err := readExpelled(ctx, q)
	if err != nil {
		return nil, err
	}
	rep.ExpelledPostBoundaryTotal, rep.LatestExpelledChangeAt = lost.Count, lost.LatestChangeAt
	if held != nil {
		rep.Evaluated = true
		rep.HeldMessages = int64(len(held))
		if rep.HeldRows, rep.HeldPayloadBytes, err = heldWeight(ctx, q, held); err != nil {
			return nil, err
		}
	}
	return rep, nil
}

// CloseWindowReport is the outcome of an explicit close.
type CloseWindowReport struct {
	DBPath        string `json:"db_path"`
	Boundary      string `json:"boundary_utc"`
	ClosedAt      string `json:"closed_at"`
	AlreadyClosed bool   `json:"already_closed"`
	Notice        string `json:"notice"`
}

// closeWindowFailpoint is a test-only hook (stage "before-commit").
var closeWindowFailpoint func(stage string) error

// CloseRollbackWindow is the ONLY way to close the window. It is never called
// by sync or prune. confirmBoundary must denote the same instant as the
// marker boundary (guards against closing the wrong DB). The change is one
// transaction on the marker only: an error or interruption leaves the window
// open; replay on a closed window is a no-op. After closing, the next
// retention pass expels post-boundary changes that are out of retention and
// `memory reconcile` will refuse a boundary covering them: rollback after
// close is a historical restore from the cold backup, which does NOT contain
// the expelled delta.
func CloseRollbackWindow(ctx context.Context, dbPath, confirmBoundary string, now time.Time) (*CloseWindowReport, error) {
	if dbPath == "" || confirmBoundary == "" {
		return nil, errors.New("close-rollback-window requires --db and --confirm-boundary")
	}
	confirm, ok, _ := parseInstant(confirmBoundary)
	if !ok {
		return nil, fmt.Errorf("--confirm-boundary %q is not a valid timestamp", confirmBoundary)
	}
	info, err := os.Lstat(dbPath)
	if err != nil {
		return nil, fmt.Errorf("close-rollback-window: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, errors.New("close-rollback-window: DB must be a regular file, not a symlink")
	}
	// Read-only preflight: marker, boundary, state. No write on refusal.
	ro, err := store.OpenReadOnly(ctx, dbPath)
	if err != nil {
		return nil, fmt.Errorf("close-rollback-window: %w", err)
	}
	m, err := ReadMarker(ctx, ro.DB())
	_ = ro.Close()
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("close-rollback-window refused, nothing written: %w", ErrNotSelective)
	}
	check := func(m *Marker) (bool, error) {
		boundary, err := markerBoundary(m)
		if err != nil {
			return false, errors.New("close-rollback-window refused: marker boundary unusable; the window stays open")
		}
		if !boundary.Equal(confirm) {
			return false, fmt.Errorf("close-rollback-window refused: --confirm-boundary %s does not match the marker boundary %s", confirm.Format(time.RFC3339Nano), m.CreatedAt)
		}
		open, _ := windowState(m)
		return !open, nil
	}
	closed, err := check(m)
	if err != nil {
		return nil, err
	}
	rep := &CloseWindowReport{DBPath: dbPath, Boundary: m.CreatedAt, Notice: "rollback window closed: from the next retention pass post-boundary changes out of retention are expelled; rollback is then a historical restore from the cold backup, which does not contain the expelled delta"}
	if closed {
		rep.AlreadyClosed, rep.ClosedAt = true, m.RollbackClosedAt
		return rep, nil
	}
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=rw&_pragma=busy_timeout(30000)", dbPath))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	cur, err := ReadMarker(ctx, tx)
	if err != nil {
		return nil, err
	}
	if cur == nil {
		return nil, fmt.Errorf("close-rollback-window refused, nothing written: %w", ErrNotSelective)
	}
	if closed, err = check(cur); err != nil {
		return nil, err
	}
	if closed {
		rep.AlreadyClosed, rep.ClosedAt = true, cur.RollbackClosedAt
		return rep, nil
	}
	cur.RollbackWindow = WindowClosed
	cur.RollbackClosedAt = now.UTC().Format(time.RFC3339Nano)
	if err := writeMarker(ctx, tx, *cur, now); err != nil {
		return nil, err
	}
	if closeWindowFailpoint != nil {
		if err := closeWindowFailpoint("before-commit"); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	_, _ = db.ExecContext(ctx, `pragma wal_checkpoint(TRUNCATE)`)
	rep.ClosedAt = cur.RollbackClosedAt
	return rep, nil
}
