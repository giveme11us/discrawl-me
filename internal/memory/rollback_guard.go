package memory

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ExpelledScope records, in the selective DB only (memory:* scope, never
// copied by reconcile, never counted as a cursor), how many messages
// retention expelled AFTER they had changed at/after the rollback boundary
// (marker created_at), and the latest such change. Those deltas cannot be
// reconciled into a parked archive any more, so `memory reconcile` refuses
// any boundary at/before LatestChangeAt instead of exiting 0 and advancing
// cursors over a loss (R2-01, fail-closed part).
const ExpelledScope = "memory:rollback_expelled_post_boundary"

type expelledRecord struct {
	Count          int64  `json:"count"`
	LatestChangeAt string `json:"latest_change_at,omitempty"`
}

func readExpelled(ctx context.Context, q queryer) (expelledRecord, error) {
	var rec expelledRecord
	rows, err := q.QueryContext(ctx, `select cursor from sync_state where scope = ?`, ExpelledScope)
	if err != nil {
		return rec, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return rec, rows.Err()
	}
	var raw sql.NullString
	if err := rows.Scan(&raw); err != nil {
		return rec, err
	}
	if err := json.Unmarshal([]byte(raw.String), &rec); err != nil {
		return rec, fmt.Errorf("%s unreadable (refusing): %w", ExpelledScope, err)
	}
	return rec, nil
}

// latestChange returns the latest instant at which message id or any of its
// children changed (creation, update, edit, delete, attachment, events).
// Unparseable timestamps count as "changed now" (conservative).
func latestChange(ctx context.Context, q queryer, id string, now time.Time) (time.Time, error) {
	var latest time.Time
	for _, stmt := range []string{
		`select created_at, updated_at, edited_at, deleted_at from messages where id = ?`,
		`select edited_at, null, null, null from message_edits where message_id = ?`,
		`select updated_at, null, null, null from message_attachments where message_id = ?`,
		`select event_at, null, null, null from message_events where message_id = ?`,
		`select event_at, null, null, null from reaction_events where message_id = ?`,
		`select event_at, null, null, null from mention_events where message_id = ?`,
	} {
		rows, err := q.QueryContext(ctx, stmt, id)
		if err != nil {
			return latest, err
		}
		vals, err := scanRows(rows, 4)
		if err != nil {
			return latest, err
		}
		for _, v := range vals {
			for _, at := range v {
				t, ok, bad := parseInstant(at)
				if bad {
					t, ok = now, true
				}
				if ok && t.After(latest) {
					latest = t
				}
			}
		}
	}
	return latest, nil
}

func recordExpelled(ctx context.Context, tx execer, q queryer, add int64, latest time.Time, now time.Time) error {
	if add == 0 {
		return nil
	}
	rec, err := readExpelled(ctx, q)
	if err != nil {
		return err
	}
	rec.Count += add
	if cur, ok, _ := parseInstant(rec.LatestChangeAt); !ok || latest.After(cur) {
		rec.LatestChangeAt = latest.UTC().Format(time.RFC3339Nano)
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `insert into sync_state(scope, cursor, updated_at) values(?,?,?)
		on conflict(scope) do update set cursor=excluded.cursor, updated_at=excluded.updated_at`,
		ExpelledScope, string(b), now.UTC().Format(time.RFC3339Nano))
	return err
}

var errNoBoundary = errors.New("selective marker created_at unusable")

func markerBoundary(m *Marker) (time.Time, error) {
	t, ok, _ := parseInstant(m.CreatedAt)
	if !ok {
		return time.Time{}, errNoBoundary
	}
	return t, nil
}
