package memory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/giveme11us/discrawl-me/internal/store"
)

// StatusReport describes what a DB is and what it retains. Counts only.
type StatusReport struct {
	DBPath          string           `json:"db_path"`
	Selective       bool             `json:"selective_memory"`
	Marker          *Marker          `json:"marker,omitempty"`
	Notice          string           `json:"notice"`
	Messages        int64            `json:"messages"`
	OldestMessage   string           `json:"oldest_message_at,omitempty"`
	NewestMessage   string           `json:"newest_message_at,omitempty"`
	Channels        int64            `json:"channels"`
	SyncStateRows   int64            `json:"sync_state_rows"`
	Embeddings      int64            `json:"message_embeddings"`
	EmbeddingJobs   int64            `json:"embedding_jobs"`
	Reasons         map[string]int64 `json:"retention_reasons,omitempty"`
	WouldPrune      int64            `json:"would_prune,omitempty"`
	PolicyEvaluated string           `json:"policy_evaluated_at,omitempty"`
	// RollbackWindow: state, boundary, held-for-rollback counts/bytes (when
	// a policy is given) and warnings. Only for selective DBs.
	RollbackWindow *RollbackWindowReport `json:"rollback_window,omitempty"`
	File           FileStats             `json:"file"`
}

// SelectiveNotice is what readers must show alongside selective results.
const SelectiveNotice = "selective memory: only messages retained by the policy (rolling window of the policy guilds; INFO rooms and pins only if the policy declares them) are present; absence of a message here does NOT mean it never existed. Catalog and sync cursors are complete. Vector search covers only retained messages that already have an embedding."

// Status opens the DB read-only (never creates/migrates) and reports.
func Status(ctx context.Context, dbPath, policyPath string, now time.Time) (*StatusReport, error) {
	s, err := store.OpenReadOnly(ctx, dbPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.Close() }()
	db := s.DB()
	rep := &StatusReport{DBPath: dbPath}
	m, err := ReadMarker(ctx, db)
	if err != nil {
		return nil, err
	}
	rep.Marker = m
	rep.Selective = m != nil
	if rep.Selective {
		rep.Notice = SelectiveNotice
	} else {
		rep.Notice = "not a selective memory DB (no marker): full archive semantics"
	}
	for table, dst := range map[string]*int64{"messages": &rep.Messages, "channels": &rep.Channels, "sync_state": &rep.SyncStateRows, "message_embeddings": &rep.Embeddings, "embedding_jobs": &rep.EmbeddingJobs} {
		if *dst, err = countRows(ctx, db, table); err != nil {
			return nil, err
		}
	}
	var oldest, newest sql.NullString
	_ = db.QueryRowContext(ctx, `select min(created_at), max(created_at) from messages`).Scan(&oldest, &newest)
	rep.OldestMessage, rep.NewestMessage = oldest.String, newest.String
	rep.File = MeasureFile(ctx, db, dbPath)
	if policyPath != "" {
		pol, err := LoadPolicy(policyPath, now)
		if err != nil {
			return nil, err
		}
		ret, err := LoadRetainer(ctx, db, pol)
		if err != nil {
			return nil, err
		}
		rep.Reasons = map[string]int64{}
		rep.PolicyEvaluated = pol.Now.Format(time.RFC3339)
		rows, err := db.QueryContext(ctx, `select id, guild_id, channel_id, created_at from messages`)
		if err != nil {
			return nil, err
		}
		var expired []string
		for rows.Next() {
			var id, g, c, at string
			if err := rows.Scan(&id, &g, &c, &at); err != nil {
				_ = rows.Close()
				return nil, err
			}
			ok, reason := ret.Decide(g, c, id, at)
			rep.Reasons[reason]++
			if !ok {
				expired = append(expired, id)
			}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if m == nil {
			rep.WouldPrune = int64(len(expired))
		} else {
			expel, hold, err := classifyExpired(ctx, db, m, expired, pol.Now)
			if err != nil {
				return nil, err
			}
			rep.WouldPrune = int64(len(expel))
			if rep.RollbackWindow, err = windowReport(ctx, db, m, hold); err != nil {
				return nil, err
			}
		}
	}
	if m != nil && rep.RollbackWindow == nil {
		if rep.RollbackWindow, err = windowReport(ctx, db, m, nil); err != nil {
			return nil, err
		}
	}
	return rep, nil
}

// PruneReport counts what prune removed.
type PruneReport struct {
	DBPath       string           `json:"db_path"`
	DryRun       bool             `json:"dry_run"`
	PolicySHA256 string           `json:"policy_sha256"`
	Reasons      map[string]int64 `json:"retention_reasons"`
	Deleted      map[string]int64 `json:"deleted_rows"`
	Expired      int64            `json:"expired_messages"`
	Remaining    int64            `json:"expired_remaining_after_bound"`
	MaxDelete    int              `json:"max_delete,omitempty"`
	// ExpelledPostBoundary counts expelled messages that had changed after
	// the rollback boundary (marker created_at): their delta can no longer
	// be reconciled into a parked archive (R2-01, recorded fail-closed).
	ExpelledPostBoundary int64                 `json:"expelled_post_boundary"`
	RollbackWindow       *RollbackWindowReport `json:"rollback_window"`
	SyncStateBef         int64                 `json:"sync_state_cursor_rows_before"`
	SyncStateAft         int64                 `json:"sync_state_cursor_rows_after"`
	Before               FileStats             `json:"file_before"`
	After                FileStats             `json:"file_after"`
}

// ErrNotSelective is returned when a DB lacks the selective marker. The check
// happens on a strictly read-only handle, before any writable open or PRAGMA,
// so a refused DB (source/full archive) is never modified, not even its
// journal mode.
var ErrNotSelective = errors.New("prune refused: DB has no selective memory marker (source/full archives are never pruned)")

// Prune deletes expired messages ONLY from a DB carrying the selective marker.
// Cursors (non-memory sync_state scopes) are never touched.
func Prune(ctx context.Context, dbPath, policyPath string, now time.Time, dryRun, vacuum bool) (*PruneReport, error) {
	return PruneWithLimit(ctx, dbPath, policyPath, now, dryRun, vacuum, 0)
}

// PruneWithLimit is Prune with an optional bound on deleted messages per pass
// (oldest first); 0 = unbounded.
func PruneWithLimit(ctx context.Context, dbPath, policyPath string, now time.Time, dryRun, vacuum bool, maxDelete int) (*PruneReport, error) {
	pol, err := LoadPolicy(policyPath, now)
	if err != nil {
		return nil, err
	}
	// 1) Read-only preflight: regular file, schema v3 exact, marker present.
	ro, err := store.OpenReadOnly(ctx, dbPath)
	if err != nil {
		return nil, fmt.Errorf("prune: %w", err)
	}
	m, err := ReadMarker(ctx, ro.DB())
	if err != nil {
		_ = ro.Close()
		return nil, err
	}
	if m == nil {
		_ = ro.Close()
		return nil, ErrNotSelective
	}
	if dryRun {
		defer func() { _ = ro.Close() }()
		return PruneDB(ctx, ro.DB(), dbPath, pol, true, false, maxDelete)
	}
	if err := ro.Close(); err != nil {
		return nil, err
	}
	// 2) Writable handle only for a verified selective DB. No journal_mode
	// PRAGMA: the selective DB is already WAL from export.
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=rw&_pragma=busy_timeout(30000)", dbPath))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()
	return PruneDB(ctx, db, dbPath, pol, false, vacuum, maxDelete)
}

// PruneDB runs one retention pass on an already opened DB. It re-checks the
// marker (fail-closed), persists INFO bookkeeping (entry/exit observed from
// the current catalog) and deletes expired messages with their children in a
// single transaction. It is used by `memory prune` and by the opt-in
// end-of-cycle retention pass of `sync --memory-policy`.
func PruneDB(ctx context.Context, db *sql.DB, dbPath string, pol *Resolved, dryRun, vacuum bool, maxDelete int) (*PruneReport, error) {
	var version int
	if err := db.QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil || version != 3 {
		return nil, fmt.Errorf("prune: unsupported schema version %d (%v)", version, err)
	}
	m, err := ReadMarker(ctx, db)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, ErrNotSelective
	}
	rep := &PruneReport{DBPath: dbPath, DryRun: dryRun, PolicySHA256: pol.PolicySHA256, Reasons: map[string]int64{}, Deleted: map[string]int64{}, MaxDelete: maxDelete}
	rep.Before = MeasureFile(ctx, db, dbPath)
	cursorCount := func() int64 {
		var n int64
		_ = db.QueryRowContext(ctx, `select count(*) from sync_state where scope not like 'memory:%'`).Scan(&n)
		return n
	}
	rep.SyncStateBef = cursorCount()
	ret, err := LoadRetainer(ctx, db, pol)
	if err != nil {
		return nil, err
	}
	if err := ret.ValidateCatalog(); err != nil {
		return nil, err
	}
	set, del := ret.InfoBookkeeping()
	// Apply bookkeeping to the in-memory view first so decisions see it.
	for k, v := range set {
		if strings.HasPrefix(k, infoLeftPrefix) {
			ret.InfoLeft[k[len(infoLeftPrefix):]] = parseStoredTime(v)
		}
	}
	rows, err := db.QueryContext(ctx, `select id, guild_id, channel_id, created_at from messages order by created_at, id`)
	if err != nil {
		return nil, err
	}
	var drop []string
	for rows.Next() {
		var id, g, c, at string
		if err := rows.Scan(&id, &g, &c, &at); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ok, reason := ret.Decide(g, c, id, at)
		rep.Reasons[reason]++
		if !ok {
			drop = append(drop, id)
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	rep.Expired = int64(len(drop))
	// Decision A (rollback window): while the window is open, expired
	// messages whose latest change (created/updated/edited/deleted/children)
	// is at/after the boundary are HELD, not expelled; an unusable boundary
	// holds every expired message. Once the window is explicitly closed they
	// are expelled and counted as post-boundary losses (R2-01 guard).
	classify := func(q queryer) ([]string, int64, time.Time, error) {
		expel, hold, err := classifyExpired(ctx, q, m, drop, pol.Now)
		if err != nil {
			return nil, 0, time.Time{}, err
		}
		if maxDelete > 0 && len(expel) > maxDelete {
			expel = expel[:maxDelete]
		}
		if rep.RollbackWindow, err = windowReport(ctx, q, m, hold); err != nil {
			return nil, 0, time.Time{}, err
		}
		ids := make([]string, 0, len(expel))
		var lost int64
		var latest time.Time
		for _, d := range expel {
			ids = append(ids, d.id)
			if d.post {
				lost++
				if d.lc.After(latest) {
					latest = d.lc
				}
			}
		}
		return ids, lost, latest, nil
	}
	finish := func(ids []string, lost int64) {
		rep.ExpelledPostBoundary = lost
		rep.Deleted["messages"] = int64(len(ids))
		rep.Remaining = rep.Expired - rep.RollbackWindow.HeldMessages - int64(len(ids))
	}
	if dryRun {
		ids, lost, _, err := classify(db)
		if err != nil {
			return nil, err
		}
		finish(ids, lost)
		rep.SyncStateAft = rep.SyncStateBef
		rep.After = rep.Before
		return rep, nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := applyBookkeeping(ctx, tx, set, del, pol.Now); err != nil {
		return nil, err
	}
	drop, lost, latestLost, err := classify(tx)
	if err != nil {
		return nil, err
	}
	finish(drop, lost)
	if err := recordExpelled(ctx, tx, tx, lost, latestLost, pol.Now); err != nil {
		return nil, err
	}
	for _, id := range drop {
		for _, t := range append(append([]string{}, messageChildTables...), "message_embeddings", "embedding_jobs") {
			res, err := tx.ExecContext(ctx, fmt.Sprintf(`delete from "%s" where message_id = ?`, t), id)
			if err != nil {
				return nil, err
			}
			n, _ := res.RowsAffected()
			rep.Deleted[t] += n
		}
		if rowID, ok := store.MessageFTSRowID(id); ok {
			if _, err := tx.ExecContext(ctx, `delete from message_fts where rowid = ?`, rowID); err != nil {
				return nil, err
			}
		}
		if _, err := tx.ExecContext(ctx, `delete from messages where id = ?`, id); err != nil {
			return nil, err
		}
	}
	m.LastPruneAt = pol.Now.Format(time.RFC3339)
	m.PolicySHA256 = pol.PolicySHA256
	if err := writeMarker(ctx, tx, *m, pol.Now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if vacuum {
		if _, err := db.ExecContext(ctx, `vacuum`); err != nil {
			return nil, err
		}
	}
	_, _ = db.ExecContext(ctx, `pragma wal_checkpoint(TRUNCATE)`)
	rep.SyncStateAft = cursorCount()
	rep.After = MeasureFile(ctx, db, dbPath)
	if rep.SyncStateAft != rep.SyncStateBef {
		return rep, fmt.Errorf("prune invariant violated: cursor rows %d -> %d", rep.SyncStateBef, rep.SyncStateAft)
	}
	return rep, nil
}
