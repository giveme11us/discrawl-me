package memory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/giveme11us/discrawl-me/internal/store"
)

// ReconcileOptions drives the rollback reconciliation: changes collected by
// the selective memory after the boundary are applied BY ID to a parked full
// archive (never the other way round, never over a live DB).
type ReconcileOptions struct {
	Selective string
	Archive   string
	// Since is the delta boundary (UTC). Default: the export snapshot time
	// stored in the selective marker (covers export->cutover->rollback).
	Since  time.Time
	DryRun bool
}

// ReconcileReport counts rows only; no message content is ever logged.
type ReconcileReport struct {
	Selective   string           `json:"selective"`
	Archive     string           `json:"archive"`
	Since       string           `json:"since_utc"`
	DryRun      bool             `json:"dry_run"`
	TouchedMsgs int              `json:"touched_messages"`
	Candidates  map[string]int64 `json:"candidate_rows"`
	Applied     map[string]int64 `json:"applied_rows"`
	KeptNewer   map[string]int64 `json:"archive_newer_kept"`
}

// tables with a primary key and an updated_at column: upsert newest wins.
var reconcilePK = []struct {
	table string
	pk    []string
}{
	{"guilds", []string{"id"}},
	{"channels", []string{"id"}},
	{"members", []string{"guild_id", "user_id"}},
}

// append-only event tables (autoincrement id, deduplicated by content).
var reconcileEvents = []string{"message_events", "reaction_events", "mention_events"}

func tableColumns(ctx context.Context, q queryer, table string) ([]string, error) {
	rows, err := q.QueryContext(ctx, fmt.Sprintf(`select name from pragma_table_info('%s') order by cid`, table))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

func quoteCols(cols []string) string {
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = `"` + c + `"`
	}
	return strings.Join(q, ",")
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func scanRows(rows *sql.Rows, n int) ([][]any, error) {
	defer func() { _ = rows.Close() }()
	var out [][]any
	for rows.Next() {
		vals := make([]any, n)
		ptrs := make([]any, n)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		out = append(out, vals)
	}
	return out, rows.Err()
}

// Reconcile applies post-boundary changes of a selective memory to a parked
// full archive by ID: new and UPDATED messages (edits, soft deletes /
// tombstones), their attachments and edit history, message/reaction/mention
// events, catalog rows and sync cursors. Newer archive versions are kept
// (updated_at comparison), so the operation is idempotent and never loses data
// already present in the archive. Expired rows pruned from the selective
// memory are NOT propagated (prune never deletes from the archive).
// Everything runs in ONE archive transaction: on any error the archive is
// unchanged. The selective DB is opened read-only.
func Reconcile(ctx context.Context, opts ReconcileOptions) (*ReconcileReport, error) {
	if opts.Selective == "" || opts.Archive == "" {
		return nil, errors.New("reconcile requires --selective and --archive")
	}
	ai, err := os.Lstat(opts.Archive)
	if err != nil {
		return nil, fmt.Errorf("archive: %w", err)
	}
	si, err := os.Lstat(opts.Selective)
	if err != nil {
		return nil, fmt.Errorf("selective: %w", err)
	}
	if os.SameFile(ai, si) {
		return nil, errors.New("reconcile: selective and archive must differ")
	}
	sel, err := store.OpenReadOnly(ctx, opts.Selective)
	if err != nil {
		return nil, fmt.Errorf("selective: %w", err)
	}
	defer func() { _ = sel.Close() }()
	m, err := ReadMarker(ctx, sel.DB())
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("reconcile: --selective has no selective memory marker")
	}
	// Archive preflight strictly read-only: refuse a selective DB as target.
	aro, err := store.OpenReadOnly(ctx, opts.Archive)
	if err != nil {
		return nil, fmt.Errorf("archive: %w", err)
	}
	am, err := ReadMarker(ctx, aro.DB())
	_ = aro.Close()
	if err != nil {
		return nil, err
	}
	if am != nil {
		return nil, errors.New("reconcile: --archive carries the selective marker; target must be the full archive")
	}
	since := opts.Since
	if since.IsZero() {
		if since, err = parseUTC(m.CreatedAt); err != nil {
			return nil, fmt.Errorf("reconcile: marker created_at unusable, pass --since: %w", err)
		}
	}
	since = since.UTC()
	bound := since.Format(time.RFC3339Nano)
	// R2-01 fail-closed: if retention already expelled messages that changed
	// at/after the boundary, their delta is gone from the selective memory;
	// applying the rest (and the advanced cursors) would silently lose it.
	if lost, err := readExpelled(ctx, sel.DB()); err != nil {
		return nil, err
	} else if lost.Count > 0 && atOrAfter(lost.LatestChangeAt, since) {
		return nil, fmt.Errorf("reconcile refused: retention expelled %d message(s) changed after the boundary (latest change %s) before they could be reconciled; archive left unchanged, cursors not advanced", lost.Count, lost.LatestChangeAt)
	}
	rep := &ReconcileReport{Selective: opts.Selective, Archive: opts.Archive, Since: bound, DryRun: opts.DryRun, Candidates: map[string]int64{}, Applied: map[string]int64{}, KeptNewer: map[string]int64{}}

	mode := "rw"
	if opts.DryRun {
		mode = "ro"
	}
	adb, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=%s&_pragma=busy_timeout(30000)", opts.Archive, mode))
	if err != nil {
		return nil, err
	}
	adb.SetMaxOpenConns(1)
	defer func() { _ = adb.Close() }()
	stx, err := sel.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = stx.Rollback() }()
	atxReal, err := adb.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = atxReal.Rollback() }()
	// Dry-run: archive opened mode=ro and NO write statement is executed;
	// only candidate rows are counted (applied/kept stay empty).
	atx := archiveWriter{tx: atxReal, dry: opts.DryRun}

	// Touched message IDs: created/updated after the boundary, or with child
	// activity after it. Only IDs still present in the selective DB.
	// Instants are compared in Go (R2-02), never as TEXT.
	touched := map[string]bool{}
	for _, q := range []string{
		`select id, created_at, updated_at, edited_at, deleted_at from messages`,
		`select message_id, edited_at from message_edits`,
		`select message_id, updated_at from message_attachments`,
		`select message_id, event_at from message_events`,
		`select message_id, event_at from reaction_events`,
		`select message_id, event_at from mention_events`,
	} {
		n := strings.Count(q, ",") + 1
		rows, err := stx.QueryContext(ctx, q)
		if err != nil {
			return nil, err
		}
		vals, err := scanRows(rows, n)
		if err != nil {
			return nil, err
		}
		for _, v := range vals {
			for _, at := range v[1:] {
				if atOrAfter(at, since) {
					touched[fmt.Sprint(v[0])] = true
					break
				}
			}
		}
	}
	ids := make([]string, 0, len(touched))
	for id := range touched {
		var one int
		if err := stx.QueryRowContext(ctx, `select 1 from messages where id = ?`, id).Scan(&one); err == nil {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	rep.TouchedMsgs = len(ids)

	// upsertNewest copies rows of table selected by where/args (and, when
	// tsFilter is set, whose updated_at is at/after the boundary as an
	// INSTANT) to the archive; newest updated_at wins, compared as instants.
	upsertNewest := func(table string, pk []string, tsFilter bool, where string, args ...any) error {
		cols, err := tableColumns(ctx, stx, table)
		if err != nil {
			return err
		}
		rows, err := stx.QueryContext(ctx, fmt.Sprintf(`select %s from "%s" where %s`, quoteCols(cols), table, where), args...)
		if err != nil {
			return err
		}
		vals, err := scanRows(rows, len(cols))
		if err != nil {
			return err
		}
		upd, pkIdx := -1, make([]int, 0, len(pk))
		var sets, pkCond []string
		for i, c := range cols {
			if c == "updated_at" {
				upd = i
			}
			isPK := false
			for _, p := range pk {
				isPK = isPK || p == c
			}
			if isPK {
				pkIdx = append(pkIdx, i)
				pkCond = append(pkCond, fmt.Sprintf(`"%s" = ?`, c))
				continue
			}
			if table == "messages" && c == "deleted_at" {
				sets = append(sets, `"deleted_at"=coalesce(excluded."deleted_at", "messages"."deleted_at")`)
				continue
			}
			sets = append(sets, fmt.Sprintf(`"%s"=excluded."%s"`, c, c))
		}
		if upd < 0 {
			return fmt.Errorf("reconcile %s: no updated_at column", table)
		}
		stmt := fmt.Sprintf(`insert into "%s"(%s) values(%s) on conflict(%s) do update set %s`,
			table, quoteCols(cols), placeholders(len(cols)), quoteCols(pk), strings.Join(sets, ","))
		cur := fmt.Sprintf(`select "updated_at" from "%s" where %s`, table, strings.Join(pkCond, " and "))
		for _, v := range vals {
			if tsFilter && !atOrAfter(v[upd], since) {
				continue
			}
			rep.Candidates[table]++
			keyArgs := make([]any, len(pkIdx))
			for i, k := range pkIdx {
				keyArgs[i] = v[k]
			}
			var existing any
			switch err := atxReal.QueryRowContext(ctx, cur, keyArgs...).Scan(&existing); {
			case errors.Is(err, sql.ErrNoRows):
			case err != nil:
				return fmt.Errorf("reconcile %s: %w", table, err)
			default:
				if !strictlyNewer(v[upd], existing) {
					if !opts.DryRun {
						rep.KeptNewer[table]++
					}
					continue
				}
			}
			if _, err := atx.ExecContext(ctx, stmt, v...); err != nil {
				return fmt.Errorf("reconcile %s: %w", table, err)
			}
			if !opts.DryRun {
				rep.Applied[table]++
			}
		}
		return nil
	}
	for _, t := range reconcilePK {
		if err := upsertNewest(t.table, t.pk, true, `1`); err != nil {
			return nil, err
		}
	}
	appliedMsgs := map[string]bool{}
	for _, id := range ids {
		before := rep.Applied["messages"]
		if err := upsertNewest("messages", []string{"id"}, false, `id = ?`, id); err != nil {
			return nil, err
		}
		if rep.Applied["messages"] > before {
			appliedMsgs[id] = true
		}
		if err := upsertNewest("message_attachments", []string{"attachment_id"}, false, `message_id = ?`, id); err != nil {
			return nil, err
		}
		// edit history: append-only by (message_id, edited_at)
		cols, err := tableColumns(ctx, stx, "message_edits")
		if err != nil {
			return nil, err
		}
		rows, err := stx.QueryContext(ctx, fmt.Sprintf(`select %s from message_edits where message_id = ?`, quoteCols(cols)), id)
		if err != nil {
			return nil, err
		}
		vals, err := scanRows(rows, len(cols))
		if err != nil {
			return nil, err
		}
		for _, v := range vals {
			rep.Candidates["message_edits"]++
			res, err := atx.ExecContext(ctx, fmt.Sprintf(`insert or ignore into message_edits(%s) values(%s)`, quoteCols(cols), placeholders(len(cols))), v...)
			if err != nil {
				return nil, err
			}
			if n, _ := res.RowsAffected(); n > 0 {
				rep.Applied["message_edits"]++
			}
		}
		for _, t := range reconcileEvents {
			cols, err := tableColumns(ctx, stx, t)
			if err != nil {
				return nil, err
			}
			var data []string
			for _, c := range cols {
				if c != "event_id" {
					data = append(data, c)
				}
			}
			rows, err := stx.QueryContext(ctx, fmt.Sprintf(`select %s from "%s" where message_id = ?`, quoteCols(data), t), id)
			if err != nil {
				return nil, err
			}
			vals, err := scanRows(rows, len(data))
			if err != nil {
				return nil, err
			}
			evAt := -1
			for i, c := range data {
				if c == "event_at" {
					evAt = i
				}
			}
			var cond []string
			for _, c := range data {
				cond = append(cond, fmt.Sprintf(`"%s" is ?`, c))
			}
			for _, v := range vals {
				if evAt >= 0 && !atOrAfter(v[evAt], since) {
					continue
				}
				rep.Candidates[t]++
				res, err := atx.ExecContext(ctx, fmt.Sprintf(`insert into "%s"(%s) select %s where not exists (select 1 from "%s" where %s)`,
					t, quoteCols(data), placeholders(len(data)), t, strings.Join(cond, " and ")), append(append([]any{}, v...), v...)...)
				if err != nil {
					return nil, err
				}
				if n, _ := res.RowsAffected(); n > 0 {
					rep.Applied[t]++
				}
			}
		}
	}
	// Legacy FTS (normalized content) for every message actually applied.
	for id := range appliedMsgs {
		rowID, ok := store.MessageFTSRowID(id)
		if !ok {
			continue
		}
		if _, err := atx.ExecContext(ctx, `delete from message_fts where rowid = ?`, rowID); err != nil {
			return nil, err
		}
		if _, err := atx.ExecContext(ctx, `insert into message_fts(rowid, message_id, guild_id, channel_id, author_id, author_name, channel_name, content)
			select ?, m.id, m.guild_id, m.channel_id, m.author_id,
				coalesce(json_extract(m.raw_json,'$.member.nick'), json_extract(m.raw_json,'$.author.global_name'), json_extract(m.raw_json,'$.author.username'), ''),
				coalesce(c.name,''), m.normalized_content
			from messages m left join channels c on c.id = m.channel_id where m.id = ?`, rowID, id); err != nil {
			return nil, err
		}
	}
	// Cursors/state advanced by the selective collector (never memory:*).
	if err := upsertNewest("sync_state", []string{"scope"}, true, `scope not like 'memory:%'`); err != nil {
		return nil, err
	}
	if opts.DryRun {
		return rep, nil
	}
	if err := atxReal.Commit(); err != nil {
		return nil, err
	}
	return rep, nil
}

type archiveWriter struct {
	tx  *sql.Tx
	dry bool
}

type dryResult struct{}

func (dryResult) LastInsertId() (int64, error) { return 0, nil }
func (dryResult) RowsAffected() (int64, error) { return -1, nil }

func (w archiveWriter) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	if w.dry {
		return dryResult{}, nil
	}
	return w.tx.ExecContext(ctx, q, args...)
}
