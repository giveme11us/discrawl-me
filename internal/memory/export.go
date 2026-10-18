package memory

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/giveme11us/discrawl-me/internal/store"
)

// Tables copied in full (catalog, members, every cursor/checkpoint).
var fullTables = []string{"guilds", "channels", "members", "sync_state"}

// Tables copied only for retained messages.
var messageChildTables = []string{"message_attachments", "message_edits", "message_events", "reaction_events", "mention_events"}

// Embedding tables copied only for retained messages (vectors and their job
// queue rows, byte-identical). Prune deletes them with the expired message.
var embeddingTables = []string{"message_embeddings", "embedding_jobs"}

// Tables deliberately NOT copied as rows: FTS tables (rebuilt for retained
// rows; member_fts copied through its own path).
var skippedTables = map[string]bool{"message_fts": true, "member_fts": true}

// ExportOptions configures memory export.
type ExportOptions struct {
	Source     string
	Output     string
	PolicyPath string
	Now        time.Time
	HashSource bool
	// NoRollbackWindow writes the marker with the rollback window already
	// closed at the export instant: retention expels every expired message
	// from the first pass after cutover (owner's final policy). The guard
	// code stays intact; reconcile then refuses boundaries covering a loss.
	NoRollbackWindow bool
	// EnqueueMissingEmbeddings inserts a pending embedding_jobs row for every
	// retained message that has neither an embedding nor a job, so the normal
	// `embed run` worker embeds the in-window backlog. Output only.
	EnqueueMissingEmbeddings bool
	// failAfter is a test failpoint: return an error after copying this table.
	failAfter string
}

// FileStats are measured, never estimated.
type FileStats struct {
	Bytes         int64 `json:"bytes"`
	WALBytes      int64 `json:"wal_bytes"`
	PageSize      int64 `json:"page_size"`
	PageCount     int64 `json:"page_count"`
	FreelistCount int64 `json:"freelist_count"`
}

// ExportReport contains only counts, sizes, coverage and hashes.
type ExportReport struct {
	Source          string            `json:"source"`
	Output          string            `json:"output"`
	PolicySHA256    string            `json:"policy_sha256"`
	RegistrySHA256  string            `json:"registry_sha256"`
	StateSHA256     string            `json:"state_sha256"`
	Now             string            `json:"now_utc"`
	Cutoff          string            `json:"cutoff_utc"`
	SourceSchemaSHA string            `json:"source_schema_sha256"`
	SourceFileSHA   string            `json:"source_file_sha256,omitempty"`
	SourceRows      map[string]int64  `json:"source_rows"`
	OutputRows      map[string]int64  `json:"output_rows"`
	Reasons         map[string]int64  `json:"retention_reasons"`
	OutputDigests   map[string]string `json:"output_table_digests"`
	ProcessedKept   int               `json:"processed_ids_retained"`
	ProcessedTotal  int               `json:"processed_ids_registered"`
	RollbackWindow  string            `json:"rollback_window"`
	EnqueuedJobs    int64             `json:"embedding_jobs_enqueued"`
	OutOfPolicy     map[string]int64  `json:"messages_dropped_out_of_policy_by_guild,omitempty"`
	SourceStats     FileStats         `json:"source_file"`
	OutputStats     FileStats         `json:"output_file"`
}

// Marker is stored in sync_state[MarkerScope] of a selective DB.
type Marker struct {
	Version      int    `json:"version"`
	PolicySHA256 string `json:"policy_sha256"`
	CreatedAt    string `json:"created_at"`
	Cutoff       string `json:"cutoff_utc"`
	LastPruneAt  string `json:"last_prune_at,omitempty"`
	// Rollback window (see rollback_window.go): "open" from export until an
	// explicit `memory close-rollback-window`; never closed implicitly.
	RollbackWindow   string `json:"rollback_window,omitempty"`
	RollbackClosedAt string `json:"rollback_window_closed_at,omitempty"`
}

func openSourceRO(ctx context.Context, path string) (*sql.DB, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, errors.New("source: must be a regular file, not a symlink")
	}
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(30000)", path))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("source: %w", err)
	}
	return db, nil
}

func checkOutputPath(source, output string) error {
	if output == "" || source == "" {
		return errors.New("--source and --output are required")
	}
	srcAbs, err := filepath.Abs(source)
	if err != nil {
		return err
	}
	outAbs, err := filepath.Abs(output)
	if err != nil {
		return err
	}
	if srcAbs == outAbs {
		return errors.New("output must differ from source")
	}
	if _, err := os.Lstat(output); err == nil {
		return errors.New("output already exists (no overwrite)")
	} else if !os.IsNotExist(err) {
		return err
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(output + suffix); err == nil {
			return fmt.Errorf("output sidecar %s exists", output+suffix)
		}
	}
	dir := filepath.Dir(outAbs)
	dinfo, err := os.Stat(dir)
	if err != nil || !dinfo.IsDir() {
		return fmt.Errorf("output directory %s does not exist", dir)
	}
	if sinfo, err := os.Stat(source); err == nil {
		if real, err := filepath.EvalSymlinks(dir); err == nil {
			if real == filepath.Dir(mustEval(srcAbs)) && filepath.Base(outAbs) == filepath.Base(srcAbs) {
				return errors.New("output resolves to source")
			}
		}
		_ = sinfo
	}
	return nil
}

func mustEval(p string) string {
	if r, err := filepath.EvalSymlinks(p); err == nil {
		return r
	}
	return p
}

func schemaSHA(ctx context.Context, q queryer) (string, int, error) {
	rows, err := q.QueryContext(ctx, `select type, name, coalesce(sql,'') from sqlite_master order by type, name`)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = rows.Close() }()
	h := sha256.New()
	for rows.Next() {
		var a, b, c string
		if err := rows.Scan(&a, &b, &c); err != nil {
			return "", 0, err
		}
		fmt.Fprintf(h, "%s\x00%s\x00%s\n", a, b, c)
	}
	if err := rows.Err(); err != nil {
		return "", 0, err
	}
	var version int
	if r2, err := q.QueryContext(ctx, `pragma user_version`); err == nil {
		if r2.Next() {
			_ = r2.Scan(&version)
		}
		_ = r2.Close()
	}
	return hex.EncodeToString(h.Sum(nil)), version, nil
}

func tableNames(ctx context.Context, q queryer) ([]string, error) {
	rows, err := q.QueryContext(ctx, `select name from sqlite_master where type='table' and name not like 'sqlite_%' and name not like '%fts_%' order by name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func columns(ctx context.Context, q queryer, table string) ([]string, error) {
	rows, err := q.QueryContext(ctx, fmt.Sprintf(`select name from pragma_table_info('%s') order by cid`, table))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func countRows(ctx context.Context, q queryer, table string) (int64, error) {
	rows, err := q.QueryContext(ctx, fmt.Sprintf(`select count(*) from "%s"`, table))
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	var n int64
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return 0, err
		}
	}
	return n, rows.Err()
}

// copyTable streams rows from src to dst; keep may filter by the row map.
func copyTable(ctx context.Context, src queryer, dst *sql.Tx, table string, keep func(map[string]any) (bool, error)) (int64, error) {
	cols, err := columns(ctx, src, table)
	if err != nil {
		return 0, err
	}
	quoted := make([]string, len(cols))
	marks := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = `"` + c + `"`
		marks[i] = "?"
	}
	rows, err := src.QueryContext(ctx, fmt.Sprintf(`select %s from "%s" order by rowid`, strings.Join(quoted, ","), table))
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	stmt, err := dst.PrepareContext(ctx, fmt.Sprintf(`insert or replace into "%s"(%s) values(%s)`, table, strings.Join(quoted, ","), strings.Join(marks, ",")))
	if err != nil {
		return 0, fmt.Errorf("prepare %s: %w", table, err)
	}
	defer func() { _ = stmt.Close() }()
	var n int64
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return n, err
		}
		if keep != nil {
			row := make(map[string]any, len(cols))
			for i, c := range cols {
				row[c] = vals[i]
			}
			ok, err := keep(row)
			if err != nil {
				return n, err
			}
			if !ok {
				continue
			}
		}
		if _, err := stmt.ExecContext(ctx, vals...); err != nil {
			return n, fmt.Errorf("insert %s: %w", table, err)
		}
		n++
	}
	return n, rows.Err()
}

func asString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

// TableDigest hashes every row of a table ordered by all columns.
func TableDigest(ctx context.Context, q queryer, table string) (string, error) {
	cols, err := columns(ctx, q, table)
	if err != nil {
		return "", err
	}
	order := make([]string, len(cols))
	quoted := make([]string, len(cols))
	for i, c := range cols {
		order[i] = fmt.Sprint(i + 1)
		quoted[i] = `"` + c + `"`
	}
	rows, err := q.QueryContext(ctx, fmt.Sprintf(`select %s from "%s" order by %s`, strings.Join(quoted, ","), table, strings.Join(order, ",")))
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()
	h := sha256.New()
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}
		for _, v := range vals {
			fmt.Fprintf(h, "%T:%s\x1f", v, asString(v))
		}
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil)), rows.Err()
}

// MeasureFile reads real size/page/freelist/WAL figures.
func MeasureFile(ctx context.Context, q queryer, path string) FileStats {
	var fs FileStats
	if info, err := os.Stat(path); err == nil {
		fs.Bytes = info.Size()
	}
	if info, err := os.Stat(path + "-wal"); err == nil {
		fs.WALBytes = info.Size()
	}
	for _, p := range []struct {
		name string
		dst  *int64
	}{{"page_size", &fs.PageSize}, {"page_count", &fs.PageCount}, {"freelist_count", &fs.FreelistCount}} {
		if rows, err := q.QueryContext(ctx, "pragma "+p.name); err == nil {
			if rows.Next() {
				_ = rows.Scan(p.dst)
			}
			_ = rows.Close()
		}
	}
	return fs
}

func fileSHA(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	buf := make([]byte, 1<<20)
	for {
		n, err := f.Read(buf)
		h.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func removeDBFiles(path string) {
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		_ = os.Remove(path + suffix)
	}
}

// Export builds a NEW compact selective DB from a read-only source snapshot.
// On any error no file exists at Output.
func Export(ctx context.Context, opts ExportOptions) (rep *ExportReport, err error) {
	if err := checkOutputPath(opts.Source, opts.Output); err != nil {
		return nil, err
	}
	pol, err := LoadPolicy(opts.PolicyPath, opts.Now)
	if err != nil {
		return nil, err
	}
	src, err := openSourceRO(ctx, opts.Source)
	if err != nil {
		return nil, err
	}
	defer func() { _ = src.Close() }()
	rep = &ExportReport{
		Source: opts.Source, Output: opts.Output, PolicySHA256: pol.PolicySHA256, RegistrySHA256: pol.RegistrySHA, StateSHA256: pol.StateSHA,
		Now: pol.Now.Format(time.RFC3339), Cutoff: pol.Cutoff.Format(time.RFC3339), SourceRows: map[string]int64{}, OutputRows: map[string]int64{},
		Reasons: map[string]int64{}, OutputDigests: map[string]string{}, ProcessedTotal: len(pol.ProcessedIDs), OutOfPolicy: map[string]int64{},
	}
	if opts.HashSource {
		if rep.SourceFileSHA, err = fileSHA(opts.Source); err != nil {
			return nil, err
		}
	}
	// One read transaction = one consistent snapshot of the source.
	stx, err := src.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stx.Rollback() }()
	schema, version, err := schemaSHA(ctx, stx)
	if err != nil {
		return nil, fmt.Errorf("source schema: %w", err)
	}
	rep.SourceSchemaSHA = schema
	if version != 3 {
		return nil, fmt.Errorf("source schema version %d unsupported (want 3)", version)
	}
	rep.SourceStats = MeasureFile(ctx, stx, opts.Source)
	tables, err := tableNames(ctx, stx)
	if err != nil {
		return nil, err
	}
	known := map[string]bool{"messages": true}
	for _, t := range fullTables {
		known[t] = true
	}
	for _, t := range messageChildTables {
		known[t] = true
	}
	for _, t := range embeddingTables {
		known[t] = true
	}
	for _, t := range tables {
		if !known[t] && !skippedTables[t] {
			return nil, fmt.Errorf("source has unknown table %q: refusing a lossy export", t)
		}
		n, err := countRows(ctx, stx, t)
		if err != nil {
			return nil, err
		}
		rep.SourceRows[t] = n
	}
	ret, err := LoadRetainer(ctx, stx, pol)
	if err != nil {
		return nil, err
	}
	if err := ret.ValidateCatalog(); err != nil {
		return nil, err
	}

	tmp := fmt.Sprintf("%s.partial-%d", opts.Output, os.Getpid())
	if _, err := os.Lstat(tmp); err == nil {
		return nil, fmt.Errorf("stale partial file %s exists", tmp)
	}
	defer func() {
		if err != nil {
			removeDBFiles(tmp)
		}
	}()
	dst, err := store.Open(ctx, tmp) // creates the exact same schema version
	if err != nil {
		return nil, err
	}
	ddb := dst.DB()
	closed := false
	defer func() {
		if !closed {
			_ = dst.Close()
		}
	}()
	dtx, err := ddb.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = dtx.Rollback() }()
	fail := func(table string) error {
		if opts.failAfter == table {
			return fmt.Errorf("failpoint after %s", table)
		}
		return nil
	}
	for _, t := range fullTables {
		n, err := copyTable(ctx, stx, dtx, t, nil)
		if err != nil {
			return nil, err
		}
		_ = n
		if err := fail(t); err != nil {
			return nil, err
		}
	}
	kept := map[string]bool{}
	if _, err := copyTable(ctx, stx, dtx, "messages", func(row map[string]any) (bool, error) {
		guild := asString(row["guild_id"])
		if !pol.Guilds[guild] {
			// Guild removed from the policy: its messages are not kept
			// (catalog and cursors are still copied, see fullTables).
			rep.Reasons[ReasonOutOfScope]++
			rep.OutOfPolicy[guild]++
			return false, nil
		}
		ok, reason := ret.Decide(guild, asString(row["channel_id"]), asString(row["id"]), asString(row["created_at"]))
		rep.Reasons[reason]++
		if ok {
			kept[asString(row["id"])] = true
		}
		return ok, nil
	}); err != nil {
		return nil, err
	}
	if err := fail("messages"); err != nil {
		return nil, err
	}
	for _, t := range append(append([]string{}, messageChildTables...), embeddingTables...) {
		if _, ok := rep.SourceRows[t]; !ok {
			continue
		}
		if _, err := copyTable(ctx, stx, dtx, t, func(row map[string]any) (bool, error) {
			return kept[asString(row["message_id"])], nil
		}); err != nil {
			return nil, err
		}
		if err := fail(t); err != nil {
			return nil, err
		}
	}
	for id := range pol.ProcessedIDs {
		if kept[id] {
			rep.ProcessedKept++
		}
	}
	// Fail closed: every registered processed ID must be present in the
	// source and retained, otherwise the selective memory would silently lose
	// a pin. No target is published (the partial is removed by the caller).
	if rep.ProcessedKept != rep.ProcessedTotal {
		return nil, fmt.Errorf("export refused: processed pin coverage incomplete (%d/%d registered processed IDs present in source)", rep.ProcessedKept, rep.ProcessedTotal)
	}
	// FTS for retained messages, including embed fields/snapshot text.
	if err := rebuildSelectiveFTS(ctx, dtx); err != nil {
		return nil, err
	}
	if _, err := copyMemberFTS(ctx, stx, dtx); err != nil {
		return nil, err
	}
	set, del := ret.InfoBookkeeping()
	if err := applyBookkeeping(ctx, dtx, set, del, pol.Now); err != nil {
		return nil, err
	}
	if opts.EnqueueMissingEmbeddings {
		res, err := dtx.ExecContext(ctx, `insert into embedding_jobs(message_id, state, attempts, updated_at)
			select m.id, 'pending', 0, ? from messages m
			where not exists (select 1 from message_embeddings e where e.message_id = m.id)
			and not exists (select 1 from embedding_jobs j where j.message_id = m.id)`, pol.Now.Format(time.RFC3339Nano))
		if err != nil {
			return nil, fmt.Errorf("enqueue missing embeddings: %w", err)
		}
		rep.EnqueuedJobs, _ = res.RowsAffected()
	}
	marker := Marker{Version: PolicyVersion, PolicySHA256: pol.PolicySHA256, CreatedAt: pol.Now.Format(time.RFC3339), Cutoff: pol.Cutoff.Format(time.RFC3339), RollbackWindow: WindowOpen}
	if opts.NoRollbackWindow {
		marker.RollbackWindow = WindowClosed
		marker.RollbackClosedAt = pol.Now.Format(time.RFC3339Nano)
	}
	rep.RollbackWindow = marker.RollbackWindow
	if err := writeMarker(ctx, dtx, marker, pol.Now); err != nil {
		return nil, err
	}
	if err := fail("commit"); err != nil {
		return nil, err
	}
	if err := dtx.Commit(); err != nil {
		return nil, err
	}
	if _, err := ddb.ExecContext(ctx, `vacuum`); err != nil {
		return nil, fmt.Errorf("vacuum output: %w", err)
	}
	if _, err := ddb.ExecContext(ctx, `pragma wal_checkpoint(TRUNCATE)`); err != nil {
		return nil, err
	}
	var integrity string
	if err := ddb.QueryRowContext(ctx, `pragma integrity_check`).Scan(&integrity); err != nil || integrity != "ok" {
		return nil, fmt.Errorf("output integrity_check failed: %v %s", err, integrity)
	}
	names, err := tableNames(ctx, ddb)
	if err != nil {
		return nil, err
	}
	for _, t := range append(names, "message_fts", "member_fts") {
		n, err := countRows(ctx, ddb, t)
		if err != nil {
			return nil, err
		}
		rep.OutputRows[t] = n
		if t == "message_fts" || t == "member_fts" {
			continue
		}
		if rep.OutputDigests[t], err = TableDigest(ctx, ddb, t); err != nil {
			return nil, err
		}
	}
	if err := fail("final"); err != nil {
		return nil, err
	}
	closed = true
	if err := dst.Close(); err != nil {
		return nil, err
	}
	// Atomic no-overwrite publish: link fails if Output appeared meanwhile.
	if err := os.Link(tmp, opts.Output); err != nil {
		return nil, fmt.Errorf("publish output: %w", err)
	}
	removeDBFiles(tmp)
	if out, oerr := store.OpenReadOnly(ctx, opts.Output); oerr == nil {
		rep.OutputStats = MeasureFile(ctx, out.DB(), opts.Output)
		_ = out.Close()
	}
	if opts.HashSource {
		after, err := fileSHA(opts.Source)
		if err != nil || after != rep.SourceFileSHA {
			return rep, fmt.Errorf("source file hash changed during export")
		}
	}
	return rep, nil
}

func rebuildSelectiveFTS(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `delete from message_fts`); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `
		select m.id, m.guild_id, m.channel_id, coalesce(m.author_id,''),
			coalesce(json_extract(m.raw_json,'$.member.nick'), json_extract(m.raw_json,'$.author.global_name'), json_extract(m.raw_json,'$.author.username'), ''),
			coalesce(c.name,''), m.normalized_content, m.raw_json
		from messages m left join channels c on c.id = m.channel_id order by cast(m.id as integer)`)
	if err != nil {
		return err
	}
	type ftsRow struct{ id, g, c, a, an, cn, text string }
	var all []ftsRow
	for rows.Next() {
		var r ftsRow
		var norm, raw string
		if err := rows.Scan(&r.id, &r.g, &r.c, &r.a, &r.an, &r.cn, &norm, &raw); err != nil {
			_ = rows.Close()
			return err
		}
		r.text = store.SelectiveFTSText(norm, raw)
		all = append(all, r)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, r := range all {
		rowID, ok := store.MessageFTSRowID(r.id)
		if !ok {
			continue
		}
		var author any = r.a
		if r.a == "" {
			author = nil
		}
		if _, err := tx.ExecContext(ctx, `insert into message_fts(rowid, message_id, guild_id, channel_id, author_id, author_name, channel_name, content) values(?,?,?,?,?,?,?,?)`,
			rowID, r.id, r.g, r.c, author, r.an, r.cn, r.text); err != nil {
			return err
		}
	}
	return nil
}

func copyMemberFTS(ctx context.Context, src queryer, tx *sql.Tx) (int64, error) {
	rows, err := src.QueryContext(ctx, `select rowid, member_key, guild_id, user_id, username, display_name, profile_text from member_fts`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	var n int64
	for rows.Next() {
		v := make([]any, 7)
		p := make([]any, 7)
		for i := range v {
			p[i] = &v[i]
		}
		if err := rows.Scan(p...); err != nil {
			return n, err
		}
		if _, err := tx.ExecContext(ctx, `insert into member_fts(rowid, member_key, guild_id, user_id, username, display_name, profile_text) values(?,?,?,?,?,?,?)`, v...); err != nil {
			return n, err
		}
		n++
	}
	return n, rows.Err()
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func applyBookkeeping(ctx context.Context, tx execer, set map[string]string, del []string, now time.Time) error {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if _, err := tx.ExecContext(ctx, `insert into sync_state(scope, cursor, updated_at) values(?,?,?) on conflict(scope) do update set cursor=excluded.cursor, updated_at=excluded.updated_at`, k, set[k], now.Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	for _, k := range del {
		if !strings.HasPrefix(k, "memory:") {
			return fmt.Errorf("refusing to delete non-memory scope %s", k)
		}
		if _, err := tx.ExecContext(ctx, `delete from sync_state where scope = ?`, k); err != nil {
			return err
		}
	}
	return nil
}

func writeMarker(ctx context.Context, tx execer, m Marker, now time.Time) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `insert into sync_state(scope, cursor, updated_at) values(?,?,?) on conflict(scope) do update set cursor=excluded.cursor, updated_at=excluded.updated_at`, MarkerScope, string(body), now.Format(time.RFC3339Nano))
	return err
}

// ReadMarker returns the selective-memory marker, or nil when absent.
func ReadMarker(ctx context.Context, q queryer) (*Marker, error) {
	rows, err := q.QueryContext(ctx, `select coalesce(cursor,'') from sync_state where scope = ?`, MarkerScope)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return nil, rows.Err()
	}
	var raw string
	if err := rows.Scan(&raw); err != nil {
		return nil, err
	}
	var m Marker
	if err := json.Unmarshal([]byte(raw), &m); err != nil || m.Version != PolicyVersion {
		return nil, fmt.Errorf("invalid selective memory marker")
	}
	return &m, nil
}
