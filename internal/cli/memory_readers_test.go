package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/giveme11us/discrawl-me/internal/store"
)

// R1-06: offline CLI readers on an explicit selective DB: read-only, no
// config/token/client, notice on every output, no DB created.
func TestMemoryCLIReadersOffline(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "selective.db")
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = s.DB().Exec(`insert into channels(id, guild_id, parent_id, kind, name, raw_json, updated_at) values('100000000000000020','100000000000000001','','text','general','{}',?)`, now)
	require.NoError(t, err)
	_, err = s.DB().Exec(`insert into guilds(id, name, raw_json, updated_at) values('100000000000000001','Synthetic','{}',?)`, now)
	require.NoError(t, err)
	_, _ = s.DB().Exec(`pragma wal_checkpoint(TRUNCATE)`)
	require.NoError(t, s.Close())
	unmarked := filepath.Join(dir, "unmarked.db")
	copyTestFile(t, dbPath, unmarked)
	s, err = store.Open(ctx, dbPath)
	require.NoError(t, err)
	_, err = s.DB().Exec(`insert into sync_state(scope, cursor, updated_at) values('memory:selective', '{"version":1,"policy_sha256":"x","created_at":"x","cutoff_utc":"x"}', ?)`, now)
	require.NoError(t, err)
	_, _ = s.DB().Exec(`pragma wal_checkpoint(TRUNCATE)`)
	require.NoError(t, s.Close())

	run := func(args ...string) (string, error) {
		out := &bytes.Buffer{}
		rt := &runtime{ctx: ctx, stdout: out, stderr: &bytes.Buffer{}, logger: discardLogger()}
		err := rt.runMemory(args)
		return out.String(), err
	}
	out, err := run("sql", "--db", dbPath, "select count(*) as n from channels")
	require.NoError(t, err)
	require.Contains(t, out, "selective memory")
	require.Contains(t, out, "absence of a message here does NOT mean")
	out, err = run("channels", "--db", dbPath, "--guild", "100000000000000001")
	require.NoError(t, err)
	require.Contains(t, out, "general")
	out, err = run("guilds", "--db", dbPath)
	require.NoError(t, err)
	require.Contains(t, out, "Synthetic")
	out, err = run("search", "--db", dbPath, "nothinghere")
	require.NoError(t, err)
	require.Contains(t, out, "selective memory", "empty result still carries the notice")
	// Final policy: vector search is no longer refused on a selective DB; by
	// text it needs an explicit query embedder (none given here).
	_, err = run("search", "--db", dbPath, "--mode", "vector", "x")
	require.ErrorContains(t, err, "requires an enabled embedding provider")
	_, err = run("sql", "--db", dbPath, "update channels set name = 'x'")
	require.Error(t, err)
	_, err = run("sql", "--db", unmarked, "select 1")
	require.ErrorContains(t, err, "no selective memory marker")
	missing := filepath.Join(dir, "missing.db")
	_, err = run("search", "--db", missing, "x")
	require.Error(t, err)
	_, statErr := os.Stat(missing)
	require.True(t, os.IsNotExist(statErr), "reader must not create a DB")
	_, err = run("search", "x")
	require.ErrorContains(t, err, "--db")
}

func copyTestFile(t *testing.T, src, dst string) {
	t.Helper()
	b, err := os.ReadFile(src)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dst, b, 0o600))
}
