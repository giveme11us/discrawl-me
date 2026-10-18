package cli

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/giveme11us/discrawl-me/internal/store"
)

func vecBlob(v ...float32) []byte {
	b := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

// Final policy: a selective DB keeps the embeddings of retained messages, so
// `memory similar` (stored vectors only, no provider) and `memory search
// --mode vector|hybrid` (explicit query embedder) answer from them, always
// with the selective notice.
func TestMemoryVectorSearchOnSelectiveDB(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "selective.db")
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	exec := func(q string, args ...any) {
		_, err := s.DB().Exec(q, args...)
		require.NoError(t, err)
	}
	exec(`insert into guilds(id, name, raw_json, updated_at) values('100000000000000001','G','{}',?)`, now)
	exec(`insert into channels(id, guild_id, parent_id, kind, name, raw_json, updated_at) values('100000000000000020','100000000000000001','','text','general','{}',?)`, now)
	vecs := map[string][]float32{
		"300000000000000001": {1, 0, 0},
		"300000000000000002": {0.9, 0.1, 0},
		"300000000000000003": {0, 0, 1},
	}
	for id, v := range vecs {
		exec(`insert into messages(id, guild_id, channel_id, author_id, message_type, created_at, content, normalized_content, raw_json, updated_at)
			values(?, '100000000000000001', '100000000000000020', 'u1', 0, ?, ?, ?, '{}', ?)`, id, now, "text "+id, "text "+id, now)
		exec(`insert into message_embeddings(message_id, model, dim, vec, created_at) values(?, 'm', 3, ?, ?)`, id, vecBlob(v...), now)
	}
	exec(`insert into sync_state(scope, cursor, updated_at) values('memory:selective', '{"version":1,"policy_sha256":"x","created_at":"2026-10-18T00:00:00Z","cutoff_utc":"x","rollback_window":"closed","rollback_window_closed_at":"2026-10-18T00:00:00Z"}', ?)`, now)
	_, _ = s.DB().Exec(`pragma wal_checkpoint(TRUNCATE)`)
	require.NoError(t, s.Close())

	run := func(args ...string) (string, error) {
		out := &bytes.Buffer{}
		rt := &runtime{ctx: ctx, stdout: out, stderr: &bytes.Buffer{}, logger: discardLogger()}
		err := rt.runMemory(args)
		return out.String(), err
	}
	out, err := run("similar", "--db", dbPath, "--message-id", "300000000000000001", "--limit", "1")
	require.NoError(t, err)
	require.Contains(t, out, "selective memory")
	require.Contains(t, out, "300000000000000002", "nearest neighbour from stored vectors")
	require.NotContains(t, out, "300000000000000003")

	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"index": 0, "embedding": []float64{0, 0.1, 0.9}}}})
	}))
	defer srv.Close()
	t.Setenv("TEST_EMBED_KEY", "dummy")
	out, err = run("search", "--db", dbPath, "--mode", "vector", "--limit", "1",
		"--embed-endpoint", srv.URL, "--embed-model", "m", "--embed-dim", "3", "--embed-api-key-env", "TEST_EMBED_KEY", "query")
	require.NoError(t, err)
	require.Equal(t, 1, calls, "exactly one query embedding")
	require.Contains(t, out, "selective memory")
	require.Contains(t, out, "300000000000000003")
	require.NotContains(t, out, "300000000000000001")
	_, err = run("search", "--db", dbPath, "--mode", "vector", "--embed-endpoint", srv.URL, "q")
	require.ErrorContains(t, err, "--embed-model")
}
