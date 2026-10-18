package userclient

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// File name sorts AFTER proxy_test.go on purpose: TestNewProxyConfigEmptyMeansEnvironment
// relies on net/http caching HTTP_PROXY on first use (pre-existing order dependency).
//
// R1-03: one PUBLIC archive page per call, cursor = oldest archive timestamp;
// the private archive endpoint is never hit (local fake server only).
func TestUserClientThreadsArchivedPage(t *testing.T) {
	var queries []string
	privateHits := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/channels/p1/threads/archived/public", func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.RawQuery)
		if r.URL.Query().Get("before") == "" {
			writeJSON(w, map[string]any{"has_more": true, "threads": []map[string]any{
				{"id": "t2", "parent_id": "p1", "type": 11, "thread_metadata": map[string]any{"archived": true, "archive_timestamp": "2026-10-02T00:00:00Z"}},
				{"id": "t1", "parent_id": "p1", "type": 11, "thread_metadata": map[string]any{"archived": true, "archive_timestamp": "2026-10-01T00:00:00Z"}},
			}})
			return
		}
		writeJSON(w, map[string]any{"has_more": false, "threads": []map[string]any{
			{"id": "t0", "parent_id": "p1", "type": 11, "thread_metadata": map[string]any{"archived": true, "archive_timestamp": "2026-09-01T00:00:00Z"}},
		}})
	})
	mux.HandleFunc("/api/v10/channels/p1/threads/archived/private", func(http.ResponseWriter, *http.Request) { privateHits++ })
	client := testClient(t, mux)
	threads, more, next, err := client.ThreadsArchivedPage(context.Background(), "p1", "", 100)
	require.NoError(t, err)
	require.Len(t, threads, 2)
	require.True(t, more)
	require.Equal(t, "2026-10-01T00:00:00Z", next)
	threads, more, next, err = client.ThreadsArchivedPage(context.Background(), "p1", next, 100)
	require.NoError(t, err)
	require.Len(t, threads, 1)
	require.False(t, more)
	require.Empty(t, next)
	require.Equal(t, []string{"limit=100", "limit=100&before=2026-10-01T00:00:00Z"}, queries)
	require.Zero(t, privateHits)
}
