package userclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steipete/discrawl/internal/config"
	"github.com/steipete/discrawl/internal/discord"
	"github.com/stretchr/testify/require"
)

func testConfig() config.UserConfig {
	return config.UserConfig{
		ClientBuildNumber: 350000,
		BrowserVersion:    "131.0.0.0",
		UserAgent:         "TestAgent/1.0",
		Locale:            "en-US",
		MinRequestGapMs:   1, // fast for tests
		JitterMsMin:       0,
		JitterMsMax:       1,
		ReadOnlyStrict:    boolPtr(true),
	}
}

func boolPtr(b bool) *bool { return &b }

// testClient creates a UserClient pointed at a test server.
func testClient(t *testing.T, handler http.Handler) *UserClient {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	cfg := testConfig()
	client, err := New("test-token", cfg)
	require.NoError(t, err)

	// Patch the transport to use the test server
	client.transport.cfg.token = "test-token"
	// Override the API base by wrapping the transport
	origDo := client.transport.client
	client.transport.client = &http.Client{
		Transport: &rewriteTransport{
			base:      server.URL,
			transport: origDo.Transport,
		},
	}
	return client
}

// rewriteTransport redirects requests to the test server.
type rewriteTransport struct {
	base      string
	transport http.RoundTripper
}

func (t *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Rewrite the URL to point to the test server
	req.URL.Scheme = "http"
	req.URL.Host = strings.TrimPrefix(t.base, "http://")
	if t.transport != nil {
		return t.transport.RoundTrip(req)
	}
	return http.DefaultTransport.RoundTrip(req)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func TestUserClientSelf(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/users/@me", func(w http.ResponseWriter, r *http.Request) {
		// Verify user-mode headers
		require.Equal(t, "test-token", r.Header.Get("Authorization"))
		require.NotEmpty(t, r.Header.Get("X-Super-Properties"))
		require.Equal(t, "en-US", r.Header.Get("X-Discord-Locale"))
		writeJSON(w, map[string]any{"id": "u123", "username": "testuser"})
	})
	client := testClient(t, mux)
	user, err := client.Self(context.Background())
	require.NoError(t, err)
	require.Equal(t, "u123", user.ID)
	require.Equal(t, "testuser", user.Username)
}

func TestUserClientGuilds(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/users/@me/guilds", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, []map[string]any{
			{"id": "g1", "name": "Guild One"},
			{"id": "g2", "name": "Guild Two"},
		})
	})
	client := testClient(t, mux)
	guilds, err := client.Guilds(context.Background())
	require.NoError(t, err)
	require.Len(t, guilds, 2)
}

func TestUserClientGuildChannels(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/guilds/g1/channels", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, []map[string]any{
			{"id": "c1", "guild_id": "g1", "name": "general", "type": 0},
		})
	})
	client := testClient(t, mux)
	channels, err := client.GuildChannels(context.Background(), "g1")
	require.NoError(t, err)
	require.Len(t, channels, 1)
	require.Equal(t, "general", channels[0].Name)
}

func TestUserClientChannelMessages(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/channels/c1/messages", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "100", r.URL.Query().Get("limit"))
		writeJSON(w, []map[string]any{
			{
				"id":         "m1",
				"channel_id": "c1",
				"content":    "hello",
				"timestamp":  time.Now().UTC().Format(time.RFC3339),
				"author":     map[string]any{"id": "u1", "username": "user"},
			},
		})
	})
	client := testClient(t, mux)
	msgs, err := client.ChannelMessages(context.Background(), "c1", 100, "", "")
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "hello", msgs[0].Content)
}

func TestUserClientPrivateChannels(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/users/@me/channels", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, []map[string]any{
			{"id": "dm1", "type": 1, "recipients": []map[string]any{{"id": "u2", "username": "friend"}}},
		})
	})
	client := testClient(t, mux)
	dms, err := client.PrivateChannels(context.Background())
	require.NoError(t, err)
	require.Len(t, dms, 1)
}

func TestUserClientReadOnlyGuard(t *testing.T) {
	cfg := testConfig()
	cfg.ReadOnlyStrict = boolPtr(true)
	client, err := New("token", cfg)
	require.NoError(t, err)

	// POST should be blocked
	_, err = client.transport.do(context.Background(), "POST", "/test", strings.NewReader("{}"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "read-only mode")
}

func TestUserClientReadOnlyDisabled(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/test", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"ok": true})
	})

	cfg := testConfig()
	cfg.ReadOnlyStrict = boolPtr(false)
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := New("token", cfg)
	require.NoError(t, err)
	client.transport.client = &http.Client{
		Transport: &rewriteTransport{base: server.URL},
	}

	resp, err := client.transport.do(context.Background(), "POST", "/test", strings.NewReader("{}"))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_ = resp.Body.Close()
}

func TestUserClient429Retry(t *testing.T) {
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/users/@me", func(w http.ResponseWriter, _ *http.Request) {
		n := calls.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]any{"retry_after": 0.01})
			return
		}
		writeJSON(w, map[string]any{"id": "u1", "username": "user"})
	})
	client := testClient(t, mux)
	user, err := client.Self(context.Background())
	require.NoError(t, err)
	require.Equal(t, "u1", user.ID)
	require.Equal(t, int32(2), calls.Load())
}

func TestUserClientAuthError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v10/users/@me", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	client := testClient(t, mux)
	_, err := client.Self(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "auth error 401")
}

func TestSuperPropertiesEncoding(t *testing.T) {
	encoded := buildSuperProperties("TestUA/1.0", "131.0.0.0", "en-US", 350000)
	require.NotEmpty(t, encoded)
	// Should be valid base64
	require.NotContains(t, encoded, " ")
}

func TestUserClientTailNotImplemented(t *testing.T) {
	cfg := testConfig()
	client, err := New("token", cfg)
	require.NoError(t, err)
	err = client.Tail(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not yet implemented")
}

// Compile-time check.
var _ discord.Client = (*UserClient)(nil)
