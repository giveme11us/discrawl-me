package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/steipete/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

type queryEmbeddingProvider struct {
	calls int
}

func (p *queryEmbeddingProvider) Name() string { return "test" }
func (p *queryEmbeddingProvider) Dim() int     { return 3 }
func (p *queryEmbeddingProvider) Embed(_ context.Context, _ []string) ([][]float32, error) {
	p.calls++
	return [][]float32{{1, 0, 0}}, nil
}

func setupTestStore(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()
	s, err := store.Open(ctx, t.TempDir()+"/test.db")
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Test Guild", RawJSON: "{}"}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: "{}"}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID: "m1", GuildID: "g1", ChannelID: "c1", ChannelName: "general",
		AuthorID: "u1", AuthorName: "User", MessageType: 0,
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Content:   "hello world test message", NormalizedContent: "hello world test message",
		RawJSON: "{}",
	}))
	return s
}

func sendRequest(t *testing.T, server *Server, method string, params any) jsonRPCResponse {
	t.Helper()
	id := json.RawMessage(`1`)
	paramsJSON, _ := json.Marshal(params)
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  paramsJSON,
	}
	reqLine, _ := json.Marshal(req)

	stdin := bytes.NewReader(append(reqLine, '\n'))
	var stdout bytes.Buffer

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = server.Run(ctx, stdin, &stdout)

	var resp jsonRPCResponse
	for _, line := range strings.Split(stdout.String(), "\n") {
		if line == "" {
			continue
		}
		_ = json.Unmarshal([]byte(line), &resp)
	}
	return resp
}

func TestMCPInitialize(t *testing.T) {
	s := setupTestStore(t)
	tools := NewToolHandler(s)
	server := NewServer(tools, nil)

	resp := sendRequest(t, server, "initialize", map[string]any{})
	require.Nil(t, resp.Error)

	result, _ := json.Marshal(resp.Result)
	require.Contains(t, string(result), "discrawl-me")
	require.Contains(t, string(result), "protocolVersion")
}

func TestMCPToolsList(t *testing.T) {
	s := setupTestStore(t)
	tools := NewToolHandler(s)
	server := NewServer(tools, nil)

	resp := sendRequest(t, server, "tools/list", map[string]any{})
	require.Nil(t, resp.Error)

	result, _ := json.Marshal(resp.Result)
	require.Contains(t, string(result), "search_messages")
	require.Contains(t, string(result), "list_guilds")
	require.Contains(t, string(result), "run_sql")
}

func TestMCPSearchMessages(t *testing.T) {
	s := setupTestStore(t)
	tools := NewToolHandler(s)
	server := NewServer(tools, nil)

	resp := sendRequest(t, server, "tools/call", map[string]any{
		"name":      "search_messages",
		"arguments": map[string]any{"query": "hello"},
	})
	require.Nil(t, resp.Error)

	result, _ := json.Marshal(resp.Result)
	require.Contains(t, string(result), "hello world")
}

func TestMCPHybridSearchEmbedsQuery(t *testing.T) {
	s := setupTestStore(t)
	provider := &queryEmbeddingProvider{}
	tools := NewToolHandler(s, provider)
	server := NewServer(tools, nil)

	resp := sendRequest(t, server, "tools/call", map[string]any{
		"name":      "search_messages",
		"arguments": map[string]any{"query": "hello", "mode": "hybrid"},
	})
	require.Nil(t, resp.Error)
	require.Equal(t, 1, provider.calls)
	result, _ := json.Marshal(resp.Result)
	require.Contains(t, string(result), "hello world")
}

func TestMCPVectorSearchRequiresProvider(t *testing.T) {
	s := setupTestStore(t)
	server := NewServer(NewToolHandler(s), nil)
	resp := sendRequest(t, server, "tools/call", map[string]any{
		"name":      "search_messages",
		"arguments": map[string]any{"query": "hello", "mode": "vector"},
	})
	require.Nil(t, resp.Error)
	result, _ := json.Marshal(resp.Result)
	require.Contains(t, string(result), "isError")
}

func TestMCPListGuilds(t *testing.T) {
	s := setupTestStore(t)
	tools := NewToolHandler(s)
	server := NewServer(tools, nil)

	resp := sendRequest(t, server, "tools/call", map[string]any{
		"name":      "list_guilds",
		"arguments": map[string]any{},
	})
	require.Nil(t, resp.Error)

	result, _ := json.Marshal(resp.Result)
	require.Contains(t, string(result), "Test Guild")
}

func TestMCPRunSQL(t *testing.T) {
	s := setupTestStore(t)
	tools := NewToolHandler(s)
	server := NewServer(tools, nil)

	resp := sendRequest(t, server, "tools/call", map[string]any{
		"name":      "run_sql",
		"arguments": map[string]any{"query": "select count(*) as cnt from messages"},
	})
	require.Nil(t, resp.Error)

	result, _ := json.Marshal(resp.Result)
	require.Contains(t, string(result), "1")
}

func TestMCPRunSQLRejectsWrite(t *testing.T) {
	s := setupTestStore(t)
	tools := NewToolHandler(s)
	server := NewServer(tools, nil)

	resp := sendRequest(t, server, "tools/call", map[string]any{
		"name":      "run_sql",
		"arguments": map[string]any{"query": "delete from messages"},
	})
	require.Nil(t, resp.Error)

	result, _ := json.Marshal(resp.Result)
	require.Contains(t, string(result), "isError")
}
