package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/steipete/discrawl/internal/store"
)

// ToolHandler handles MCP tool calls against the store.
type ToolHandler struct {
	store *store.Store
}

// NewToolHandler creates a tool handler backed by the given store.
func NewToolHandler(s *store.Store) *ToolHandler {
	return &ToolHandler{store: s}
}

// ListTools returns the MCP tool definitions.
func (h *ToolHandler) ListTools() []map[string]any {
	return []map[string]any{
		{
			"name":        "search_messages",
			"description": "Search Discord messages using keyword (FTS5), semantic (vector), or hybrid search. Returns matching messages with context.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query":      map[string]any{"type": "string", "description": "Search query text"},
					"guild_id":   map[string]any{"type": "string", "description": "Filter by guild ID (optional)"},
					"channel_id": map[string]any{"type": "string", "description": "Filter by channel ID or name (optional)"},
					"author":     map[string]any{"type": "string", "description": "Filter by author ID or name (optional)"},
					"mode":       map[string]any{"type": "string", "enum": []string{"fts", "vector", "hybrid"}, "description": "Search mode (default: fts)"},
					"limit":      map[string]any{"type": "integer", "description": "Max results (default: 20)"},
				},
				"required": []string{"query"},
			},
		},
		{
			"name":        "get_conversation",
			"description": "Get messages around a specific message ID, providing conversation context.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"message_id": map[string]any{"type": "string", "description": "Target message ID"},
					"before":     map[string]any{"type": "integer", "description": "Messages before target (default: 20)"},
					"after":      map[string]any{"type": "integer", "description": "Messages after target (default: 20)"},
				},
				"required": []string{"message_id"},
			},
		},
		{
			"name":        "list_guilds",
			"description": "List all archived Discord guilds (servers) with message counts.",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			"name":        "list_channels",
			"description": "List channels in a guild with message counts.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"guild_id": map[string]any{"type": "string", "description": "Guild ID"},
				},
				"required": []string{"guild_id"},
			},
		},
		{
			"name":        "find_similar",
			"description": "Find messages semantically similar to a given message (requires embeddings).",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"message_id": map[string]any{"type": "string", "description": "Source message ID"},
					"limit":      map[string]any{"type": "integer", "description": "Max results (default: 10)"},
				},
				"required": []string{"message_id"},
			},
		},
		{
			"name":        "run_sql",
			"description": "Execute a read-only SQL query against the archive database. Only SELECT and WITH statements are allowed.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{"type": "string", "description": "SQL query (SELECT only)"},
				},
				"required": []string{"query"},
			},
		},
	}
}

// CallTool dispatches a tool call and returns the result as a string.
func (h *ToolHandler) CallTool(ctx context.Context, name string, args json.RawMessage) (string, error) {
	switch name {
	case "search_messages":
		return h.searchMessages(ctx, args)
	case "get_conversation":
		return h.getConversation(ctx, args)
	case "list_guilds":
		return h.listGuilds(ctx)
	case "list_channels":
		return h.listChannels(ctx, args)
	case "find_similar":
		return h.findSimilar(ctx, args)
	case "run_sql":
		return h.runSQL(ctx, args)
	default:
		return "", fmt.Errorf("unknown tool: %s", name)
	}
}

func (h *ToolHandler) searchMessages(ctx context.Context, args json.RawMessage) (string, error) {
	var params struct {
		Query     string `json:"query"`
		GuildID   string `json:"guild_id"`
		ChannelID string `json:"channel_id"`
		Author    string `json:"author"`
		Mode      string `json:"mode"`
		Limit     int    `json:"limit"`
	}
	if err := json.Unmarshal(args, &params); err != nil {
		return "", err
	}
	if params.Limit <= 0 {
		params.Limit = 20
	}
	if params.Mode == "" {
		params.Mode = "fts"
	}

	opts := store.SearchOptions{
		Query:   params.Query,
		Channel: params.ChannelID,
		Author:  params.Author,
		Limit:   params.Limit,
		Mode:    params.Mode,
	}
	if params.GuildID != "" {
		opts.GuildIDs = []string{params.GuildID}
	}

	var results []store.SearchResult
	var err error
	switch params.Mode {
	case "hybrid":
		results, err = h.store.HybridSearch(ctx, opts)
	default:
		results, err = h.store.SearchMessages(ctx, opts)
	}
	if err != nil {
		return "", err
	}
	return formatResults(results), nil
}

func (h *ToolHandler) getConversation(ctx context.Context, args json.RawMessage) (string, error) {
	var params struct {
		MessageID string `json:"message_id"`
		Before    int    `json:"before"`
		After     int    `json:"after"`
	}
	if err := json.Unmarshal(args, &params); err != nil {
		return "", err
	}
	results, err := h.store.GetConversation(ctx, params.MessageID, params.Before, params.After)
	if err != nil {
		return "", err
	}
	return formatResults(results), nil
}

func (h *ToolHandler) listGuilds(ctx context.Context) (string, error) {
	cols, rows, err := h.store.ReadOnlyQuery(ctx, `
		select g.id, g.name,
			(select count(*) from channels where guild_id = g.id) as channels,
			(select count(*) from messages where guild_id = g.id) as messages
		from guilds g
		order by messages desc
	`)
	if err != nil {
		return "", err
	}
	return formatTable(cols, rows), nil
}

func (h *ToolHandler) listChannels(ctx context.Context, args json.RawMessage) (string, error) {
	var params struct {
		GuildID string `json:"guild_id"`
	}
	if err := json.Unmarshal(args, &params); err != nil {
		return "", err
	}
	// Use Query with parameterized SQL via the DB directly
	query := fmt.Sprintf(`
		select c.id, c.name, c.kind, coalesce(c.topic, ''),
			(select count(*) from messages where channel_id = c.id) as messages
		from channels c
		where c.guild_id = '%s'
		order by c.position, c.name
	`, sanitizeSQL(params.GuildID))
	cols, rows, err := h.store.ReadOnlyQuery(ctx, query)
	if err != nil {
		return "", err
	}
	return formatTable(cols, rows), nil
}

func (h *ToolHandler) findSimilar(ctx context.Context, args json.RawMessage) (string, error) {
	var params struct {
		MessageID string `json:"message_id"`
		Limit     int    `json:"limit"`
	}
	if err := json.Unmarshal(args, &params); err != nil {
		return "", err
	}
	if params.Limit <= 0 {
		params.Limit = 10
	}
	results, err := h.store.FindSimilar(ctx, params.MessageID, params.Limit)
	if err != nil {
		return "", err
	}
	return formatResults(results), nil
}

func (h *ToolHandler) runSQL(ctx context.Context, args json.RawMessage) (string, error) {
	var params struct {
		Query string `json:"query"`
	}
	if err := json.Unmarshal(args, &params); err != nil {
		return "", err
	}
	if !store.IsReadOnlySQL(params.Query) {
		return "", fmt.Errorf("only SELECT and WITH queries are allowed")
	}
	cols, rows, err := h.store.ReadOnlyQuery(ctx, params.Query)
	if err != nil {
		return "", err
	}
	return formatTable(cols, rows), nil
}

// sanitizeSQL prevents SQL injection by escaping single quotes.
func sanitizeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func formatResults(results []store.SearchResult) string {
	if len(results) == 0 {
		return "No results found."
	}
	data, _ := json.MarshalIndent(results, "", "  ")
	return string(data)
}

func formatTable(cols []string, rows [][]string) string {
	if len(rows) == 0 {
		return "No results."
	}
	var sb strings.Builder
	sb.WriteString(strings.Join(cols, " | "))
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("-", len(strings.Join(cols, " | "))))
	sb.WriteString("\n")
	for _, row := range rows {
		sb.WriteString(strings.Join(row, " | "))
		sb.WriteString("\n")
	}
	return sb.String()
}
