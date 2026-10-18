package mcp

import (
	"context"
	"encoding/json"
)

// SetSelectiveMemory marks this handler as serving a selective memory DB:
// every result is prefixed with notice and a memory_status tool is exposed.
// Vector/hybrid search and find_similar work as on a full archive, over the
// embeddings kept for the retained messages (vector/hybrid by query text
// still need an embedding provider; find_similar does not).
func (h *ToolHandler) SetSelectiveMemory(notice string, status func(context.Context) (string, error)) {
	h.memoryNotice = notice
	h.memoryStatus = status
}

func (h *ToolHandler) selectiveTools(tools []map[string]any) []map[string]any {
	if h.memoryStatus == nil {
		return tools
	}
	out := make([]map[string]any, 0, len(tools)+1)
	for _, t := range tools {
		if t["name"] == "search_messages" {
			copyTool := map[string]any{}
			for k, v := range t {
				copyTool[k] = v
			}
			copyTool["description"] = "Search the SELECTIVE memory (retained messages only: rolling window of the policy). FTS5 includes embed fields and forwarded snapshots; vector/hybrid use the embeddings kept for retained messages (messages not yet embedded are absent from vector results). Absence of a result does not mean the message never existed."
			t = copyTool
		}
		out = append(out, t)
	}
	return append(out, map[string]any{
		"name":        "memory_status",
		"description": "Report selective-memory retention policy, coverage window, counts and file size. Call this before concluding that something does not exist.",
		"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
	})
}

func (h *ToolHandler) callSelective(ctx context.Context, name string, args json.RawMessage, next func() (string, error)) (string, error) {
	if h.memoryStatus == nil {
		return next()
	}
	if name == "memory_status" {
		return h.memoryStatus(ctx)
	}
	out, err := next()
	if err != nil {
		return out, err
	}
	return "[" + h.memoryNotice + "]\n" + out, nil
}
