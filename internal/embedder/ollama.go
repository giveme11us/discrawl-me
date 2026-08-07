package embedder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// OllamaProvider generates embeddings using a local Ollama instance.
type OllamaProvider struct {
	endpoint string
	model    string
	dim      int
	client   *http.Client
}

// NewOllama creates an Ollama embedding provider.
// Default endpoint is http://localhost:11434, default model is nomic-embed-text (768d).
func NewOllama(endpoint, model string, dim int) *OllamaProvider {
	if endpoint == "" {
		endpoint = "http://localhost:11434"
	}
	if model == "" {
		model = "nomic-embed-text"
	}
	if dim <= 0 {
		dim = 768
	}
	return &OllamaProvider{
		endpoint: strings.TrimRight(endpoint, "/"),
		model:    model,
		dim:      dim,
		client:   &http.Client{Timeout: 60 * time.Second},
	}
}

func (p *OllamaProvider) Name() string { return "ollama:" + p.model }
func (p *OllamaProvider) Dim() int     { return p.dim }

func (p *OllamaProvider) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	// Truncate texts to max embedding length to avoid context length errors.
	// nomic-embed-text has 8192 token context; ~4 chars/token, use 30000 as safe limit.
	const maxEmbedChars = 30000
	results := make([][]float32, len(texts))
	for i, text := range texts {
		if len(text) > maxEmbedChars {
			text = text[:maxEmbedChars]
		}
		vec, err := p.embedSingle(ctx, text)
		if err != nil {
			return nil, fmt.Errorf("embed text %d: %w", i, err)
		}
		results[i] = vec
	}
	return results, nil
}

func (p *OllamaProvider) embedSingle(ctx context.Context, text string) ([]float32, error) {
	body, _ := json.Marshal(map[string]any{
		"model":  p.model,
		"prompt": text,
	})
	req, err := http.NewRequestWithContext(ctx, "POST", p.endpoint+"/api/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("ollama error %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Embedding []float64 `json:"embedding"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode ollama response: %w", err)
	}
	if len(result.Embedding) == 0 {
		return nil, fmt.Errorf("ollama returned an empty embedding")
	}

	// Convert float64 to float32
	vec := make([]float32, len(result.Embedding))
	for i, v := range result.Embedding {
		vec[i] = float32(v)
	}
	return vec, nil
}
