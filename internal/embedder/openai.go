package embedder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

// OpenAIProvider generates embeddings using the OpenAI API.
type OpenAIProvider struct {
	apiKey string
	model  string
	dim    int
	client *http.Client
}

// NewOpenAI creates an OpenAI embedding provider.
func NewOpenAI(apiKeyEnv, model string, dim int) *OpenAIProvider {
	if model == "" {
		model = "text-embedding-3-small"
	}
	if dim <= 0 {
		dim = 1536
	}
	return &OpenAIProvider{
		apiKey: os.Getenv(apiKeyEnv),
		model:  model,
		dim:    dim,
		client: &http.Client{},
	}
}

func (p *OpenAIProvider) Name() string { return "openai:" + p.model }
func (p *OpenAIProvider) Dim() int     { return p.dim }

func (p *OpenAIProvider) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if p.apiKey == "" {
		return nil, fmt.Errorf("OpenAI API key not set")
	}
	body, _ := json.Marshal(map[string]any{
		"model": p.model,
		"input": texts,
	})
	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.openai.com/v1/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("openai error %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
			Index     int       `json:"index"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode openai response: %w", err)
	}

	vecs := make([][]float32, len(texts))
	for _, d := range result.Data {
		vec := make([]float32, len(d.Embedding))
		for i, v := range d.Embedding {
			vec[i] = float32(v)
		}
		vecs[d.Index] = vec
	}
	return vecs, nil
}
