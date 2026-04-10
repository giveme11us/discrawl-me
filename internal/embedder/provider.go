package embedder

import "context"

// Provider generates embeddings from text inputs.
type Provider interface {
	Name() string
	Dim() int
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}
