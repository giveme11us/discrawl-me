package cli

import (
	"testing"

	"github.com/giveme11us/discrawl-me/internal/config"
)

// The embedding worker validates every provider response against the
// dimension the provider reports, and rejects the whole batch on a mismatch.
// Before the dim field existed, createEmbedProvider passed a hardcoded 0, so
// any model that does not return 1536 values (bge-m3 returns 1024) failed on
// every batch with "provider returned dimension 1024, want 1536".
func TestCreateEmbedProviderHonoursConfiguredDim(t *testing.T) {
	r := &runtime{cfg: config.Config{}}
	r.cfg.Search.Embeddings = config.EmbeddingsConfig{
		Provider: "openai",
		Model:    "baai/bge-m3",
		Dim:      1024,
	}

	got := r.createEmbedProvider().Dim()
	if got != 1024 {
		t.Fatalf("configured dim not honoured: got %d, want 1024", got)
	}
}

// A zero dim must keep the previous behaviour so existing configs that never
// set the field are unaffected by the change.
func TestCreateEmbedProviderDefaultsWhenDimUnset(t *testing.T) {
	r := &runtime{cfg: config.Config{}}
	r.cfg.Search.Embeddings = config.EmbeddingsConfig{
		Provider: "openai",
		Model:    "openai/text-embedding-3-small",
	}

	got := r.createEmbedProvider().Dim()
	if got != 1536 {
		t.Fatalf("unset dim should fall back to the provider default: got %d, want 1536", got)
	}
}
