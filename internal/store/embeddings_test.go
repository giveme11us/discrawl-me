package store

import (
	"context"
	"encoding/binary"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVectorSearchWithOptionsReturnsTopKAndAppliesFilters(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "One", RawJSON: `{}`}))
	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g2", Name: "Two", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c2", GuildID: "g2", Kind: "text", Name: "other", RawJSON: `{}`}))

	fixtures := []struct {
		id, guild, channel, author string
		vec                        []float32
	}{
		{"m1", "g1", "c1", "u1", []float32{1, 0, 0}},
		{"m2", "g1", "c1", "u2", []float32{0.8, 0.2, 0}},
		{"m3", "g2", "c2", "u1", []float32{0.99, 0.01, 0}},
	}
	for _, fixture := range fixtures {
		require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
			ID: fixture.id, GuildID: fixture.guild, ChannelID: fixture.channel,
			AuthorID: fixture.author, AuthorName: fixture.author, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Content: fixture.id, NormalizedContent: fixture.id, RawJSON: `{}`,
		}))
		_, err := s.db.ExecContext(ctx, `
			insert into message_embeddings(message_id, model, dim, vec, created_at)
			values(?, 'test', 3, ?, ?)
		`, fixture.id, testVectorBytes(fixture.vec), time.Now().UTC().Format(time.RFC3339Nano))
		require.NoError(t, err)
	}

	results, err := s.VectorSearchWithOptions(ctx, SearchOptions{
		QueryVec: []float32{1, 0, 0}, GuildIDs: []string{"g1"}, Author: "u1", Limit: 1,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "m1", results[0].MessageID)
}

func testVectorBytes(values []float32) []byte {
	out := make([]byte, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(value))
	}
	return out
}
