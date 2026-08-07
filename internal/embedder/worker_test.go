package embedder

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/steipete/discrawl/internal/store"
	"github.com/stretchr/testify/require"
)

type fakeProvider struct{}

func (f *fakeProvider) Name() string { return "fake" }
func (f *fakeProvider) Dim() int     { return 3 }
func (f *fakeProvider) Embed(_ context.Context, texts []string) ([][]float32, error) {
	vecs := make([][]float32, len(texts))
	for i := range texts {
		vecs[i] = []float32{1.0, 2.0, 3.0}
	}
	return vecs, nil
}

type invalidProvider struct {
	err error
}

func (p *invalidProvider) Name() string { return "invalid" }
func (p *invalidProvider) Dim() int     { return 3 }
func (p *invalidProvider) Embed(_ context.Context, _ []string) ([][]float32, error) {
	if p.err != nil {
		return nil, p.err
	}
	return [][]float32{{1, 2}}, nil
}

func TestWorkerRunOnceEmpty(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, t.TempDir()+"/test.db")
	require.NoError(t, err)
	defer s.Close()

	w := NewWorker(s, &fakeProvider{}, 10, nil)
	n, err := w.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

func TestWorkerBacklog(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, t.TempDir()+"/test.db")
	require.NoError(t, err)
	defer s.Close()

	w := NewWorker(s, &fakeProvider{}, 10, nil)
	backlog, err := w.Backlog(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, backlog)
}

func TestWorkerProcessesJob(t *testing.T) {
	ctx := context.Background()
	dbPath := t.TempDir() + "/test.db"
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer s.Close()

	// Insert a message and an embedding job
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "G", RawJSON: "{}"}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "gen", RawJSON: "{}"}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID: "m1", GuildID: "g1", ChannelID: "c1", ChannelName: "gen",
		AuthorID: "u1", AuthorName: "User", MessageType: 0,
		CreatedAt: "2026-01-01T00:00:00Z", Content: "hello world",
		NormalizedContent: "hello world", RawJSON: "{}",
	}))
	// Enqueue embedding job
	_, err = s.DB().ExecContext(ctx, `
		insert into embedding_jobs(message_id, state, attempts, updated_at)
		values('m1', 'pending', 0, '2026-01-01T00:00:00Z')
	`)
	require.NoError(t, err)

	w := NewWorker(s, &fakeProvider{}, 10, nil)
	backlog, err := w.Backlog(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, backlog)

	n, err := w.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Verify embedding stored
	var count int
	err = s.DB().QueryRowContext(ctx, `select count(*) from message_embeddings where message_id = 'm1'`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Verify job marked done
	var state string
	err = s.DB().QueryRowContext(ctx, `select state from embedding_jobs where message_id = 'm1'`).Scan(&state)
	require.NoError(t, err)
	require.Equal(t, "done", state)
}

func TestFloat32ToBytes(t *testing.T) {
	vec := []float32{1.0, 2.0, 3.0}
	b := float32ToBytes(vec)
	require.Len(t, b, 12) // 3 * 4 bytes
}

func TestWorkerRejectsInvalidVectorAndRequeuesJob(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, t.TempDir()+"/test.db")
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, insertEmbeddingJobFixture(ctx, s, "m1", "pending", time.Now().UTC()))

	w := NewWorker(s, &invalidProvider{}, 10, nil)
	n, err := w.RunOnce(ctx)
	require.Error(t, err)
	require.Zero(t, n)

	var state string
	var attempts int
	require.NoError(t, s.DB().QueryRowContext(ctx,
		`select state, attempts from embedding_jobs where message_id = 'm1'`,
	).Scan(&state, &attempts))
	require.Equal(t, "pending", state)
	require.Equal(t, 1, attempts)
}

func TestWorkerRecoversStaleProcessingJob(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, t.TempDir()+"/test.db")
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, insertEmbeddingJobFixture(ctx, s, "m1", "processing", time.Now().UTC().Add(-time.Hour)))

	w := NewWorker(s, &fakeProvider{}, 10, nil)
	n, err := w.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	var state string
	require.NoError(t, s.DB().QueryRowContext(ctx,
		`select state from embedding_jobs where message_id = 'm1'`,
	).Scan(&state))
	require.Equal(t, "done", state)
}

func TestWorkerReportsProviderFailure(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, t.TempDir()+"/test.db")
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, insertEmbeddingJobFixture(ctx, s, "m1", "pending", time.Now().UTC()))

	w := NewWorker(s, &invalidProvider{err: errors.New("provider unavailable")}, 10, nil)
	_, err = w.RunOnce(ctx)
	require.ErrorContains(t, err, "embedding job(s) failed")
}

func insertEmbeddingJobFixture(ctx context.Context, s *store.Store, messageID, state string, updatedAt time.Time) error {
	if err := s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "G", RawJSON: "{}"}); err != nil {
		return err
	}
	if err := s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "gen", RawJSON: "{}"}); err != nil {
		return err
	}
	if err := s.UpsertMessage(ctx, store.MessageRecord{
		ID: messageID, GuildID: "g1", ChannelID: "c1", ChannelName: "gen",
		AuthorID: "u1", AuthorName: "User", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Content: "hello", NormalizedContent: "hello", RawJSON: "{}",
	}); err != nil {
		return err
	}
	_, err := s.DB().ExecContext(ctx, `
		insert into embedding_jobs(message_id, state, attempts, updated_at)
		values(?, ?, 0, ?)
		on conflict(message_id) do update set state=excluded.state, attempts=0, updated_at=excluded.updated_at
	`, messageID, state, updatedAt.UTC().Format(time.RFC3339Nano))
	return err
}
