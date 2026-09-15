package embedder

import (
	"context"
	"fmt"
	"strings"
	"time"
	"testing"

	"github.com/giveme11us/discrawl-me/internal/store"
	"github.com/stretchr/testify/require"
)

// countingProvider records the size of every Embed call so a test can assert
// how the worker grouped the jobs, not just that it finished.
type countingProvider struct {
	callSizes []int
	// failOn makes the batch containing this text fail, to exercise the
	// individual-retry fallback.
	failOn string
}

func (p *countingProvider) Name() string { return "counting" }
func (p *countingProvider) Dim() int     { return 3 }

func (p *countingProvider) Embed(_ context.Context, texts []string) ([][]float32, error) {
	p.callSizes = append(p.callSizes, len(texts))
	if p.failOn != "" && len(texts) > 1 {
		for _, t := range texts {
			if strings.Contains(t, p.failOn) {
				return nil, fmt.Errorf("simulated batch rejection")
			}
		}
	}
	out := make([][]float32, len(texts))
	for i, t := range texts {
		if t == p.failOn {
			return nil, fmt.Errorf("simulated per-message rejection")
		}
		out[i] = []float32{float32(i), 1, 2}
	}
	return out, nil
}

func seedMessages(ctx context.Context, t *testing.T, s *store.Store, contents map[string]string) {
	t.Helper()
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "G", RawJSON: "{}"}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "gen", RawJSON: "{}"}))
	for id, body := range contents {
		require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
			ID: id, GuildID: "g1", ChannelID: "c1", ChannelName: "gen",
			AuthorID: "u1", AuthorName: "User", MessageType: 0,
			CreatedAt: "2026-01-01T00:00:00Z", Content: body,
			NormalizedContent: body, RawJSON: "{}",
		}))
		_, err := s.DB().ExecContext(ctx, `
			insert into embedding_jobs(message_id, state, attempts, updated_at)
			values(?, 'pending', 0, '2026-01-01T00:00:00Z')
		`, id)
		require.NoError(t, err)
	}
}

// One HTTP request per message does not scale: 981k messages meant 981k calls
// and a CDN answering 403 to the burst. The worker must group jobs into
// batch_size requests instead.
func TestWorkerBatchesRequests(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, t.TempDir()+"/test.db")
	require.NoError(t, err)
	defer s.Close()

	contents := map[string]string{}
	for i := 0; i < 10; i++ {
		contents[fmt.Sprintf("m%02d", i)] = fmt.Sprintf("message number %d", i)
	}
	seedMessages(ctx, t, s, contents)

	p := &countingProvider{}
	w := NewWorker(s, p, 4, nil)
	n, err := w.RunAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, n)

	// 10 jobs at batch size 4 is three requests (4+4+2), never ten.
	require.Equal(t, []int{4, 4, 2}, p.callSizes,
		"worker should send one request per batch, not one per message")
}

// A batch that the provider rejects must not discard the messages bundled with
// the offending one: they are retried individually and still stored.
func TestWorkerFallsBackToIndividualOnBatchFailure(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, t.TempDir()+"/test.db")
	require.NoError(t, err)
	defer s.Close()

	seedMessages(ctx, t, s, map[string]string{
		"m1": "good one",
		"m2": "poison",
		"m3": "good two",
	})

	p := &countingProvider{failOn: "poison"}
	w := NewWorker(s, p, 3, nil)
	n, _ := w.RunAll(ctx)

	// The poisoned message fails, the other two survive. RunAll no longer
	// propagates the error: it retries failed passes and the bad job burns its
	// own retry allowance, so the run ends cleanly with the good work stored.
	require.Equal(t, 2, n, "messages batched with a bad one must still be stored")

	var stored int
	require.NoError(t, s.DB().QueryRowContext(ctx,
		`select count(*) from message_embeddings`).Scan(&stored))
	require.Equal(t, 2, stored)

	// First the batch of three, then the individual retries.
	require.Equal(t, 3, p.callSizes[0])
	require.Greater(t, len(p.callSizes), 1, "a rejected batch must be retried individually")
}

// flakyProvider fails the first `failFirst` calls, then succeeds. It models a
// provider that returns 502 for a few seconds and recovers.
type flakyProvider struct {
	calls     int
	failFirst int
}

func (p *flakyProvider) Name() string { return "flaky" }
func (p *flakyProvider) Dim() int     { return 3 }

func (p *flakyProvider) Embed(_ context.Context, texts []string) ([][]float32, error) {
	p.calls++
	if p.calls <= p.failFirst {
		return nil, fmt.Errorf("simulated 502 from gateway")
	}
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = []float32{float32(i), 1, 2}
	}
	return out, nil
}

// A transient provider outage must not end a long run: a single 502 once
// killed a 15-hour indexing pass at 9% and nothing noticed for two hours.
func TestRunAllSurvivesTransientProviderFailure(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, t.TempDir()+"/test.db")
	require.NoError(t, err)
	defer s.Close()

	contents := map[string]string{}
	for i := 0; i < 6; i++ {
		contents[fmt.Sprintf("m%02d", i)] = fmt.Sprintf("message %d", i)
	}
	seedMessages(ctx, t, s, contents)

	// Fails the batch and all 6 individual retries, then recovers.
	p := &flakyProvider{failFirst: 7}
	w := NewWorker(s, p, 6, nil)

	n, err := w.RunAll(ctx)
	require.NoError(t, err, "a recovered outage must not fail the run")
	require.Equal(t, 6, n, "every message must be embedded after recovery")

	var stored int
	require.NoError(t, s.DB().QueryRowContext(ctx,
		`select count(*) from message_embeddings`).Scan(&stored))
	require.Equal(t, 6, stored)
}

// A provider that never recovers must terminate the run instead of retrying
// forever. Note the run may end either by exhausting the consecutive-failure
// budget or because every job burned its own retry allowance and left the
// queue — both are correct terminations; what matters is that RunAll returns
// and does not store a vector it never received.
func TestRunAllGivesUpOnPersistentFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s, err := store.Open(ctx, t.TempDir()+"/test.db")
	require.NoError(t, err)
	defer s.Close()

	seedMessages(ctx, t, s, map[string]string{"m1": "one"})

	p := &flakyProvider{failFirst: 1 << 30}
	w := NewWorker(s, p, 1, nil)

	done := make(chan struct{})
	var n int
	go func() {
		n, _ = w.RunAll(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("RunAll never returned against a permanently failing provider")
	}

	require.Equal(t, 0, n, "nothing can be reported as embedded")
	var stored int
	require.NoError(t, s.DB().QueryRowContext(ctx,
		`select count(*) from message_embeddings`).Scan(&stored))
	require.Equal(t, 0, stored, "no vector may be stored when every call failed")
}
