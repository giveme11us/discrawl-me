package embedder

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"time"
)

const staleProcessingAfter = 15 * time.Minute

type embeddingJob struct {
	messageID string
	content   string
}

// EmbeddingStore is the subset of store methods the worker needs.
type EmbeddingStore interface {
	DB() *sql.DB
}

// Worker processes pending embedding_jobs and stores vectors.
type Worker struct {
	db        *sql.DB
	provider  Provider
	batchSize int
	logger    *slog.Logger
}

// NewWorker creates an embedding worker.
func NewWorker(store EmbeddingStore, provider Provider, batchSize int, logger *slog.Logger) *Worker {
	if batchSize <= 0 {
		batchSize = 32
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		db:        store.DB(),
		provider:  provider,
		batchSize: batchSize,
		logger:    logger,
	}
}

// RunOnce processes one batch of pending embedding jobs. Returns the count processed.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	jobs, err := w.claimJobs(ctx)
	if err != nil {
		return 0, err
	}
	if len(jobs) == 0 {
		return 0, nil
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	processed := 0
	failed := 0

	// Embed in batches: one HTTP request per message does not scale, and a
	// remote endpoint behind a CDN answers 403 to the resulting burst (seen
	// with ~1000 messages against the Nous Portal proxy). A failed batch is
	// retried message by message, so a single bad input still cannot take the
	// rest of the batch down with it.
	for start := 0; start < len(jobs); start += w.batchSize {
		end := start + w.batchSize
		if end > len(jobs) {
			end = len(jobs)
		}
		chunk := jobs[start:end]

		texts := make([]string, len(chunk))
		for i, j := range chunk {
			texts[i] = truncateForEmbedding(j.content)
		}

		vecs, err := w.provider.Embed(ctx, texts)
		if err == nil {
			err = validateEmbeddingBatch(vecs, len(chunk), w.provider.Dim())
		}
		if err != nil {
			w.logger.Warn("embed batch failed, retrying individually",
				"size", len(chunk), "err", err)
			p, f, stateErr := w.embedIndividually(ctx, chunk, now)
			processed += p
			failed += f
			if stateErr != nil {
				return processed, stateErr
			}
			continue
		}

		for i, j := range chunk {
			if storeErr := w.storeVector(ctx, j.messageID, vecs[i], now); storeErr != nil {
				failed++
				w.logger.Warn("store embedding failed", "message_id", j.messageID, "err", storeErr)
				if stateErr := w.failJob(ctx, j.messageID, now); stateErr != nil {
					return processed, stateErr
				}
				continue
			}
			processed++
		}
	}

	if failed > 0 {
		return processed, fmt.Errorf("%d embedding job(s) failed", failed)
	}
	return processed, nil
}

// truncateForEmbedding caps a message at the length the providers accept,
// keeping whole bytes only — a Discord message never approaches this, but a
// pasted log or stack trace does.
func truncateForEmbedding(content string) string {
	if len(content) > 30000 {
		return content[:30000]
	}
	return content
}

// embedIndividually is the fallback for a rejected batch: it re-sends one
// message at a time so a single oversized or malformed input cannot discard
// the other messages that were bundled with it.
func (w *Worker) embedIndividually(ctx context.Context, jobs []embeddingJob, now string) (processed, failed int, fatal error) {
	for _, j := range jobs {
		vecs, err := w.provider.Embed(ctx, []string{truncateForEmbedding(j.content)})
		if err == nil {
			err = validateEmbeddingBatch(vecs, 1, w.provider.Dim())
		}
		if err == nil {
			err = w.storeVector(ctx, j.messageID, vecs[0], now)
		}
		if err != nil {
			failed++
			w.logger.Warn("embed failed", "message_id", j.messageID, "err", err)
			if stateErr := w.failJob(ctx, j.messageID, now); stateErr != nil {
				return processed, failed, stateErr
			}
			continue
		}
		processed++
	}
	return processed, failed, nil
}

// storeVector persists one embedding and marks its job done. Both statements
// belong together: a stored vector whose job stays pending would be recomputed
// and paid for on every later run.
func (w *Worker) storeVector(ctx context.Context, messageID string, vec []float32, now string) error {
	if _, err := w.db.ExecContext(ctx, `
		insert into message_embeddings(message_id, model, dim, vec, created_at)
		values(?, ?, ?, ?, ?)
		on conflict(message_id) do update set
			model=excluded.model, dim=excluded.dim, vec=excluded.vec, created_at=excluded.created_at
	`, messageID, w.provider.Name(), w.provider.Dim(), float32ToBytes(vec), now); err != nil {
		return err
	}
	if _, err := w.db.ExecContext(ctx, `
		update embedding_jobs set state = 'done', updated_at = ? where message_id = ?
	`, now, messageID); err != nil {
		return fmt.Errorf("mark embedding job done: %w", err)
	}
	return nil
}

func (w *Worker) claimJobs(ctx context.Context) ([]embeddingJob, error) {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin embedding claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	staleBefore := time.Now().UTC().Add(-staleProcessingAfter).Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `
		update embedding_jobs
		set state = 'pending', attempts = attempts + 1, updated_at = ?
		where state = 'processing' and updated_at < ?
	`, time.Now().UTC().Format(time.RFC3339Nano), staleBefore); err != nil {
		return nil, fmt.Errorf("recover stale embedding jobs: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `
		select ej.message_id, coalesce(m.content, '')
		from embedding_jobs ej
		join messages m on m.id = ej.message_id
		where ej.state = 'pending'
		order by ej.message_id
		limit ?
	`, w.batchSize)
	if err != nil {
		return nil, fmt.Errorf("query pending jobs: %w", err)
	}
	var jobs []embeddingJob
	for rows.Next() {
		var job embeddingJob
		if err := rows.Scan(&job.messageID, &job.content); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if job.content == "" {
			job.content = " "
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, job := range jobs {
		result, err := tx.ExecContext(ctx, `
			update embedding_jobs set state = 'processing', updated_at = ?
			where message_id = ? and state = 'pending'
		`, now, job.messageID)
		if err != nil {
			return nil, fmt.Errorf("claim embedding job %s: %w", job.messageID, err)
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return nil, fmt.Errorf("claim embedding job %s: concurrent state change", job.messageID)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit embedding claim: %w", err)
	}
	return jobs, nil
}

func (w *Worker) failJob(ctx context.Context, messageID, now string) error {
	result, err := w.db.ExecContext(ctx, `
		update embedding_jobs
		set state = case when attempts + 1 >= 3 then 'failed' else 'pending' end,
			attempts = attempts + 1, updated_at = ?
		where message_id = ? and state = 'processing'
	`, now, messageID)
	if err != nil {
		return fmt.Errorf("release embedding job %s: %w", messageID, err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return fmt.Errorf("release embedding job %s: unexpected state", messageID)
	}
	return nil
}

// validateEmbeddingBatch checks a provider response against the number of
// inputs sent. It generalises the single-vector check: the per-vector rules
// (right dimension, no NaN or Inf) are unchanged, but a batch must also come
// back with exactly one vector per input and in the same order, since the
// caller pairs vecs[i] with chunk[i].
func validateEmbeddingBatch(vecs [][]float32, want int, expectedDim int) error {
	if len(vecs) != want {
		return fmt.Errorf("provider returned %d vectors, want %d", len(vecs), want)
	}
	for i, vec := range vecs {
		if len(vec) == 0 || len(vec) != expectedDim {
			return fmt.Errorf("vector %d: provider returned dimension %d, want %d", i, len(vec), expectedDim)
		}
		for _, value := range vec {
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return fmt.Errorf("vector %d: provider returned a non-finite vector value", i)
			}
		}
	}
	return nil
}

func validateEmbeddingResponse(vecs [][]float32, expectedDim int) error {
	return validateEmbeddingBatch(vecs, 1, expectedDim)
}

// RunAll processes all pending jobs in batches until none remain.
//
// A transient provider error must not end the run: a single 502 from the
// endpoint once killed a 15-hour indexing pass that was 9% done, and nothing
// noticed for two hours. Failures are tolerated with backoff and only abort
// the run when they persist, which distinguishes "the network hiccuped" from
// "the endpoint is gone".
func (w *Worker) RunAll(ctx context.Context) (int, error) {
	const maxConsecutiveFailures = 10

	total := 0
	consecutive := 0

	for {
		n, err := w.RunOnce(ctx)
		// Count the work that succeeded even when the pass reports an error:
		// RunOnce returns both, and a batch where some messages failed still
		// stored the others. Returning the pre-pass total would understate the
		// vectors actually written and paid for.
		total += n

		if err != nil {
			// A cancelled context is the operator stopping us, not a fault.
			if ctx.Err() != nil {
				return total, ctx.Err()
			}
			consecutive++
			if consecutive >= maxConsecutiveFailures {
				return total, fmt.Errorf("%d consecutive failed passes, last: %w",
					consecutive, err)
			}
			// Exponential backoff capped at 30s: a provider under load needs
			// room to recover, and hammering it makes the outage worse.
			wait := time.Duration(1<<uint(consecutive-1)) * time.Second
			if wait > 30*time.Second {
				wait = 30 * time.Second
			}
			w.logger.Warn("embedding pass failed, retrying",
				"attempt", consecutive, "wait", wait, "total", total, "err", err)
			select {
			case <-ctx.Done():
				return total, ctx.Err()
			case <-time.After(wait):
			}
			continue
		}

		consecutive = 0
		if n == 0 {
			return total, nil
		}
		w.logger.Info("embedding batch processed", "batch", n, "total", total)
	}
}

// Backlog returns the number of pending embedding jobs.
func (w *Worker) Backlog(ctx context.Context) (int, error) {
	var count int
	err := w.db.QueryRowContext(ctx, `
		select count(*) from embedding_jobs where state = 'pending'
	`).Scan(&count)
	return count, err
}

// float32ToBytes converts a float32 slice to little-endian bytes.
func float32ToBytes(vec []float32) []byte {
	buf := make([]byte, len(vec)*4)
	for i, v := range vec {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}
	return buf
}
