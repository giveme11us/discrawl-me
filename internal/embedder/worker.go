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

	// Embed each message individually to handle failures gracefully
	for _, j := range jobs {
		// Truncate content to avoid context length errors
		content := j.content
		if len(content) > 30000 {
			content = content[:30000]
		}

		vecs, err := w.provider.Embed(ctx, []string{content})
		if err == nil {
			err = validateEmbeddingResponse(vecs, w.provider.Dim())
		}
		if err != nil {
			failed++
			if stateErr := w.failJob(ctx, j.messageID, now); stateErr != nil {
				return processed, stateErr
			}
			w.logger.Warn("embed failed", "message_id", j.messageID, "err", err)
			continue
		}

		vecBlob := float32ToBytes(vecs[0])
		_, err = w.db.ExecContext(ctx, `
			insert into message_embeddings(message_id, model, dim, vec, created_at)
			values(?, ?, ?, ?, ?)
			on conflict(message_id) do update set
				model=excluded.model, dim=excluded.dim, vec=excluded.vec, created_at=excluded.created_at
		`, j.messageID, w.provider.Name(), w.provider.Dim(), vecBlob, now)
		if err != nil {
			failed++
			w.logger.Warn("store embedding failed", "message_id", j.messageID, "err", err)
			if stateErr := w.failJob(ctx, j.messageID, now); stateErr != nil {
				return processed, stateErr
			}
			continue
		}
		if _, err := w.db.ExecContext(ctx, `
			update embedding_jobs set state = 'done', updated_at = ? where message_id = ?
		`, now, j.messageID); err != nil {
			return processed, fmt.Errorf("mark embedding job done: %w", err)
		}
		processed++
	}

	if failed > 0 {
		return processed, fmt.Errorf("%d embedding job(s) failed", failed)
	}
	return processed, nil
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

func validateEmbeddingResponse(vecs [][]float32, expectedDim int) error {
	if len(vecs) != 1 {
		return fmt.Errorf("provider returned %d vectors, want 1", len(vecs))
	}
	if len(vecs[0]) == 0 || len(vecs[0]) != expectedDim {
		return fmt.Errorf("provider returned dimension %d, want %d", len(vecs[0]), expectedDim)
	}
	for _, value := range vecs[0] {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return fmt.Errorf("provider returned a non-finite vector value")
		}
	}
	return nil
}

// RunAll processes all pending jobs in batches until none remain.
func (w *Worker) RunAll(ctx context.Context) (int, error) {
	total := 0
	for {
		n, err := w.RunOnce(ctx)
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, nil
		}
		total += n
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
