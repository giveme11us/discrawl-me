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
	// Claim a batch of pending jobs
	rows, err := w.db.QueryContext(ctx, `
		select ej.message_id, m.content
		from embedding_jobs ej
		join messages m on m.id = ej.message_id
		where ej.state = 'pending'
		order by ej.message_id
		limit ?
	`, w.batchSize)
	if err != nil {
		return 0, fmt.Errorf("query pending jobs: %w", err)
	}
	defer rows.Close()

	type job struct {
		messageID string
		content   string
	}
	var jobs []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.messageID, &j.content); err != nil {
			return 0, err
		}
		if j.content == "" {
			j.content = " " // avoid empty string
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(jobs) == 0 {
		return 0, nil
	}

	// Mark as processing
	for _, j := range jobs {
		_, _ = w.db.ExecContext(ctx, `
			update embedding_jobs set state = 'processing', updated_at = ? where message_id = ?
		`, time.Now().UTC().Format(time.RFC3339Nano), j.messageID)
	}

	// Generate embeddings
	texts := make([]string, len(jobs))
	for i, j := range jobs {
		texts[i] = j.content
	}
	vecs, err := w.provider.Embed(ctx, texts)
	if err != nil {
		// Mark as failed
		for _, j := range jobs {
			_, _ = w.db.ExecContext(ctx, `
				update embedding_jobs set state = 'pending', attempts = attempts + 1, updated_at = ?
				where message_id = ?
			`, time.Now().UTC().Format(time.RFC3339Nano), j.messageID)
		}
		return 0, fmt.Errorf("embed batch: %w", err)
	}

	// Store embeddings and mark complete
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for i, j := range jobs {
		vecBlob := float32ToBytes(vecs[i])
		_, err := w.db.ExecContext(ctx, `
			insert into message_embeddings(message_id, model, dim, vec, created_at)
			values(?, ?, ?, ?, ?)
			on conflict(message_id) do update set
				model=excluded.model, dim=excluded.dim, vec=excluded.vec, created_at=excluded.created_at
		`, j.messageID, w.provider.Name(), w.provider.Dim(), vecBlob, now)
		if err != nil {
			w.logger.Warn("store embedding failed", "message_id", j.messageID, "err", err)
			continue
		}
		_, _ = w.db.ExecContext(ctx, `
			update embedding_jobs set state = 'done', updated_at = ? where message_id = ?
		`, now, j.messageID)
	}

	return len(jobs), nil
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
