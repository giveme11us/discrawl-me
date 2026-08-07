package store

import (
	"container/heap"
	"context"
	"encoding/binary"
	"math"
	"strings"
)

// VectorSearch finds messages by cosine similarity to a query vector.
func (s *Store) VectorSearch(ctx context.Context, queryVec []float32, limit int) ([]SearchResult, error) {
	return s.VectorSearchWithOptions(ctx, SearchOptions{QueryVec: queryVec, Limit: limit})
}

// VectorSearchWithOptions finds the top-k messages by cosine similarity while
// retaining only k candidates in memory.
func (s *Store) VectorSearchWithOptions(ctx context.Context, opts SearchOptions) ([]SearchResult, error) {
	if opts.Limit <= 0 {
		opts.Limit = 20
	}
	if len(opts.QueryVec) == 0 {
		return nil, nil
	}
	clauses := []string{"me.dim = ?"}
	args := []any{len(opts.QueryVec)}
	if len(opts.GuildIDs) > 0 {
		clauses = append(clauses, "m.guild_id in ("+placeholders(len(opts.GuildIDs))+")")
		for _, guildID := range opts.GuildIDs {
			args = append(args, guildID)
		}
	}
	if strings.TrimSpace(opts.Channel) != "" {
		clauses = append(clauses, "(m.channel_id = ? or c.name like ?)")
		args = append(args, opts.Channel, "%"+opts.Channel+"%")
	}
	if strings.TrimSpace(opts.Author) != "" {
		clauses = append(clauses, "(m.author_id = ? or m.raw_json like ?)")
		args = append(args, opts.Author, "%"+opts.Author+"%")
	}
	query := `
		select me.message_id, me.vec,
			m.guild_id, m.channel_id, coalesce(c.name, ''),
			coalesce(m.author_id, ''), '',
			coalesce(m.content, m.normalized_content),
			m.created_at
		from message_embeddings me
		join messages m on m.id = me.message_id
		left join channels c on c.id = m.channel_id
		where ` + strings.Join(clauses, " and ")
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	candidates := &scoredResultHeap{}
	heap.Init(candidates)
	for rows.Next() {
		var r SearchResult
		var vecBlob []byte
		var created string
		if err := rows.Scan(&r.MessageID, &vecBlob, &r.GuildID, &r.ChannelID, &r.ChannelName,
			&r.AuthorID, &r.AuthorName, &r.Content, &created); err != nil {
			return nil, err
		}
		r.CreatedAt = parseTime(created)
		vec := bytesToFloat32(vecBlob)
		sim := cosineSimilarity(opts.QueryVec, vec)
		candidate := scoredResult{result: r, score: sim}
		if candidates.Len() < opts.Limit {
			heap.Push(candidates, candidate)
		} else if sim > (*candidates)[0].score {
			(*candidates)[0] = candidate
			heap.Fix(candidates, 0)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	results := make([]SearchResult, candidates.Len())
	for i := len(results) - 1; i >= 0; i-- {
		c := heap.Pop(candidates).(scoredResult)
		c.result.Score = c.score
		results[i] = c.result
	}
	return results, nil
}

// HybridSearch combines FTS5 and vector search using reciprocal rank fusion.
func (s *Store) HybridSearch(ctx context.Context, opts SearchOptions) ([]SearchResult, error) {
	if opts.Limit <= 0 {
		opts.Limit = 20
	}

	// FTS results (keyword match)
	ftsOpts := opts
	ftsOpts.Limit = 1000
	ftsResults, err := s.SearchMessages(ctx, ftsOpts)
	if err != nil {
		return nil, err
	}

	// Vector results (semantic match)
	var vecResults []SearchResult
	if len(opts.QueryVec) > 0 {
		vectorOpts := opts
		vectorOpts.Limit = 1000
		vecResults, err = s.VectorSearchWithOptions(ctx, vectorOpts)
		if err != nil {
			return nil, err
		}
	}

	// Reciprocal rank fusion
	merged := reciprocalRankFusion(ftsResults, vecResults, 60)

	if len(merged) > opts.Limit {
		merged = merged[:opts.Limit]
	}
	return merged, nil
}

type scoredResult struct {
	result SearchResult
	score  float64
}

type scoredResultHeap []scoredResult

func (h scoredResultHeap) Len() int           { return len(h) }
func (h scoredResultHeap) Less(i, j int) bool { return h[i].score < h[j].score }
func (h scoredResultHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *scoredResultHeap) Push(value any)    { *h = append(*h, value.(scoredResult)) }
func (h *scoredResultHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

// GetConversation returns messages around a target message.
func (s *Store) GetConversation(ctx context.Context, messageID string, before, after int) ([]SearchResult, error) {
	if before <= 0 {
		before = 20
	}
	if after <= 0 {
		after = 20
	}

	// Find the target message's channel and created_at
	var channelID, createdAt string
	err := s.db.QueryRowContext(ctx, `
		select channel_id, created_at from messages where id = ?
	`, messageID).Scan(&channelID, &createdAt)
	if err != nil {
		return nil, err
	}

	query := `
		select m.id, m.guild_id, m.channel_id, coalesce(c.name, ''),
			coalesce(m.author_id, ''), coalesce(m.content, m.normalized_content),
			m.created_at
		from messages m
		left join channels c on c.id = m.channel_id
		where m.channel_id = ?
		and m.created_at >= (
			select created_at from messages where channel_id = ? and created_at <= ?
			order by created_at desc limit 1 offset ?
		)
		and m.created_at <= (
			select created_at from messages where channel_id = ? and created_at >= ?
			order by created_at asc limit 1 offset ?
		)
		order by m.created_at asc
	`
	rows, err := s.db.QueryContext(ctx, query,
		channelID, channelID, createdAt, before, channelID, createdAt, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SearchResult
	for rows.Next() {
		var r SearchResult
		var created string
		if err := rows.Scan(&r.MessageID, &r.GuildID, &r.ChannelID, &r.ChannelName,
			&r.AuthorID, &r.Content, &created); err != nil {
			return nil, err
		}
		r.CreatedAt = parseTime(created)
		results = append(results, r)
	}
	return results, rows.Err()
}

// FindSimilar finds messages similar to a given message using its embedding.
func (s *Store) FindSimilar(ctx context.Context, messageID string, limit int) ([]SearchResult, error) {
	var vecBlob []byte
	err := s.db.QueryRowContext(ctx, `
		select vec from message_embeddings where message_id = ?
	`, messageID).Scan(&vecBlob)
	if err != nil {
		return nil, err
	}
	vec := bytesToFloat32(vecBlob)
	results, err := s.VectorSearch(ctx, vec, limit+1)
	if err != nil {
		return nil, err
	}
	// Remove the source message itself
	filtered := make([]SearchResult, 0, len(results))
	for _, r := range results {
		if r.MessageID != messageID {
			filtered = append(filtered, r)
		}
	}
	if len(filtered) > limit {
		filtered = filtered[:limit]
	}
	return filtered, nil
}

// reciprocalRankFusion merges two ranked lists using RRF with parameter k.
func reciprocalRankFusion(list1, list2 []SearchResult, k int) []SearchResult {
	scores := make(map[string]float64)
	results := make(map[string]SearchResult)

	for rank, r := range list1 {
		scores[r.MessageID] += 1.0 / float64(k+rank+1)
		results[r.MessageID] = r
	}
	for rank, r := range list2 {
		scores[r.MessageID] += 1.0 / float64(k+rank+1)
		if _, exists := results[r.MessageID]; !exists {
			results[r.MessageID] = r
		}
	}

	type scoredResult struct {
		result SearchResult
		score  float64
	}
	var merged []scoredResult
	for id, score := range scores {
		r := results[id]
		r.Score = score
		merged = append(merged, scoredResult{result: r, score: score})
	}
	for i := 1; i < len(merged); i++ {
		j := i
		for j > 0 && merged[j].score > merged[j-1].score {
			merged[j], merged[j-1] = merged[j-1], merged[j]
			j--
		}
	}

	out := make([]SearchResult, len(merged))
	for i, s := range merged {
		out[i] = s.result
	}
	return out
}

func cosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		fa, fb := float64(a[i]), float64(b[i])
		dot += fa * fb
		normA += fa * fa
		normB += fb * fb
	}
	denom := math.Sqrt(normA) * math.Sqrt(normB)
	if denom == 0 {
		return 0
	}
	return dot / denom
}

func bytesToFloat32(b []byte) []float32 {
	n := len(b) / 4
	vec := make([]float32, n)
	for i := 0; i < n; i++ {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return vec
}
