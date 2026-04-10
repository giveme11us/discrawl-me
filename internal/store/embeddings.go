package store

import (
	"context"
	"encoding/binary"
	"math"
)

// VectorSearch finds messages by cosine similarity to a query vector.
func (s *Store) VectorSearch(ctx context.Context, queryVec []float32, limit int) ([]SearchResult, error) {
	if limit <= 0 {
		limit = 20
	}
	// Load all embeddings — feasible for personal archives (< 1M messages)
	rows, err := s.db.QueryContext(ctx, `
		select me.message_id, me.vec,
			m.guild_id, m.channel_id, coalesce(c.name, ''),
			coalesce(m.author_id, ''), coalesce(m.content, m.normalized_content),
			m.created_at
		from message_embeddings me
		join messages m on m.id = me.message_id
		left join channels c on c.id = m.channel_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type scoredResult struct {
		result SearchResult
		score  float64
	}
	var candidates []scoredResult
	for rows.Next() {
		var r SearchResult
		var vecBlob []byte
		var created string
		if err := rows.Scan(&r.MessageID, &vecBlob, &r.GuildID, &r.ChannelID, &r.ChannelName,
			&r.AuthorID, &r.Content, &created); err != nil {
			return nil, err
		}
		r.CreatedAt = parseTime(created)
		vec := bytesToFloat32(vecBlob)
		sim := cosineSimilarity(queryVec, vec)
		candidates = append(candidates, scoredResult{result: r, score: sim})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Sort by score descending (insertion sort, fine for < 1000 items)
	for i := 1; i < len(candidates); i++ {
		j := i
		for j > 0 && candidates[j].score > candidates[j-1].score {
			candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
			j--
		}
	}
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	results := make([]SearchResult, len(candidates))
	for i, c := range candidates {
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
		vecResults, err = s.VectorSearch(ctx, opts.QueryVec, 1000)
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

