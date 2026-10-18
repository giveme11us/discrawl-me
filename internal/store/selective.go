package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// MessageRetention is the opt-in selective-memory write policy. When set, the
// store drops message mutations the policy does not retain BEFORE opening the
// write transaction and indexes embed fields and forwarded snapshots in FTS.
// Retained messages are written exactly like on a full archive, including
// embedding job enqueueing when the caller asked for it (--with-embeddings). Sync cursors are written by the syncer from
// the fetched page, independently of this filter.
type MessageRetention interface {
	RetainMessage(record MessageRecord) bool
}

// SetMessageRetention installs (or clears with nil) the selective write policy.
func (s *Store) SetMessageRetention(r MessageRetention) {
	s.retention = r
}

// RetentionActive reports whether a selective write policy is installed.
func (s *Store) RetentionActive() bool { return s.retention != nil }

func (s *Store) applyRetention(messages []MessageMutation) []MessageMutation {
	if s.retention == nil {
		return messages
	}
	kept := make([]MessageMutation, 0, len(messages))
	for _, m := range messages {
		if !s.retention.RetainMessage(m.Record) {
			s.retentionSkipped++
			continue
		}
		kept = append(kept, m)
	}
	return kept
}

// RetentionSkipped returns how many mutations the selective policy dropped
// since the store was opened (diagnostic counter, never message content).
func (s *Store) RetentionSkipped() int { return s.retentionSkipped }

// selectiveOrphan reports whether, in selective mode, an event refers to a
// message that is not retained: edits/reactions/deletes for it are dropped so
// no dependent row exists without its message.
func (s *Store) selectiveOrphan(ctx context.Context, messageID string) bool {
	if s.retention == nil {
		return false
	}
	var n int
	if err := s.db.QueryRowContext(ctx, `select count(*) from messages where id = ?`, messageID).Scan(&n); err != nil {
		return false
	}
	if n == 0 {
		s.retentionSkipped++
		return true
	}
	return false
}

func (s *Store) ftsContent(message MessageRecord) string {
	if s.retention == nil {
		return message.NormalizedContent
	}
	return SelectiveFTSText(message.NormalizedContent, message.RawJSON)
}

type rawEmbed struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Fields      []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	} `json:"fields"`
	Footer *struct {
		Text string `json:"text"`
	} `json:"footer"`
	Author *struct {
		Name string `json:"name"`
	} `json:"author"`
}

type rawMessageText struct {
	Embeds    []rawEmbed `json:"embeds"`
	Snapshots []struct {
		Message struct {
			Content string     `json:"content"`
			Embeds  []rawEmbed `json:"embeds"`
		} `json:"message"`
	} `json:"message_snapshots"`
}

func embedParts(embeds []rawEmbed, withTitle bool) []string {
	var parts []string
	for _, e := range embeds {
		if withTitle {
			parts = append(parts, e.Title, e.Description)
		}
		if e.Author != nil {
			parts = append(parts, e.Author.Name)
		}
		for _, f := range e.Fields {
			parts = append(parts, f.Name, f.Value)
		}
		if e.Footer != nil {
			parts = append(parts, e.Footer.Text)
		}
	}
	return parts
}

// SelectiveFTSText extends the normalized content with embed fields/footer/
// author and forwarded message_snapshots text extracted from raw_json. The raw
// payload itself is stored unchanged; this only affects the FTS column.
func SelectiveFTSText(normalized, raw string) string {
	parts := []string{normalized}
	var payload rawMessageText
	if raw != "" && json.Unmarshal([]byte(raw), &payload) == nil {
		// title/description are already in normalized content.
		parts = append(parts, embedParts(payload.Embeds, false)...)
		for _, snap := range payload.Snapshots {
			parts = append(parts, snap.Message.Content)
			parts = append(parts, embedParts(snap.Message.Embeds, true)...)
		}
	}
	out := make([]string, 0, len(parts))
	seen := map[string]bool{}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	return strings.Join(out, "\n")
}

// OpenReadOnly opens an EXISTING discrawl DB for queries only: mode=ro,
// query_only, no file creation, no chmod, no migration. The schema version
// must match exactly; otherwise the caller must not read it.
func OpenReadOnly(ctx context.Context, path string) (*Store, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("open read-only: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("open read-only: refusing symlinked database path")
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("open read-only: database path is not a regular file")
	}
	dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(30000)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite read-only: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite read-only: %w", err)
	}
	s := &Store{db: db, path: path, readOnly: true}
	version, err := s.schemaVersion(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if version != storeSchemaVersion {
		_ = db.Close()
		return nil, fmt.Errorf("open read-only: schema version %d, want %d (no migration in read-only mode)", version, storeSchemaVersion)
	}
	return s, nil
}

// ReadOnly reports whether the store was opened with OpenReadOnly.
func (s *Store) ReadOnly() bool { return s.readOnly }

// MessageFTSRowID exposes the FTS rowid mapping used by the writer so that
// selective exports index rows exactly like incremental writes.
func MessageFTSRowID(messageID string) (int64, bool) { return messageFTSRowID(messageID) }
