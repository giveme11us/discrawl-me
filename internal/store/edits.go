package store

import (
	"context"
	"time"
)

// MessageEditRecord represents a snapshot of a message at a specific edit time.
type MessageEditRecord struct {
	MessageID string
	EditedAt  string
	Content   string
	RawJSON   string
}

// AppendMessageEdit saves a snapshot of a message's content before it was edited.
func (s *Store) AppendMessageEdit(ctx context.Context, rec MessageEditRecord) error {
	if rec.EditedAt == "" {
		rec.EditedAt = time.Now().UTC().Format(timeLayout)
	}
	_, err := s.db.ExecContext(ctx, `
		insert or ignore into message_edits(message_id, edited_at, content, raw_json)
		values(?, ?, ?, ?)
	`, rec.MessageID, rec.EditedAt, rec.Content, rec.RawJSON)
	return err
}
