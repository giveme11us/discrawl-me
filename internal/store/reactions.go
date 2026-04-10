package store

import (
	"context"
	"time"
)

// ReactionEventRecord represents a reaction add or remove event.
type ReactionEventRecord struct {
	MessageID string
	ChannelID string
	GuildID   string
	UserID    string
	EmojiName string
	EmojiID   string
	Action    string // "add" or "remove"
	EventAt   string
}

// AppendReactionEvent records a reaction add or remove.
func (s *Store) AppendReactionEvent(ctx context.Context, rec ReactionEventRecord) error {
	if rec.EventAt == "" {
		rec.EventAt = time.Now().UTC().Format(timeLayout)
	}
	_, err := s.db.ExecContext(ctx, `
		insert into reaction_events(message_id, channel_id, guild_id, user_id, emoji_name, emoji_id, action, event_at)
		values(?, ?, ?, ?, ?, ?, ?, ?)
	`, rec.MessageID, rec.ChannelID, rec.GuildID, rec.UserID, rec.EmojiName, rec.EmojiID, rec.Action, rec.EventAt)
	return err
}
