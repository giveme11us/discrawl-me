package store

import (
	"context"
	"fmt"
)

// migrateV1toV2 adds reaction_events and message_edits tables.
func (s *Store) migrateV1toV2(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)

	stmts := []string{
		`create table if not exists message_edits (
			message_id text not null,
			edited_at  text not null,
			content    text not null,
			raw_json   text not null,
			primary key (message_id, edited_at)
		);`,
		`create table if not exists reaction_events (
			event_id    integer primary key autoincrement,
			message_id  text not null,
			channel_id  text not null,
			guild_id    text,
			user_id     text not null,
			emoji_name  text not null,
			emoji_id    text,
			action      text not null,
			event_at    text not null
		);`,
		`create index if not exists idx_reactions_message_id on reaction_events(message_id);`,
		`create index if not exists idx_reactions_user_id on reaction_events(user_id, event_at);`,
		`create index if not exists idx_edits_message_id on message_edits(message_id);`,
	}

	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate v1→v2: %w", err)
		}
	}
	return tx.Commit()
}
