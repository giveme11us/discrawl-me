package syncer

import (
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

func TestToMessageRecordGuildIDFallback(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		name         string
		messageGuild string
		paramGuild   string
		wantGuild    string
	}{
		{
			name:         "message has guild_id — use it",
			messageGuild: "12345",
			paramGuild:   "99999",
			wantGuild:    "12345",
		},
		{
			name:         "message guild_id empty — fallback to param",
			messageGuild: "",
			paramGuild:   "99999",
			wantGuild:    "99999",
		},
		{
			name:         "both empty — stays empty",
			messageGuild: "",
			paramGuild:   "",
			wantGuild:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			record := toMessageRecord(
				&discordgo.Message{
					ID:        "m1",
					GuildID:   tt.messageGuild,
					ChannelID: "c1",
					Content:   "hello",
					Timestamp: now,
					Author:    &discordgo.User{ID: "u1", Username: "test"},
				},
				tt.paramGuild,
				"general",
				"hello",
			)
			require.Equal(t, tt.wantGuild, record.GuildID)
		})
	}
}