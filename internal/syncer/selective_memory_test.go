package syncer

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/giveme11us/discrawl-me/internal/store"
)

type cutoffRetention struct{ cutoff time.Time }

func (c cutoffRetention) RetainMessage(rec store.MessageRecord) bool {
	t, err := time.Parse(time.RFC3339Nano, rec.CreatedAt)
	return err != nil || !t.Before(c.cutoff)
}

// Selective memory: the native writer stores only retained payload, keeps the
// high-water cursor from the fetched page, bounds the backfill at the floor
// without declaring history_complete, and a second run reimports nothing.
func TestSelectiveFloorBoundsBackfillWithoutFakeCompletion(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	now := time.Now().UTC()
	cutoff := now.AddDate(0, 0, -30)
	var messages []*discordgo.Message
	for id := 250; id >= 101; id-- {
		ts := now.Add(-time.Duration(250-id) * time.Hour)
		if id <= 200 {
			ts = now.AddDate(0, 0, -60).Add(-time.Duration(200-id) * time.Hour)
		}
		messages = append(messages, &discordgo.Message{
			ID: fmt.Sprintf("%03d", id), GuildID: "g1", ChannelID: "c1",
			Content: fmt.Sprintf("msg-%03d", id), Timestamp: ts,
			Author: &discordgo.User{ID: "u1", Username: "user"},
		})
	}
	client := &fakeClient{
		guilds:    []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{"g1": {ID: "g1", Name: "Guild"}},
		channels: map[string][]*discordgo.Channel{
			"g1": {{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText, LastMessageID: "250"}},
		},
		messages: map[string][]*discordgo.Message{"c1": messages},
	}
	s.SetMessageRetention(cutoffRetention{cutoff: cutoff})
	svc := New(client, s, nil)
	svc.SetChannelSinceFloor(func(string) time.Time { return cutoff })
	_, err = svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1, Embeddings: true})
	require.NoError(t, err)

	var stored, oldStored, jobs int
	require.NoError(t, s.DB().QueryRow(`select count(*) from messages`).Scan(&stored))
	require.NoError(t, s.DB().QueryRow(`select count(*) from messages where cast(id as integer) <= 200`).Scan(&oldStored))
	require.NoError(t, s.DB().QueryRow(`select count(*) from embedding_jobs`).Scan(&jobs))
	require.Equal(t, 50, stored)
	require.Zero(t, oldStored, "no old payload imported")
	require.Equal(t, 50, jobs, "retained messages are enqueued like on a full archive when embeddings are requested")
	latest, err := s.GetSyncState(ctx, channelLatestScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "250", latest)
	complete, err := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
	require.NoError(t, err)
	require.Empty(t, complete, "bounded backfill must not fake history_complete")
	backfill, err := s.GetSyncState(ctx, channelBackfillScope("c1"))
	require.NoError(t, err)
	require.Equal(t, "201", backfill)

	// Second run: cursors persist, nothing old is reimported.
	_, err = svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1})
	require.NoError(t, err)
	require.NoError(t, s.DB().QueryRow(`select count(*) from messages`).Scan(&stored))
	require.Equal(t, 50, stored)
	latest2, _ := s.GetSyncState(ctx, channelLatestScope("c1"))
	require.Equal(t, "250", latest2)
}
