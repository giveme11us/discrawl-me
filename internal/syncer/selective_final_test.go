package syncer

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/giveme11us/discrawl-me/internal/store"
)

func snowflakeAt(t time.Time, seq int) string {
	return strconv.FormatUint(uint64(t.UnixMilli()-1420070400000)<<22|uint64(seq), 10)
}

// Final policy (strict 30 days): after retention removed the old payload, the
// collector resumes from the persisted cursors. A backfill cursor older than
// the rolling floor issues NO backfill request at all, a completed history
// stays complete, and nothing older than the floor is downloaded again.
func TestSelectiveNoRedownloadOfPrunedHistory(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	cutoff := now.AddDate(0, 0, -30)
	var messages []*discordgo.Message
	ids := map[string]time.Time{}
	// 20 recent (0..19 days) then 40 old (31..70 days), newest first.
	for i := 0; i < 60; i++ {
		ts := now.Add(-time.Duration(i) * 24 * time.Hour)
		if i >= 20 {
			ts = now.AddDate(0, 0, -11-i)
		}
		id := snowflakeAt(ts, i)
		ids[id] = ts
		messages = append(messages, &discordgo.Message{ID: id, GuildID: "g1", ChannelID: "c1", Content: "m" + id, Timestamp: ts, Author: &discordgo.User{ID: "u1", Username: "u"}})
	}
	oldest := messages[len(messages)-1].ID
	for _, tc := range []struct {
		name     string
		complete bool
	}{{"backfill cursor older than floor", false}, {"history complete", true}} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
			require.NoError(t, err)
			defer func() { _ = s.Close() }()
			// State after cutover + prune: cursors copied verbatim, latest
			// at the 5th newest message, old payload already expelled.
			require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), messages[5].ID))
			require.NoError(t, s.SetSyncState(ctx, channelBackfillScope("c1"), oldest))
			if tc.complete {
				require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("c1"), "1"))
			}
			client := &recordingClient{fakeClient: &fakeClient{
				guilds:    []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
				guildByID: map[string]*discordgo.Guild{"g1": {ID: "g1", Name: "Guild"}},
				channels: map[string][]*discordgo.Channel{
					"g1": {{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText, LastMessageID: messages[0].ID}},
				},
				messages: map[string][]*discordgo.Message{"c1": messages},
			}}
			s.SetMessageRetention(cutoffRetention{cutoff: cutoff})
			svc := New(client, s, nil)
			svc.SetChannelSinceFloor(func(string) time.Time { return cutoff })
			for run := 0; run < 2; run++ {
				_, err = svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1})
				require.NoError(t, err)
			}
			for _, q := range client.reqs {
				require.Empty(t, q.before, "no backfill request below the rolling floor")
				for _, id := range q.served {
					require.False(t, ids[id].Before(cutoff), "served a message older than the floor")
				}
			}
			var stored, old int
			require.NoError(t, s.DB().QueryRow(`select count(*) from messages`).Scan(&stored))
			require.NoError(t, s.DB().QueryRow(`select count(*) from messages where created_at < ?`, cutoff.Format(time.RFC3339Nano)).Scan(&old))
			require.Equal(t, 5, stored, "only the 5 messages newer than the latest cursor")
			require.Zero(t, old)
			bf, _ := s.GetSyncState(ctx, channelBackfillScope("c1"))
			require.Equal(t, oldest, bf, "backfill cursor untouched")
			done, _ := s.GetSyncState(ctx, channelHistoryCompleteScope("c1"))
			require.Equal(t, tc.complete, done != "", "history_complete neither faked nor cleared")
		})
	}
}

func TestSnowflakeOlderThan(t *testing.T) {
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	id := snowflakeAt(at, 0)
	require.True(t, snowflakeOlderThan(id, at.Add(time.Second)))
	require.False(t, snowflakeOlderThan(id, at))
	require.False(t, snowflakeOlderThan("", at))
	require.False(t, snowflakeOlderThan("x", at))
	require.False(t, snowflakeOlderThan(id, time.Time{}))
}
