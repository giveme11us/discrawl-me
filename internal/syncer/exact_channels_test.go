package syncer

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/giveme11us/discrawl-me/internal/store"
	"github.com/stretchr/testify/require"
)

func TestExactChannelsForumPlanOwnsCollection(t *testing.T) {
	for _, cachedForum := range []bool{false, true} {
		name := "cold-forum"
		if cachedForum {
			name = "cached-forum"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
			require.NoError(t, err)
			defer db.Close()
			gid := "1347333602794541066"
			forum := &discordgo.Channel{ID: "forum", GuildID: gid, Name: "one-piece-releases", Type: discordgo.ChannelTypeGuildForum}
			good := &discordgo.Channel{ID: "good", GuildID: gid, ParentID: forum.ID, Name: "new-pokemon-post", Type: discordgo.ChannelTypeGuildPublicThread, LastMessageID: "100"}
			legacy := &discordgo.Channel{ID: "legacy", GuildID: gid, Name: "02-11-funko", Type: discordgo.ChannelTypeGuildText, LastMessageID: "200"}
			excluded := &discordgo.Channel{ID: "excluded", GuildID: gid, ParentID: forum.ID, Name: "02-11-funko", Type: discordgo.ChannelTypeGuildPublicThread, LastMessageID: "300"}
			private := &discordgo.Channel{ID: "private", GuildID: gid, ParentID: forum.ID, Name: "private", Type: discordgo.ChannelTypeGuildPrivateThread}
			late := &discordgo.Channel{ID: "late", GuildID: gid, ParentID: forum.ID, Name: "unplanned-post", Type: discordgo.ChannelTypeGuildPublicThread}
			for _, ch := range []*discordgo.Channel{good, legacy, excluded, private} {
				raw, err := json.Marshal(ch)
				require.NoError(t, err)
				require.NoError(t, db.UpsertChannel(ctx, toChannelRecord(ch, string(raw))))
			}
			if cachedForum {
				raw, err := json.Marshal(forum)
				require.NoError(t, err)
				require.NoError(t, db.UpsertChannel(ctx, toChannelRecord(forum, string(raw))))
			}
			messages := map[string][]*discordgo.Message{}
			for _, ch := range []*discordgo.Channel{good, legacy, excluded} {
				messages[ch.ID] = []*discordgo.Message{{ID: ch.LastMessageID, ChannelID: ch.ID, GuildID: gid, Content: "synthetic " + ch.ID, Timestamp: time.Now(), Author: &discordgo.User{ID: "fixture"}}}
			}
			f := &fakeClient{
				guilds:         []*discordgo.UserGuild{{ID: gid}},
				guildByID:      map[string]*discordgo.Guild{gid: {ID: gid}},
				channels:       map[string][]*discordgo.Channel{gid: {forum, legacy}},
				activeThreads:  map[string][]*discordgo.Channel{forum.ID: {good, excluded, private, late}},
				publicArchived: map[string][]*discordgo.Channel{forum.ID: {excluded}},
				messages:       messages,
			}
			svc := New(f, db, nil)
			opts := SyncOptions{Full: true, GuildIDs: []string{gid}, ChannelIDs: []string{forum.ID, good.ID, legacy.ID}, ExactChannels: true, Concurrency: 1}
			stats, err := svc.Sync(ctx, opts)
			require.NoError(t, err)
			require.Equal(t, 2, stats.Messages)
			require.Equal(t, 3, stats.Channels)
			require.Equal(t, 1, stats.Threads)
			require.Zero(t, f.threadCalls, "exact plan must not invoke implicit forum discovery")
			require.Zero(t, f.messageCalls[excluded.ID])
			require.Zero(t, f.messageCalls[private.ID])
			require.Zero(t, f.messageCalls[late.ID])
			require.Positive(t, f.messageCalls[good.ID])
			require.Positive(t, f.messageCalls[legacy.ID], "explicit legacy subject is preserved")
			stats, err = svc.Sync(ctx, opts)
			require.NoError(t, err)
			require.Zero(t, stats.Messages)
			_, rows, err := db.ReadOnlyQuery(ctx, "select count(*) from messages")
			require.NoError(t, err)
			require.Equal(t, "2", rows[0][0])
			require.Zero(t, f.messageCalls[excluded.ID])
		})
	}
}

func TestExactChannelsEmptyCacheAndInvalidOptions(t *testing.T) {
	ctx := context.Background()
	forum := &discordgo.Channel{ID: "forum", GuildID: "g", Type: discordgo.ChannelTypeGuildForum}
	thread := &discordgo.Channel{ID: "unplanned", GuildID: "g", ParentID: forum.ID, Type: discordgo.ChannelTypeGuildPublicThread}
	f := &fakeClient{channels: map[string][]*discordgo.Channel{"g": {forum, thread}}}
	svc := New(f, nil, nil)
	channels, targeted, err := svc.channelList(ctx, "g", []string{forum.ID}, channelCatalogFull, true)
	require.NoError(t, err)
	require.True(t, targeted)
	require.Equal(t, []*discordgo.Channel{forum}, channels)
	require.Zero(t, f.threadCalls)
	_, err = svc.Sync(ctx, SyncOptions{ExactChannels: true})
	require.ErrorContains(t, err, "requires non-empty")
	_, err = svc.Sync(ctx, SyncOptions{ExactChannels: true, ChannelIDs: []string{forum.ID}, IncludeDMs: true})
	require.ErrorContains(t, err, "DMs disabled")
}
