package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/giveme11us/discrawl-me/internal/store"
	"github.com/stretchr/testify/require"
)

type metadataClient struct {
	*fakeClient
	discoveryErr error
	archiveErr   error
}

func (f *metadataClient) GuildChannels(ctx context.Context, id string) ([]*discordgo.Channel, error) {
	if f.discoveryErr != nil {
		return nil, f.discoveryErr
	}
	return f.fakeClient.GuildChannels(ctx, id)
}

func (f *metadataClient) ThreadsArchived(ctx context.Context, id string, private bool) ([]*discordgo.Channel, error) {
	if f.archiveErr != nil {
		return nil, f.archiveErr
	}
	return f.fakeClient.ThreadsArchived(ctx, id, private)
}

func TestDiscoveryBothGuildsMetadataThenIncrementalPayloads(t *testing.T) {
	for _, gid := range []string{"422855027535642625", "1347333602794541066"} {
		t.Run(gid, func(t *testing.T) {
			ctx := context.Background()
			db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
			require.NoError(t, err)
			defer db.Close()
			channel := &discordgo.Channel{ID: "700000000000000001", GuildID: gid, Name: "06-11-new-item", Type: discordgo.ChannelTypeGuildText, LastMessageID: "800000000000000001"}
			excluded := &discordgo.Channel{ID: "700000000000000002", GuildID: gid, Name: "excluded", Type: discordgo.ChannelTypeGuildText}
			var msg discordgo.Message
			require.NoError(t, json.Unmarshal([]byte(`{"id":"800000000000000001","channel_id":"700000000000000001","timestamp":"2026-10-15T08:00:00Z","author":{"id":"900000000000000001","username":"fixture"},"content":"synthetic text","embeds":[{"title":"synthetic embed","fields":[{"name":"edition","value":"fixture EN"}]}],"message_reference":{"type":1,"message_id":"800000000000000000"},"message_snapshots":[{"message":{"content":"synthetic forward","embeds":[{"title":"forward embed"}]}}]}`), &msg))
			f := &metadataClient{fakeClient: &fakeClient{guilds: []*discordgo.UserGuild{{ID: gid}}, guildByID: map[string]*discordgo.Guild{gid: {ID: gid, Name: "fixture"}}, channels: map[string][]*discordgo.Channel{gid: {channel, excluded}}, messages: map[string][]*discordgo.Message{channel.ID: {&msg}}}}
			syncer := New(f, db, nil)
			report, err := syncer.DiscoverChannels(ctx, gid, nil)
			require.NoError(t, err)
			require.Len(t, report.Channels, 2)
			require.Empty(t, f.messageCalls)
			require.Zero(t, f.memberCalls)
			require.Zero(t, f.tailCalls)
			require.False(t, report.Coverage.Complete)
			opts := SyncOptions{GuildIDs: []string{gid}, ChannelIDs: []string{channel.ID}, Full: true, Concurrency: 1}
			stats, err := syncer.Sync(ctx, opts)
			require.NoError(t, err)
			require.Equal(t, 1, stats.Messages)
			require.Zero(t, f.messageCalls[excluded.ID])
			_, rows, err := db.ReadOnlyQuery(ctx, "select count(*),raw_json from messages")
			require.NoError(t, err)
			require.Equal(t, "1", rows[0][0])
			require.Contains(t, rows[0][1], "synthetic embed")
			require.Contains(t, rows[0][1], "synthetic forward")
			require.Contains(t, rows[0][1], "message_snapshots")
			require.Contains(t, rows[0][1], "fixture EN")
			_, err = syncer.DiscoverChannels(ctx, gid, nil)
			require.NoError(t, err)
			stats, err = syncer.Sync(ctx, opts)
			require.NoError(t, err)
			require.Zero(t, stats.Messages)
			_, rows, err = db.ReadOnlyQuery(ctx, "select count(*) from messages")
			require.NoError(t, err)
			require.Equal(t, "1", rows[0][0])
			require.Zero(t, f.messageCalls[excluded.ID])
		})
	}
}

func TestDiscoveryRenameMoveArchiveCoverageAndFailurePreservesState(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	require.NoError(t, err)
	defer db.Close()
	gid := "422855027535642625"
	parent := &discordgo.Channel{ID: "parent", GuildID: gid, Name: "TCG INFO", Type: discordgo.ChannelTypeGuildText}
	thread := &discordgo.Channel{ID: "thread", GuildID: gid, ParentID: "parent", Name: "item", Type: discordgo.ChannelTypeGuildPublicThread, ThreadMetadata: &discordgo.ThreadMetadata{Archived: true, ArchiveTimestamp: time.Now()}}
	f := &metadataClient{fakeClient: &fakeClient{channels: map[string][]*discordgo.Channel{gid: {parent}}, guildThreadErrs: map[string]error{gid: errors.New("fixture active unavailable")}, publicArchived: map[string][]*discordgo.Channel{"parent": {thread}}}}
	syncer := New(f, db, nil)
	report, err := syncer.DiscoverChannels(ctx, gid, []string{"parent"})
	require.NoError(t, err)
	require.Len(t, report.Channels, 2)
	require.Equal(t, "unavailable", report.Coverage.ActiveThreads)
	require.Equal(t, "observed", report.Coverage.PublicArchives["parent"])
	require.False(t, report.Coverage.Complete)
	parent.Name = "renamed"
	parent.ParentID = "past-category"
	f.archiveErr = errors.New("fixture archive forbidden")
	report, err = syncer.DiscoverChannels(ctx, gid, []string{"parent"})
	require.NoError(t, err)
	require.Len(t, report.CachedThreads, 1)
	require.Equal(t, "unavailable", report.Coverage.PublicArchives["parent"])
	rows, err := db.Channels(ctx, gid)
	require.NoError(t, err)
	var found store.ChannelRow
	for _, r := range rows {
		if r.ID == "parent" {
			found = r
		}
	}
	require.Equal(t, "renamed", found.Name)
	require.Equal(t, "past-category", found.ParentID)
	require.NoError(t, db.SetSyncState(ctx, "channel:parent:latest_message_id", "keep-cursor"))
	before, err := db.GetSyncState(ctx, "discovery:"+gid+":last_success")
	require.NoError(t, err)
	f.discoveryErr = errors.New("fixture discovery offline")
	_, err = syncer.DiscoverChannels(ctx, gid, nil)
	require.Error(t, err)
	after, err := db.GetSyncState(ctx, "discovery:"+gid+":last_success")
	require.NoError(t, err)
	require.Equal(t, before, after)
	cursor, err := db.GetSyncState(ctx, "channel:parent:latest_message_id")
	require.NoError(t, err)
	require.Equal(t, "keep-cursor", cursor)
	rows, err = db.Channels(ctx, gid)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Empty(t, f.messageCalls)
}

func TestDiscoveryFormerlyEmptyChannelNoticesFirstMessage(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	require.NoError(t, err)
	defer db.Close()
	gid := "422855027535642625"
	ch := &discordgo.Channel{ID: "700000000000000001", GuildID: gid, Name: "empty", Type: discordgo.ChannelTypeGuildText}
	f := &metadataClient{fakeClient: &fakeClient{guilds: []*discordgo.UserGuild{{ID: gid}}, guildByID: map[string]*discordgo.Guild{gid: {ID: gid, Name: "fixture"}}, channels: map[string][]*discordgo.Channel{gid: {ch}}, messages: map[string][]*discordgo.Message{}}}
	syncer := New(f, db, nil)
	opts := SyncOptions{GuildIDs: []string{gid}, ChannelIDs: []string{ch.ID}, Full: true, Concurrency: 1, RefreshMembers: true}
	_, err = syncer.DiscoverChannels(ctx, gid, nil)
	require.NoError(t, err)
	_, err = syncer.Sync(ctx, opts)
	require.NoError(t, err)
	require.Equal(t, 1, f.memberCalls)
	ch.LastMessageID = "800000000000000001"
	f.messages[ch.ID] = []*discordgo.Message{{ID: ch.LastMessageID, ChannelID: ch.ID, Content: "first message", Timestamp: time.Now(), Author: &discordgo.User{ID: "fixture"}}}
	_, err = syncer.DiscoverChannels(ctx, gid, nil)
	require.NoError(t, err)
	stats, err := syncer.Sync(ctx, opts)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)
}

func TestDiscoveryCachedLastMessageIDAndScope(t *testing.T) {
	row := store.ChannelRow{ID: "fixture", Kind: "text", RawJSON: `{"last_message_id":"123"}`}
	require.Equal(t, "123", channelFromRow(row).LastMessageID)
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	require.NoError(t, err)
	defer db.Close()
	f := &metadataClient{fakeClient: &fakeClient{channels: map[string][]*discordgo.Channel{"guild": {{ID: "other", GuildID: "wrong", Type: discordgo.ChannelTypeGuildText}}}}}
	_, err = New(f, db, nil).DiscoverChannels(ctx, "guild", nil)
	require.ErrorContains(t, err, "cross-guild")
	rows, err := db.Channels(ctx, "")
	require.NoError(t, err)
	require.Empty(t, rows)
}
