package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/giveme11us/discrawl-me/internal/store"
	"github.com/stretchr/testify/require"
)

type publicParentClient struct {
	*metadataClient
	activeParentIDs  []string
	archiveParentIDs []string
	privateCalls     int
}

func (f *publicParentClient) ThreadsActive(ctx context.Context, id string) ([]*discordgo.Channel, error) {
	f.activeParentIDs = append(f.activeParentIDs, id)
	return f.fakeClient.ThreadsActive(ctx, id)
}

func (f *publicParentClient) ThreadsArchived(ctx context.Context, id string, private bool) ([]*discordgo.Channel, error) {
	if private {
		f.privateCalls++
	}
	f.archiveParentIDs = append(f.archiveParentIDs, id)
	return f.metadataClient.ThreadsArchived(ctx, id, private)
}

func TestDiscoveryPublicParentsArchiveUncachedAndActiveFallbackBeforeExactSync(t *testing.T) {
	for _, gid := range []string{"422855027535642625", "1347333602794541066"} {
		for _, guildUnavailable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/guild-unavailable-%t", gid, guildUnavailable), func(t *testing.T) {
				ctx := context.Background()
				db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
				require.NoError(t, err)
				defer db.Close()
				forum := &discordgo.Channel{ID: "forum", GuildID: gid, Name: "pokemon-releases", Type: discordgo.ChannelTypeGuildForum}
				other := &discordgo.Channel{ID: "unselected-parent", GuildID: gid, Name: "community", Type: discordgo.ChannelTypeGuildForum}
				active := &discordgo.Channel{ID: "active-parent-only", GuildID: gid, ParentID: forum.ID, Name: "new-active-pokemon", Type: discordgo.ChannelTypeGuildPublicThread, LastMessageID: "100"}
				archived := &discordgo.Channel{ID: "new-archive-not-cached", GuildID: gid, ParentID: forum.ID, Name: "new-archived-pokemon", Type: discordgo.ChannelTypeGuildPublicThread, LastMessageID: "200", ThreadMetadata: &discordgo.ThreadMetadata{Archived: true}}
				excluded := &discordgo.Channel{ID: "excluded-archive-not-cached", GuildID: gid, ParentID: forum.ID, Name: "02-11-funko", Type: discordgo.ChannelTypeGuildPublicThread, LastMessageID: "300", ThreadMetadata: &discordgo.ThreadMetadata{Archived: true}}
				private := &discordgo.Channel{ID: "private", GuildID: gid, ParentID: forum.ID, Type: discordgo.ChannelTypeGuildPrivateThread}
				f := &publicParentClient{metadataClient: &metadataClient{fakeClient: &fakeClient{
					guilds: []*discordgo.UserGuild{{ID: gid}}, guildByID: map[string]*discordgo.Guild{gid: {ID: gid}},
					channels:        map[string][]*discordgo.Channel{gid: {forum, other}},
					guildThreads:    map[string][]*discordgo.Channel{gid: {}}, // success-but-absent also needs per-parent discovery
					guildThreadErrs: map[string]error{}, activeThreads: map[string][]*discordgo.Channel{forum.ID: {active, private}},
					publicArchived: map[string][]*discordgo.Channel{forum.ID: {archived, excluded, private}},
					messages:       map[string][]*discordgo.Message{},
				}}}
				for _, ch := range []*discordgo.Channel{active, archived, excluded} {
					f.messages[ch.ID] = []*discordgo.Message{{ID: ch.LastMessageID, ChannelID: ch.ID, GuildID: gid, Content: "synthetic " + ch.ID, Timestamp: time.Now(), Author: &discordgo.User{ID: "fixture"}}}
				}
				if guildUnavailable {
					f.guildThreadErrs[gid] = errors.New("fetch threads: discord API error 403: synthetic private body not to be reported")
				}
				svc := New(f, db, nil)
				rows, err := db.Channels(ctx, gid)
				require.NoError(t, err)
				require.Empty(t, rows)
				report, err := svc.DiscoverChannels(ctx, gid, nil)
				require.NoError(t, err)
				require.Len(t, report.Channels, 2)
				require.Empty(t, f.activeParentIDs)
				report, err = svc.DiscoverChannels(ctx, gid, []string{forum.ID, forum.ID})
				require.NoError(t, err)
				require.Equal(t, []string{forum.ID}, f.activeParentIDs)
				require.Equal(t, []string{forum.ID}, f.archiveParentIDs)
				require.Zero(t, f.privateCalls)
				require.Len(t, report.Channels, 5)
				require.Empty(t, report.CachedThreads)
				require.Equal(t, "observed", report.Coverage.ParentActiveThreads[forum.ID])
				require.Equal(t, "observed", report.Coverage.PublicArchives[forum.ID])
				require.Equal(t, "not_requested", report.Coverage.PrivateArchives)
				require.False(t, report.Coverage.Complete)
				if guildUnavailable {
					require.Equal(t, "unavailable", report.Coverage.ActiveThreads)
					require.Equal(t, "http_403", report.Coverage.UnavailableReasons["guild_active"])
				}
				for _, ch := range report.Channels {
					require.NotEqual(t, private.ID, ch.ID)
				}
				require.Empty(t, f.messageCalls, "metadata phase must not fetch even a selected message")
				require.Zero(t, f.memberCalls)
				require.Zero(t, f.tailCalls)
				threadCallsBeforeSync := f.threadCalls
				// Selection itself is asserted in the runner's fixture test. Here
				// exercise the native plan boundary using the resulting explicit IDs.
				opts := SyncOptions{GuildIDs: []string{gid}, ChannelIDs: []string{forum.ID, active.ID, archived.ID}, ExactChannels: true, Full: true, Concurrency: 1}
				stats, err := svc.Sync(ctx, opts)
				require.NoError(t, err)
				require.Equal(t, 2, stats.Messages)
				require.Equal(t, threadCallsBeforeSync, f.threadCalls, "message phase cannot expand forum ancestry")
				require.Zero(t, f.messageCalls[excluded.ID])
				require.Zero(t, f.messageCalls[private.ID])
				require.Zero(t, f.messageCalls[other.ID])
				second, err := svc.DiscoverChannels(ctx, gid, []string{forum.ID})
				require.NoError(t, err)
				require.Equal(t, report.Channels, second.Channels)
				require.Empty(t, second.CachedThreads)
				stats, err = svc.Sync(ctx, opts)
				require.NoError(t, err)
				require.Zero(t, stats.Messages)
				_, count, err := db.ReadOnlyQuery(ctx, "select count(*) from messages")
				require.NoError(t, err)
				require.Equal(t, "2", count[0][0])
				require.Zero(t, f.messageCalls[excluded.ID])
				// A failed later inventory preserves existing channels, messages,
				// discovery marker and the incremental checkpoint exactly.
				marker, err := db.GetSyncState(ctx, "discovery:"+gid+":last_success")
				require.NoError(t, err)
				cursor, err := db.GetSyncState(ctx, "channel:"+archived.ID+":latest_message_id")
				require.NoError(t, err)
				require.NotEmpty(t, cursor)
				rows, err = db.Channels(ctx, gid)
				require.NoError(t, err)
				f.discoveryErr = errors.New("fixture top-level offline")
				_, err = svc.DiscoverChannels(ctx, gid, []string{forum.ID})
				require.Error(t, err)
				afterMarker, err := db.GetSyncState(ctx, "discovery:"+gid+":last_success")
				require.NoError(t, err)
				require.Equal(t, marker, afterMarker)
				afterCursor, err := db.GetSyncState(ctx, "channel:"+archived.ID+":latest_message_id")
				require.NoError(t, err)
				require.Equal(t, cursor, afterCursor)
				afterRows, err := db.Channels(ctx, gid)
				require.NoError(t, err)
				require.Equal(t, rows, afterRows)
			})
		}
	}
}

func TestDiscoveryParentDenialsKeepCachedUnconfirmedAndReasons(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	require.NoError(t, err)
	defer db.Close()
	gid := "guild"
	parent := &discordgo.Channel{ID: "forum", GuildID: gid, Type: discordgo.ChannelTypeGuildForum}
	cached := &discordgo.Channel{ID: "old-archive", GuildID: gid, ParentID: parent.ID, Type: discordgo.ChannelTypeGuildPublicThread}
	raw, err := json.Marshal(cached)
	require.NoError(t, err)
	require.NoError(t, db.UpsertChannel(ctx, toChannelRecord(cached, string(raw))))
	f := &publicParentClient{metadataClient: &metadataClient{fakeClient: &fakeClient{
		channels: map[string][]*discordgo.Channel{gid: {parent}}, guildThreads: map[string][]*discordgo.Channel{gid: {}},
		threadErrors: map[string]error{parent.ID: errors.New("discord API error 404: private response body")},
	}, archiveErr: errors.New("discord API error 403: private response body")}}
	report, err := New(f, db, nil).DiscoverChannels(ctx, gid, []string{parent.ID})
	require.NoError(t, err)
	require.Len(t, report.Channels, 1)
	require.Len(t, report.CachedThreads, 1)
	require.Equal(t, cached.ID, report.CachedThreads[0].ID)
	require.Equal(t, "unavailable", report.Coverage.ParentActiveThreads[parent.ID])
	require.Equal(t, "unavailable", report.Coverage.PublicArchives[parent.ID])
	require.Equal(t, "http_404", report.Coverage.UnavailableReasons["parent_active:"+parent.ID])
	require.Equal(t, "http_403", report.Coverage.UnavailableReasons["public_archive:"+parent.ID])
	raw, err = json.Marshal(report)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "private response body")
	require.Zero(t, f.privateCalls)
	require.Empty(t, f.messageCalls)
}

func TestDiscoveryParentScopeAndMalformedResponses(t *testing.T) {
	for _, route := range []string{"invalid-scope", "active-parent", "active-guild", "archive-parent"} {
		t.Run(route, func(t *testing.T) {
			ctx := context.Background()
			db, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
			require.NoError(t, err)
			defer db.Close()
			parent := &discordgo.Channel{ID: "forum", GuildID: "guild", Type: discordgo.ChannelTypeGuildForum}
			thread := &discordgo.Channel{ID: "thread", GuildID: "guild", ParentID: parent.ID, Type: discordgo.ChannelTypeGuildPublicThread}
			f := &publicParentClient{metadataClient: &metadataClient{fakeClient: &fakeClient{
				channels: map[string][]*discordgo.Channel{"guild": {parent}}, guildThreads: map[string][]*discordgo.Channel{"guild": {}},
				activeThreads: map[string][]*discordgo.Channel{}, publicArchived: map[string][]*discordgo.Channel{},
			}}}
			ids := []string{parent.ID}
			switch route {
			case "invalid-scope":
				ids = append(ids, "not-current")
			case "active-parent":
				thread.ParentID = "wrong"
				f.activeThreads[parent.ID] = []*discordgo.Channel{thread}
			case "active-guild":
				thread.GuildID = "wrong"
				f.activeThreads[parent.ID] = []*discordgo.Channel{thread}
			case "archive-parent":
				thread.ParentID = "wrong"
				f.publicArchived[parent.ID] = []*discordgo.Channel{thread}
			}
			_, err = New(f, db, nil).DiscoverChannels(ctx, "guild", ids)
			require.Error(t, err)
			if route == "invalid-scope" {
				require.Empty(t, f.activeParentIDs)
				require.Empty(t, f.archiveParentIDs)
			}
			rows, err := db.Channels(ctx, "guild")
			require.NoError(t, err)
			require.Empty(t, rows)
			require.Empty(t, f.messageCalls)
		})
	}
}

func TestDiscoveryFailureReasonsAreBounded(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{context.Canceled, "request_canceled"},
		{fmt.Errorf("wrapped: %w", context.DeadlineExceeded), "request_deadline_exceeded"},
		{&discordgo.RESTError{Response: &http.Response{StatusCode: 403}}, "http_403"},
		{errors.New("fetch: discord API error 401: sensitive body"), "http_401"},
		{errors.New("sensitive arbitrary transport text"), "request_failed"},
	} {
		require.Equal(t, tc.want, discoveryFailureReason(tc.err))
	}
}
