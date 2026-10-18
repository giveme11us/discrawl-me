package syncer

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/giveme11us/discrawl-me/internal/store"
)

// pagedArchiveClient serves public archives page by page (fake, no network).
type pagedArchiveClient struct {
	*fakeClient
	pages        map[string][][]*discordgo.Channel // parent -> pages (newest first)
	calls        []string                          // parent|before
	failAt       map[string]int                    // parent -> call index (0-based) that fails
	privateCalls int
}

func (p *pagedArchiveClient) ThreadsArchived(_ context.Context, _ string, private bool) ([]*discordgo.Channel, error) {
	if private {
		p.privateCalls++
	}
	return nil, errors.New("monolithic archive call must not be used when paging is enabled")
}

func (p *pagedArchiveClient) ThreadsArchivedPage(_ context.Context, parent, before string, _ int) ([]*discordgo.Channel, bool, string, error) {
	n := 0
	for _, c := range p.calls {
		if len(c) > len(parent) && c[:len(parent)] == parent {
			n++
		}
	}
	p.calls = append(p.calls, parent+"|"+before)
	if at, ok := p.failAt[parent]; ok && at == n {
		return nil, false, "", context.DeadlineExceeded
	}
	idx := 0
	if before != "" {
		_, err := fmt.Sscanf(before, "page-%d", &idx)
		if err != nil {
			return nil, false, "", err
		}
	}
	pages := p.pages[parent]
	if idx >= len(pages) {
		return nil, false, "", nil
	}
	more := idx+1 < len(pages)
	next := ""
	if more {
		next = fmt.Sprintf("page-%d", idx+1)
	}
	return pages[idx], more, next, nil
}

// R1-03: a parent whose public archive is longer than the per-run bound makes
// DURABLE progress page by page across runs (cursor in sync_state, threads
// upserted per page), an interruption keeps completed pages, the next parent
// is still scanned in the same run, private threads are never stored and the
// private archive is never requested.
func TestDiscoveryArchivePagesResumeAcrossRuns(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	const guild, slow, next = "g1", "500000000000000001", "500000000000000002"
	thread := func(parent string, n int, typ discordgo.ChannelType) *discordgo.Channel {
		return &discordgo.Channel{
			ID: fmt.Sprintf("7%s%016d", parent[17:], n), GuildID: guild, ParentID: parent, Name: fmt.Sprintf("t%d", n), Type: typ,
			ThreadMetadata: &discordgo.ThreadMetadata{Archived: true, ArchiveTimestamp: time.Unix(int64(1_700_000_000-n), 0).UTC()},
		}
	}
	var slowPages [][]*discordgo.Channel
	for p := 0; p < 5; p++ {
		page := []*discordgo.Channel{thread(slow, p*10+1, discordgo.ChannelTypeGuildPublicThread), thread(slow, p*10+2, discordgo.ChannelTypeGuildPublicThread)}
		if p == 1 {
			page = append(page, thread(slow, p*10+3, discordgo.ChannelTypeGuildPrivateThread))
		}
		slowPages = append(slowPages, page)
	}
	base := &fakeClient{
		guilds:    []*discordgo.UserGuild{{ID: guild, Name: "G"}},
		guildByID: map[string]*discordgo.Guild{guild: {ID: guild, Name: "G"}},
		channels: map[string][]*discordgo.Channel{guild: {
			{ID: slow, GuildID: guild, Name: "slow-forum", Type: discordgo.ChannelTypeGuildForum},
			{ID: next, GuildID: guild, Name: "next-forum", Type: discordgo.ChannelTypeGuildForum},
		}},
	}
	client := &pagedArchiveClient{fakeClient: base, pages: map[string][][]*discordgo.Channel{
		slow: slowPages,
		next: {{thread(next, 1, discordgo.ChannelTypeGuildPublicThread)}},
	}}
	svc := New(client, s, nil)
	svc.SetArchivePagesPerParent(2)
	stored := func(parent string) int {
		var n int
		require.NoError(t, s.DB().QueryRow(`select count(*) from channels where parent_id = ?`, parent).Scan(&n))
		return n
	}
	cursor := func() string {
		v, err := s.GetSyncState(ctx, archiveCursorScope(guild, slow))
		require.NoError(t, err)
		return v
	}

	// Run 1: slow parent bounded to 2 pages -> partial, cursor persisted;
	// next parent still scanned in the same run.
	rep, err := svc.DiscoverChannels(ctx, guild, []string{slow, next})
	require.NoError(t, err)
	require.Equal(t, "partial", rep.Coverage.PublicArchives[slow])
	require.Equal(t, "observed", rep.Coverage.PublicArchives[next])
	require.Equal(t, "page-2", cursor())
	require.Equal(t, 4, stored(slow), "pages 1-2 public threads stored, private skipped")
	require.Equal(t, 1, stored(next))

	// Run 2: interrupted on its 2nd page request (budget/kill-like error):
	// page 3 progress is kept, nothing restarts from page 1.
	client.failAt = map[string]int{slow: 3}
	rep, err = svc.DiscoverChannels(ctx, guild, []string{slow, next})
	require.NoError(t, err)
	require.Equal(t, "unavailable", rep.Coverage.PublicArchives[slow])
	require.Equal(t, "request_deadline_exceeded", rep.Coverage.UnavailableReasons["public_archive:"+slow])
	require.Equal(t, "page-3", cursor(), "completed page 3 is durable")
	require.Equal(t, 6, stored(slow))

	// Run 3: resumes from page 4 and completes the pass; cursor cleared.
	client.failAt = nil
	rep, err = svc.DiscoverChannels(ctx, guild, []string{slow, next})
	require.NoError(t, err)
	require.Equal(t, "observed", rep.Coverage.PublicArchives[slow])
	require.Equal(t, "", cursor())
	require.Equal(t, 10, stored(slow))
	var privateStored int
	require.NoError(t, s.DB().QueryRow(`select count(*) from channels where kind like '%private%' or is_private_thread = 1`).Scan(&privateStored))
	require.Zero(t, privateStored)
	require.Zero(t, client.privateCalls)
	// Never restarted from page 1 before the pass completed.
	var firstPageCalls int
	for _, c := range client.calls {
		if c == slow+"|" {
			firstPageCalls++
		}
	}
	require.Equal(t, 1, firstPageCalls)
}
