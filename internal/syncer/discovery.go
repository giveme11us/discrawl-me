package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/bwmarrin/discordgo"
)

// DiscoveryReport is a CURRENT observation, not a list of every accessible
// channel ever cached. Missing IDs are not deleted from the archive.
type DiscoveryReport struct {
	GuildID       string               `json:"guild_id"`
	ObservedAt    string               `json:"observed_at"`
	Channels      []*discordgo.Channel `json:"channels"`
	CachedThreads []*discordgo.Channel `json:"cached_threads"`
	Coverage      DiscoveryCoverage    `json:"coverage"`
}
type DiscoveryCoverage struct {
	TopLevel        string            `json:"top_level"`
	ActiveThreads   string            `json:"active_threads"`
	PublicArchives  map[string]string `json:"public_archives"`
	PrivateArchives string            `json:"private_archives"`
	Complete        bool              `json:"complete"`
}

// DiscoverChannels only fetches metadata. It never fetches messages/members,
// opens Gateway, or changes message cursors. Public archive scans are opt-in
// by explicit parent ID; failures remain visible instead of claiming coverage.
func (s *Syncer) DiscoverChannels(ctx context.Context, guildID string, archiveParents []string) (DiscoveryReport, error) {
	report := DiscoveryReport{GuildID: guildID, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Channels: []*discordgo.Channel{}, CachedThreads: []*discordgo.Channel{}, Coverage: DiscoveryCoverage{TopLevel: "observed", ActiveThreads: "observed", PublicArchives: map[string]string{}, PrivateArchives: "not_requested", Complete: false}}
	if s.store == nil {
		return report, fmt.Errorf("discovery requires the existing archive store")
	}
	if guildID == "" {
		return report, fmt.Errorf("discovery requires an explicit guild")
	}
	channels, err := s.client.GuildChannels(ctx, guildID)
	if err != nil {
		return report, fmt.Errorf("discover guild channels: %w", err)
	}
	all := map[string]*discordgo.Channel{}
	add := func(ch *discordgo.Channel) error {
		if ch == nil || ch.ID == "" {
			return fmt.Errorf("invalid channel in discovery")
		}
		if ch.GuildID != "" && ch.GuildID != guildID {
			return fmt.Errorf("cross-guild discovery channel")
		}
		if ch.Type == discordgo.ChannelTypeDM || ch.Type == discordgo.ChannelTypeGroupDM || ch.Type == discordgo.ChannelTypeGuildPrivateThread {
			return nil
		}
		copy := *ch
		copy.GuildID = guildID
		all[copy.ID] = &copy
		return nil
	}
	for _, ch := range channels {
		if err := add(ch); err != nil {
			return report, err
		}
	}
	threads, err := s.client.GuildThreadsActive(ctx, guildID)
	if err != nil {
		report.Coverage.ActiveThreads = "unavailable"
	} else {
		for _, ch := range threads {
			if err := add(ch); err != nil {
				return report, err
			}
		}
	}
	for _, id := range uniqueIDs(archiveParents) {
		if !isThreadParent(all[id]) {
			return report, fmt.Errorf("archive parent %s is not a current guild text/news/forum", id)
		}
		archived, err := s.client.ThreadsArchived(ctx, id, false)
		if err != nil {
			report.Coverage.PublicArchives[id] = "unavailable"
			continue
		}
		report.Coverage.PublicArchives[id] = "observed"
		for _, ch := range archived {
			if ch == nil || ch.ParentID != id || !isThreadChannel(ch) {
				return report, fmt.Errorf("invalid archived thread parent")
			}
			if err := add(ch); err != nil {
				return report, err
			}
		}
	}
	rows, err := s.store.Channels(ctx, guildID)
	if err != nil {
		return report, err
	}
	for _, row := range rows {
		ch := channelFromRow(row)
		if _, fresh := all[ch.ID]; !fresh && isThreadChannel(ch) && ch.Type != discordgo.ChannelTypeGuildPrivateThread {
			report.CachedThreads = append(report.CachedThreads, ch)
		}
	}
	sort.Slice(report.CachedThreads, func(i, j int) bool { return report.CachedThreads[i].ID < report.CachedThreads[j].ID })
	for _, ch := range all {
		report.Channels = append(report.Channels, ch)
	}
	sort.Slice(report.Channels, func(i, j int) bool { return report.Channels[i].ID < report.Channels[j].ID })
	// No pruning/reset: a failed top-level discovery leaves prior state intact.
	for _, ch := range report.Channels {
		raw, err := json.Marshal(ch)
		if err != nil {
			return report, err
		}
		if err := s.store.UpsertChannel(ctx, toChannelRecord(ch, string(raw))); err != nil {
			return report, err
		}
	}
	if err := s.store.SetSyncState(ctx, "discovery:"+guildID+":last_success", report.ObservedAt); err != nil {
		return report, err
	}
	return report, nil
}
