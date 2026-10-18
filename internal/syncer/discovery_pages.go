package syncer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bwmarrin/discordgo"
)

// archivedPager is implemented by clients that can fetch ONE page of public
// archived threads (userclient). Paging lets discovery checkpoint progress
// durably instead of restarting a long archive from page 1 every cycle.
type archivedPager interface {
	ThreadsArchivedPage(ctx context.Context, channelID, before string, limit int) ([]*discordgo.Channel, bool, string, error)
}

// SetArchivePagesPerParent bounds public archive pages per parent per
// discovery run (0 = legacy unbounded single call). With a bound, the page
// cursor is persisted after every page and the next run resumes from it.
func (s *Syncer) SetArchivePagesPerParent(n int) {
	if n < 0 {
		n = 0
	}
	s.archivePagesPerParent = n
}

func archiveCursorScope(guildID, parentID string) string {
	return "discovery:" + guildID + ":public_archive:" + parentID + ":before"
}

type archivePageStatus struct {
	coverage string // observed (pass complete) | partial (cursor kept) | unavailable
	reason   string
}

// discoverArchivePages scans at most archivePagesPerParent pages of ONE
// explicit parent's PUBLIC archive, resuming from the persisted cursor. Each
// page's threads are validated (same parent, thread type, same guild) and
// upserted immediately, then the cursor is persisted, so an interruption
// (budget kill) never loses completed pages. A full pass clears the cursor so
// the next rotation starts again from the newest archive.
func (s *Syncer) discoverArchivePages(ctx context.Context, guildID, parentID string, pager archivedPager, add func(*discordgo.Channel) error) (archivePageStatus, error) {
	scope := archiveCursorScope(guildID, parentID)
	before, err := s.store.GetSyncState(ctx, scope)
	if err != nil {
		return archivePageStatus{}, err
	}
	for page := 0; page < s.archivePagesPerParent; page++ {
		threads, more, next, err := pager.ThreadsArchivedPage(ctx, parentID, before, 100)
		if err != nil {
			// Cursor unchanged: the failed page is retried next run.
			return archivePageStatus{coverage: "unavailable", reason: discoveryFailureReason(err)}, nil
		}
		for _, ch := range threads {
			if ch == nil || ch.ParentID != parentID || !isThreadChannel(ch) {
				return archivePageStatus{}, fmt.Errorf("invalid archived thread parent")
			}
			if ch.Type == discordgo.ChannelTypeGuildPrivateThread {
				continue
			}
			if err := add(ch); err != nil {
				return archivePageStatus{}, err
			}
			row := *ch
			row.GuildID = guildID
			raw, err := json.Marshal(&row)
			if err != nil {
				return archivePageStatus{}, err
			}
			if err := s.store.UpsertChannel(ctx, toChannelRecord(&row, string(raw))); err != nil {
				return archivePageStatus{}, err
			}
		}
		if !more {
			if err := s.store.SetSyncState(ctx, scope, ""); err != nil {
				return archivePageStatus{}, err
			}
			return archivePageStatus{coverage: "observed"}, nil
		}
		if err := s.store.SetSyncState(ctx, scope, next); err != nil {
			return archivePageStatus{}, err
		}
		before = next
	}
	return archivePageStatus{coverage: "partial"}, nil
}
