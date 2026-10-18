package memory

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Channel is the catalog subset needed for retention decisions.
type Channel struct {
	ID            string
	GuildID       string
	ParentID      string
	Kind          string
	PrivateThread bool
	ObservedAt    time.Time
}

// Retainer decides whether a message belongs to the selective memory.
type Retainer struct {
	P        *Resolved
	Channels map[string]Channel
	InfoNow  map[string]bool      // channels currently under an INFO category (public)
	InfoSeen map[string]time.Time // from memory:info_seen:* (selective DB only)
	InfoLeft map[string]time.Time // from memory:info_left:* (selective DB only)
}

// Reason labels (counts only are ever logged).
const (
	ReasonRecent     = "recent"
	ReasonInfo       = "info_active"
	ReasonInfoGrace  = "info_post_transition"
	ReasonInfoStale  = "info_stale_catalog"
	ReasonPin        = "pin_since"
	ReasonProcessed  = "pin_processed"
	ReasonDropped    = "expired"
	ReasonOutOfScope = "guild_out_of_policy"
)

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func parseStoredTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

// LoadRetainer reads catalog and memory bookkeeping from q.
func LoadRetainer(ctx context.Context, q queryer, p *Resolved) (*Retainer, error) {
	r := &Retainer{P: p, Channels: map[string]Channel{}, InfoNow: map[string]bool{}, InfoSeen: map[string]time.Time{}, InfoLeft: map[string]time.Time{}}
	rows, err := q.QueryContext(ctx, `select id, guild_id, coalesce(parent_id,''), kind, is_private_thread, updated_at from channels`)
	if err != nil {
		return nil, fmt.Errorf("read channel catalog: %w", err)
	}
	for rows.Next() {
		var c Channel
		var priv int
		var updated string
		if err := rows.Scan(&c.ID, &c.GuildID, &c.ParentID, &c.Kind, &priv, &updated); err != nil {
			_ = rows.Close()
			return nil, err
		}
		c.PrivateThread = priv != 0
		c.ObservedAt = parseStoredTime(updated)
		r.Channels[c.ID] = c
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	rows, err = q.QueryContext(ctx, `select scope, coalesce(cursor,'') from sync_state where scope like 'memory:info_%'`)
	if err != nil {
		return nil, fmt.Errorf("read memory bookkeeping: %w", err)
	}
	for rows.Next() {
		var scope, cursor string
		if err := rows.Scan(&scope, &cursor); err != nil {
			_ = rows.Close()
			return nil, err
		}
		switch {
		case strings.HasPrefix(scope, infoSeenPrefix):
			r.InfoSeen[strings.TrimPrefix(scope, infoSeenPrefix)] = parseStoredTime(cursor)
		case strings.HasPrefix(scope, infoLeftPrefix):
			r.InfoLeft[strings.TrimPrefix(scope, infoLeftPrefix)] = parseStoredTime(cursor)
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for id := range r.Channels {
		if r.underInfo(id) {
			r.InfoNow[id] = true
		}
	}
	return r, nil
}

// ValidateCatalog checks policy IDs against the catalog (fail-closed).
func (r *Retainer) ValidateCatalog() error {
	for id := range r.P.InfoCats {
		c, ok := r.Channels[id]
		if !ok {
			return fmt.Errorf("info category %s not in channel catalog", id)
		}
		if c.GuildID != r.P.InfoGuild {
			return fmt.Errorf("info category %s belongs to guild %s, not info.guild_id", id, c.GuildID)
		}
		if c.Kind != "category" && c.Kind != "guild_category" && c.Kind != "4" {
			return fmt.Errorf("info category %s has kind %q, not a category", id, c.Kind)
		}
	}
	for id, pin := range r.P.Pins {
		c, ok := r.Channels[id]
		if !ok {
			return fmt.Errorf("pinned channel %s (%s) not in channel catalog", id, pin.Slug)
		}
		if c.GuildID != pin.GuildID {
			return fmt.Errorf("pinned channel %s: registry guild %s != catalog guild %s", id, pin.GuildID, c.GuildID)
		}
	}
	return nil
}

func (r *Retainer) underInfo(id string) bool {
	seen := map[string]bool{}
	c, ok := r.Channels[id]
	if !ok || c.GuildID != r.P.InfoGuild || c.PrivateThread {
		return false
	}
	for cur := id; cur != "" && !seen[cur]; {
		seen[cur] = true
		if r.P.InfoCats[cur] {
			return true
		}
		ch, ok := r.Channels[cur]
		if !ok || ch.PrivateThread {
			return false
		}
		cur = ch.ParentID
	}
	return false
}

func (r *Retainer) stale(c Channel) bool {
	return c.ObservedAt.IsZero() || r.P.Now.Sub(c.ObservedAt) > r.P.StaleAfter
}

// Decide returns whether the message is retained and why.
func (r *Retainer) Decide(guildID, channelID, messageID, createdAt string) (bool, string) {
	if !r.P.Guilds[guildID] {
		return false, ReasonOutOfScope
	}
	if r.P.ProcessedIDs[messageID] {
		return true, ReasonProcessed
	}
	created := parseStoredTime(createdAt)
	if created.IsZero() {
		// Unparseable timestamp: never expel on doubt.
		return true, ReasonRecent
	}
	if !created.Before(r.P.Cutoff) {
		return true, ReasonRecent
	}
	if pin, ok := r.P.Pins[channelID]; ok && !created.Before(pin.Since) {
		return true, ReasonPin
	}
	if !r.P.InfoEnabled() {
		return false, ReasonDropped
	}
	if r.InfoNow[channelID] {
		return true, ReasonInfo
	}
	if _, wasInfo := r.InfoSeen[channelID]; wasInfo {
		c, ok := r.Channels[channelID]
		if !ok || r.stale(c) {
			return true, ReasonInfoStale
		}
		left, ok := r.InfoLeft[channelID]
		if !ok || r.P.Now.Before(left.Add(r.P.PostInfo)) {
			return true, ReasonInfoGrace
		}
	}
	return false, ReasonDropped
}

// InfoBookkeeping returns sync_state updates for info tracking: channels now
// under INFO get info_seen=now (and lose any info_left); channels previously
// seen under INFO that are now observed FRESH outside it get info_left set to
// that observation time. Stale observations never confirm a transition.
func (r *Retainer) InfoBookkeeping() (set map[string]string, del []string) {
	set = map[string]string{}
	if !r.P.InfoEnabled() {
		// INFO disabled: no bookkeeping is written or confirmed.
		return set, nil
	}
	now := r.P.Now.Format(time.RFC3339Nano)
	for id := range r.InfoNow {
		set[infoSeenPrefix+id] = now
		if _, ok := r.InfoLeft[id]; ok {
			del = append(del, infoLeftPrefix+id)
		}
	}
	for id := range r.InfoSeen {
		if r.InfoNow[id] {
			continue
		}
		if _, ok := r.InfoLeft[id]; ok {
			continue
		}
		c, ok := r.Channels[id]
		if !ok || r.stale(c) {
			continue
		}
		set[infoLeftPrefix+id] = c.ObservedAt.Format(time.RFC3339Nano)
	}
	return set, del
}
