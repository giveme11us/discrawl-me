// Package memory implements the opt-in "selective memory" mode of discrawl-me:
// a compatible SQLite file (same schema, same path after adoption) that keeps
// the full catalog and every sync cursor, but only the messages selected by an
// explicit, validated retention policy (policy JSON v1, docs/selective-memory.md).
//
// Nothing in this package is active unless a policy is passed explicitly.
// Source archives are only ever opened with mode=ro + query_only.
package memory

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

// PolicyVersion is the only accepted policy schema version.
const PolicyVersion = 1

// MarkerScope is the sync_state scope that marks a DB as a selective memory.
// Prune and the native retention writer refuse any DB without it.
const MarkerScope = "memory:selective"

// Info channel bookkeeping scopes (sync_state, never touch real cursors).
const (
	infoSeenPrefix = "memory:info_seen:"
	infoLeftPrefix = "memory:info_left:"
)

var snowflakeRE = regexp.MustCompile(`^[1-9][0-9]{16,19}$`)

// Policy is the on-disk policy JSON v1.
type Policy struct {
	Version     int      `json:"version"`
	GuildIDs    []string `json:"guild_ids"`
	RollingDays int      `json:"rolling_days"`
	// Info and Pins are optional: omitted (or null) = disabled. When present
	// they are validated exactly as before (fail-closed).
	Info *InfoPolicy `json:"info,omitempty"`
	Pins *PinPolicy  `json:"pins,omitempty"`
}

// InfoPolicy keeps whole INFO rooms while they stay under the listed
// categories, then PostTransitionDays after a confirmed move.
type InfoPolicy struct {
	GuildID               string   `json:"guild_id"`
	CategoryIDs           []string `json:"category_ids"`
	PostTransitionDays    int      `json:"post_transition_days"`
	CatalogStaleAfterHour int      `json:"catalog_stale_after_hours"`
}

// PinPolicy points at the copy-learning registry (read-only).
type PinPolicy struct {
	RegistryPath string `json:"registry_path"`
	StatePath    string `json:"state_path"`
}

// Pin is one registered release channel kept from Since.
type Pin struct {
	Slug      string
	GuildID   string
	ChannelID string
	Since     time.Time
}

// Resolved is a validated policy bound to a concrete UTC evaluation time.
type Resolved struct {
	Policy        Policy
	PolicySHA256  string
	RegistrySHA   string
	StateSHA      string
	Now           time.Time
	Cutoff        time.Time
	Guilds        map[string]bool
	InfoGuild     string
	InfoCats      map[string]bool
	PostInfo      time.Duration
	StaleAfter    time.Duration
	Pins          map[string]Pin // by channel id
	ProcessedIDs  map[string]bool
	ProcessedList []string
}

type registryFile struct {
	Releases []struct {
		Slug      string `json:"slug"`
		GuildID   string `json:"guild_id"`
		ChannelID string `json:"channel_id"`
		Since     string `json:"since"`
	} `json:"releases"`
}

type stateFile struct {
	Processed map[string]string `json:"processed"`
}

func decodeStrict(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing data after JSON value")
	}
	return nil
}

func sha(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// LoadPolicy reads and validates a policy, its registry and state. Any error is
// fatal: callers must not fall back to a partial policy.
func LoadPolicy(path string, now time.Time) (*Resolved, error) {
	if now.IsZero() {
		return nil, errors.New("policy evaluation time is required")
	}
	now = now.UTC()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read policy: %w", err)
	}
	var p Policy
	if err := decodeStrict(data, &p); err != nil {
		return nil, fmt.Errorf("invalid policy JSON: %w", err)
	}
	if p.Version != PolicyVersion {
		return nil, fmt.Errorf("unsupported policy version %d (want %d)", p.Version, PolicyVersion)
	}
	r := &Resolved{Policy: p, PolicySHA256: sha(data), Now: now, Guilds: map[string]bool{}, InfoCats: map[string]bool{}, Pins: map[string]Pin{}, ProcessedIDs: map[string]bool{}}
	if len(p.GuildIDs) == 0 {
		return nil, errors.New("policy guild_ids is empty")
	}
	for _, id := range p.GuildIDs {
		if !snowflakeRE.MatchString(id) {
			return nil, fmt.Errorf("policy guild id %q is not a Discord snowflake", id)
		}
		if r.Guilds[id] {
			return nil, fmt.Errorf("duplicate guild id %s", id)
		}
		r.Guilds[id] = true
	}
	if p.RollingDays < 1 || p.RollingDays > 366 {
		return nil, fmt.Errorf("rolling_days %d out of range 1..366", p.RollingDays)
	}
	r.Cutoff = now.Add(-time.Duration(p.RollingDays) * 24 * time.Hour)
	if p.Info != nil {
		if err := r.resolveInfo(p.Info); err != nil {
			return nil, err
		}
	}
	if p.Pins != nil {
		if err := r.resolvePins(p.Pins); err != nil {
			return nil, err
		}
	}
	sort.Strings(r.ProcessedList)
	return r, nil
}

// InfoEnabled reports whether the policy declares an INFO section.
func (r *Resolved) InfoEnabled() bool { return r.InfoGuild != "" }

// resolveInfo validates an explicit INFO section (fail-closed).
func (r *Resolved) resolveInfo(info *InfoPolicy) error {
	if !snowflakeRE.MatchString(info.GuildID) || !r.Guilds[info.GuildID] {
		return fmt.Errorf("info.guild_id %q must be one of guild_ids", info.GuildID)
	}
	r.InfoGuild = info.GuildID
	if len(info.CategoryIDs) == 0 {
		return errors.New("info.category_ids is empty")
	}
	for _, id := range info.CategoryIDs {
		if !snowflakeRE.MatchString(id) {
			return fmt.Errorf("info category id %q is not a Discord snowflake", id)
		}
		r.InfoCats[id] = true
	}
	if info.PostTransitionDays < 1 {
		return errors.New("info.post_transition_days must be >= 1")
	}
	r.PostInfo = time.Duration(info.PostTransitionDays) * 24 * time.Hour
	if info.CatalogStaleAfterHour < 1 {
		return errors.New("info.catalog_stale_after_hours must be >= 1")
	}
	r.StaleAfter = time.Duration(info.CatalogStaleAfterHour) * time.Hour
	return nil
}

// resolvePins validates an explicit pins section; registry and state are
// only read (sha256 kept for reports).
func (r *Resolved) resolvePins(pins *PinPolicy) error {
	if pins.RegistryPath == "" || pins.StatePath == "" {
		return errors.New("pins.registry_path and pins.state_path are required")
	}
	reg, err := os.ReadFile(pins.RegistryPath)
	if err != nil {
		return fmt.Errorf("read pin registry: %w", err)
	}
	r.RegistrySHA = sha(reg)
	var rf registryFile
	// The registry carries many other fields (report_dir, drafts...): only the
	// keys we need are decoded, but they are validated strictly.
	if err := json.Unmarshal(reg, &rf); err != nil {
		return fmt.Errorf("invalid pin registry JSON: %w", err)
	}
	if rf.Releases == nil {
		return errors.New("pin registry has no releases array")
	}
	for i, rel := range rf.Releases {
		if !snowflakeRE.MatchString(rel.ChannelID) || !snowflakeRE.MatchString(rel.GuildID) {
			return fmt.Errorf("pin registry release %d: invalid guild/channel id", i)
		}
		if !r.Guilds[rel.GuildID] {
			return fmt.Errorf("pin registry release %d: guild %s not in policy guild_ids", i, rel.GuildID)
		}
		since, err := parseUTC(rel.Since)
		if err != nil {
			return fmt.Errorf("pin registry release %d: since: %w", i, err)
		}
		if prev, ok := r.Pins[rel.ChannelID]; ok {
			if prev.GuildID != rel.GuildID {
				return fmt.Errorf("pin registry: channel %s registered in two guilds", rel.ChannelID)
			}
			if since.Before(prev.Since) {
				prev.Since = since
				r.Pins[rel.ChannelID] = prev
			}
			continue
		}
		r.Pins[rel.ChannelID] = Pin{Slug: rel.Slug, GuildID: rel.GuildID, ChannelID: rel.ChannelID, Since: since}
	}
	st, err := os.ReadFile(pins.StatePath)
	if err != nil {
		return fmt.Errorf("read pin state: %w", err)
	}
	r.StateSHA = sha(st)
	sf, err := decodeState(st)
	if err != nil {
		return err
	}
	for id := range sf.Processed {
		if !snowflakeRE.MatchString(id) {
			return fmt.Errorf("pin state processed id %q is not a snowflake", id)
		}
		r.ProcessedIDs[id] = true
		r.ProcessedList = append(r.ProcessedList, id)
	}
	return nil
}

// decodeState validates the copy-learning state shape fail-closed: the
// "processed" key is mandatory and must be a JSON object of id -> string hash.
// A missing, null or mistyped field is an error, never a revocation of pins.
func decodeState(st []byte) (*stateFile, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(st, &top); err != nil {
		return nil, fmt.Errorf("invalid pin state JSON: %w", err)
	}
	if top == nil {
		return nil, errors.New("invalid pin state: top-level value must be a JSON object")
	}
	raw, ok := top["processed"]
	if !ok {
		return nil, errors.New("invalid pin state: required field \"processed\" is missing")
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errors.New("invalid pin state: \"processed\" must be a JSON object (null/array/scalar refused)")
	}
	var processed map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &processed); err != nil {
		return nil, fmt.Errorf("invalid pin state processed: %w", err)
	}
	sf := &stateFile{Processed: map[string]string{}}
	for id, v := range processed {
		var hash string
		if err := json.Unmarshal(v, &hash); err != nil || hash == "" {
			return nil, fmt.Errorf("invalid pin state: processed[%q] must be a non-empty string", id)
		}
		sf.Processed[id] = hash
	}
	return sf, nil
}

// parseUTC accepts only full RFC3339 timestamps with an explicit UTC offset
// (Z or +00:00). Day-month strings and local offsets are rejected.
func parseUTC(value string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is not RFC3339", value)
	}
	if !(strings.HasSuffix(value, "Z") || strings.HasSuffix(value, "+00:00")) {
		return time.Time{}, fmt.Errorf("%q is not UTC", value)
	}
	return t.UTC(), nil
}

// ParseNow parses an explicit --now override (UTC only).
func ParseNow(value string) (time.Time, error) {
	if value == "" {
		return time.Now().UTC(), nil
	}
	return parseUTC(value)
}
