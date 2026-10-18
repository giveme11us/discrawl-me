package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/giveme11us/discrawl-me/internal/config"
	"github.com/giveme11us/discrawl-me/internal/store"
)

type floorSyncService struct {
	fakeSyncService
	floorSet       bool
	attachmentSets []bool
}

func (f *floorSyncService) SetChannelSinceFloor(func(string) time.Time) { f.floorSet = true }
func (f *floorSyncService) SetAttachmentTextEnabled(enabled bool) {
	f.attachmentSets = append(f.attachmentSets, enabled)
	f.fakeSyncService.SetAttachmentTextEnabled(enabled)
}

// R1-07 + R1-02 wiring: `sync --memory-policy` disables asset download
// (attachment text) on the syncer and runs the bounded retention pass in the
// same process after sync; legacy sync leaves both untouched.
func TestSyncMemoryPolicyDisablesAssetsAndRunsRetentionPass(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "selective.db")
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	const guild, cat, ch = "100000000000000001", "100000000000000010", "100000000000000020"
	now := time.Now().UTC()
	exec := func(q string, args ...any) {
		_, err := s.DB().Exec(q, args...)
		require.NoError(t, err)
	}
	exec(`insert into channels(id, guild_id, parent_id, kind, name, raw_json, updated_at) values(?,?,?,?,?,?,?)`, cat, guild, "", "category", "INFO", "{}", now.Format(time.RFC3339Nano))
	exec(`insert into channels(id, guild_id, parent_id, kind, name, raw_json, updated_at) values(?,?,?,?,?,?,?)`, ch, guild, "", "text", "general", "{}", now.Format(time.RFC3339Nano))
	exec(`insert into sync_state(scope, cursor, updated_at) values('memory:selective', ?, ?)`, `{"version":1,"policy_sha256":"x","created_at":"`+now.AddDate(0, 0, -50).Format(time.RFC3339)+`","cutoff_utc":"x","rollback_window":"open"}`, now.Format(time.RFC3339Nano))
	exec(`insert into sync_state(scope, cursor, updated_at) values('channel:`+ch+`:latest_message_id', '300000000000000002', ?)`, now.Format(time.RFC3339Nano))
	for i, at := range []time.Time{now.AddDate(0, 0, -40), now.Add(-time.Hour)} {
		id := []string{"300000000000000001", "300000000000000002"}[i]
		exec(`insert into messages(id, guild_id, channel_id, author_id, message_type, created_at, content, normalized_content, raw_json, updated_at) values(?,?,?,?,0,?,?,?,?,?)`,
			id, guild, ch, "1", at.Format(time.RFC3339Nano), "c", "c", "{}", now.Format(time.RFC3339Nano))
	}
	write := func(name string, v any) string {
		p := filepath.Join(dir, name)
		b, _ := json.Marshal(v)
		require.NoError(t, os.WriteFile(p, b, 0o600))
		return p
	}
	reg := write("releases.json", map[string]any{"releases": []any{}})
	st := write("state.json", map[string]any{"processed": map[string]string{}})
	pol := write("policy.json", map[string]any{
		"version": 1, "guild_ids": []string{guild}, "rolling_days": 30,
		"info": map[string]any{"guild_id": guild, "category_ids": []string{cat}, "post_transition_days": 90, "catalog_stale_after_hours": 72},
		"pins": map[string]any{"registry_path": reg, "state_path": st},
	})
	cfg := config.Default()
	cfg.DBPath = dbPath

	// Legacy: no asset override, no retention pass.
	legacy := &floorSyncService{}
	rt := &runtime{ctx: ctx, cfg: cfg, store: s, syncer: legacy, stdout: &bytes.Buffer{}, logger: discardLogger(), json: true}
	require.NoError(t, rt.runSync([]string{"--guild", guild}))
	require.Empty(t, legacy.attachmentSets)
	require.False(t, legacy.floorSet)
	var n int
	require.NoError(t, s.DB().QueryRow(`select count(*) from messages`).Scan(&n))
	require.Equal(t, 2, n)

	sel := &floorSyncService{}
	rt.syncer = sel
	require.NoError(t, rt.runSync([]string{"--guild", guild, "--memory-policy", pol, "--memory-prune-max", "10"}))
	require.Equal(t, []bool{false}, sel.attachmentSets, "selective mode disables attachment download")
	require.True(t, sel.floorSet)
	require.NoError(t, s.DB().QueryRow(`select count(*) from messages`).Scan(&n))
	require.Equal(t, 2, n, "rollback window open: the 40-day-old message updated after the boundary is held, not expelled")
	var window string
	require.NoError(t, s.DB().QueryRow(`select json_extract(cursor,'$.rollback_window') from sync_state where scope='memory:selective'`).Scan(&window))
	require.Equal(t, "open", window, "sync never closes the rollback window")
	// Explicit close (separate command), then the next cycle expels it.
	exec(`update sync_state set cursor=json_set(cursor,'$.rollback_window','closed','$.rollback_window_closed_at',?) where scope='memory:selective'`, now.Format(time.RFC3339Nano))
	require.NoError(t, rt.runSync([]string{"--guild", guild, "--memory-policy", pol, "--memory-prune-max", "10"}))
	require.NoError(t, s.DB().QueryRow(`select count(*) from messages`).Scan(&n))
	require.Equal(t, 1, n, "retention pass expired the 40-day-old message in the same cycle")
	var latest string
	require.NoError(t, s.DB().QueryRow(`select cursor from sync_state where scope = ?`, "channel:"+ch+":latest_message_id").Scan(&latest))
	require.Equal(t, "300000000000000002", latest)
	require.Error(t, rt.runSync([]string{"--guild", guild, "--memory-policy", pol, "--memory-prune-max", "0"}))
}

// R4-01 on the native in-cycle pass: a window explicitly closed with a valid
// closed_at whose boundary (marker created_at) later becomes unusable must
// NOT expel anything; the same message is expelled when the boundary is
// valid (discriminating control).
func TestSyncMemoryPolicyClosedWindowUnusableBoundaryHolds(t *testing.T) {
	for _, tc := range []struct {
		name     string
		boundary string
		want     int
	}{
		{"valid boundary control", time.Now().UTC().AddDate(0, 0, -35).Format(time.RFC3339), 0},
		{"unusable boundary", "not-a-time", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "selective.db")
			s, err := store.Open(ctx, dbPath)
			require.NoError(t, err)
			defer func() { _ = s.Close() }()
			const guild, cat, ch, id = "100000000000000001", "100000000000000010", "100000000000000020", "300000000000000001"
			now := time.Now().UTC()
			exec := func(q string, args ...any) {
				_, err := s.DB().Exec(q, args...)
				require.NoError(t, err)
			}
			exec(`insert into channels(id, guild_id, parent_id, kind, name, raw_json, updated_at) values(?,?,?,?,?,?,?)`, cat, guild, "", "category", "INFO", "{}", now.Format(time.RFC3339Nano))
			exec(`insert into channels(id, guild_id, parent_id, kind, name, raw_json, updated_at) values(?,?,?,?,?,?,?)`, ch, guild, "", "text", "general", "{}", now.Format(time.RFC3339Nano))
			marker, _ := json.Marshal(map[string]any{"version": 1, "policy_sha256": "x", "created_at": tc.boundary, "cutoff_utc": "x", "rollback_window": "closed", "rollback_window_closed_at": now.Add(-time.Hour).Format(time.RFC3339Nano)})
			exec(`insert into sync_state(scope, cursor, updated_at) values('memory:selective', ?, ?)`, string(marker), now.Format(time.RFC3339Nano))
			exec(`insert into sync_state(scope, cursor, updated_at) values('channel:`+ch+`:latest_message_id', ?, ?)`, id, now.Format(time.RFC3339Nano))
			old := now.AddDate(0, 0, -40).Format(time.RFC3339Nano)
			exec(`insert into messages(id, guild_id, channel_id, author_id, message_type, created_at, content, normalized_content, raw_json, updated_at) values(?,?,?,?,0,?,?,?,?,?)`,
				id, guild, ch, "1", old, "c", "c", "{}", old)
			exec(`insert into message_attachments(attachment_id, message_id, guild_id, channel_id, filename, updated_at) values(?,?,?,?,'a.png',?)`, "a"+id, id, guild, ch, old)
			write := func(name string, v any) string {
				p := filepath.Join(dir, name)
				b, _ := json.Marshal(v)
				require.NoError(t, os.WriteFile(p, b, 0o600))
				return p
			}
			reg := write("releases.json", map[string]any{"releases": []any{}})
			st := write("state.json", map[string]any{"processed": map[string]string{}})
			pol := write("policy.json", map[string]any{
				"version": 1, "guild_ids": []string{guild}, "rolling_days": 30,
				"info": map[string]any{"guild_id": guild, "category_ids": []string{cat}, "post_transition_days": 90, "catalog_stale_after_hours": 72},
				"pins": map[string]any{"registry_path": reg, "state_path": st},
			})
			cfg := config.Default()
			cfg.DBPath = dbPath
			rt := &runtime{ctx: ctx, cfg: cfg, store: s, syncer: &floorSyncService{}, stdout: &bytes.Buffer{}, logger: discardLogger(), json: true}
			require.NoError(t, rt.runSync([]string{"--guild", guild, "--memory-policy", pol, "--memory-prune-max", "10"}))
			var msgs, atts int
			require.NoError(t, s.DB().QueryRow(`select count(*) from messages`).Scan(&msgs))
			require.NoError(t, s.DB().QueryRow(`select count(*) from message_attachments`).Scan(&atts))
			require.Equal(t, tc.want, msgs)
			require.Equal(t, tc.want, atts)
			var latest, window string
			require.NoError(t, s.DB().QueryRow(`select cursor from sync_state where scope = ?`, "channel:"+ch+":latest_message_id").Scan(&latest))
			require.Equal(t, id, latest, "cursor preserved")
			require.NoError(t, s.DB().QueryRow(`select json_extract(cursor,'$.rollback_window') from sync_state where scope='memory:selective'`).Scan(&window))
			require.Equal(t, "closed", window, "marker not rewritten by sync")
		})
	}
}
