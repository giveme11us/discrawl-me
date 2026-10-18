package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/giveme11us/discrawl-me/internal/memory"
	"github.com/giveme11us/discrawl-me/internal/store"
)

// R1-08(a) + R1-07: one integrated synthetic flow
//
//	legacy full archive (fake client) -> memory export -> memory prune ->
//	real writer policy (InstallWriterRetention) -> native sync (fake client)
//
// with: no reimport of history, no fake history_complete, a channel that only
// has messages below the floor, stop/resume after an injected failure, and a
// LOCAL-ONLY HTTP server counting attachment GETs (0 in selective mode).
func TestSelectiveEndToEndExportPruneWriterSync(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Now().UTC()

	var assetGETs atomic.Int64
	asset := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		assetGETs.Add(1)
		_, _ = w.Write([]byte("synthetic attachment text"))
	}))
	defer asset.Close()

	const (
		guild = "100000000000000001"
		cat   = "100000000000000010"
		gen   = "100000000000000020"
		info  = "100000000000000030"
		lone  = "100000000000000040" // appears later, only messages below floor
	)
	id := func(n int) string { return fmt.Sprintf("%018d", 300000000000000000+n) }
	author := &discordgo.User{ID: "200000000000000001", Username: "synthetic"}
	msg := func(ch string, n int, at time.Time) *discordgo.Message {
		return &discordgo.Message{ID: id(n), GuildID: guild, ChannelID: ch, Content: fmt.Sprintf("m-%d", n), Timestamp: at, Author: author}
	}
	withAttachment := func(m *discordgo.Message) *discordgo.Message {
		m.Attachments = []*discordgo.MessageAttachment{{ID: "9" + m.ID[1:], Filename: "notes.txt", ContentType: "text/plain", Size: 10, URL: asset.URL + "/" + m.ID}}
		return m
	}
	// newest first, as Discord returns them
	var genMsgs []*discordgo.Message
	for n := 1200; n >= 1001; n-- {
		at := now.Add(-time.Duration(1200-n) * time.Hour) // n>1100: recent (<100h)
		if n <= 1100 {
			at = now.AddDate(0, 0, -60).Add(-time.Duration(1100-n) * time.Hour)
		}
		m := msg(gen, n, at)
		if n == 1150 || n == 1050 { // one retained, one dropped
			m = withAttachment(m)
		}
		genMsgs = append(genMsgs, m)
	}
	var infoMsgs []*discordgo.Message
	for n := 2005; n >= 2001; n-- {
		infoMsgs = append(infoMsgs, msg(info, n, now.AddDate(0, 0, -200).Add(time.Duration(n)*time.Minute)))
	}
	client := &fakeClient{
		guilds:    []*discordgo.UserGuild{{ID: guild, Name: "Synthetic"}},
		guildByID: map[string]*discordgo.Guild{guild: {ID: guild, Name: "Synthetic"}},
		channels: map[string][]*discordgo.Channel{guild: {
			{ID: cat, GuildID: guild, Name: "INFO", Type: discordgo.ChannelTypeGuildCategory},
			{ID: gen, GuildID: guild, Name: "general", Type: discordgo.ChannelTypeGuildText, LastMessageID: id(1200)},
			{ID: info, GuildID: guild, Name: "rules", ParentID: cat, Type: discordgo.ChannelTypeGuildText, LastMessageID: id(2005)},
		}},
		messages: map[string][]*discordgo.Message{gen: genMsgs, info: infoMsgs},
	}

	// 1) Legacy full archive. Control: legacy mode DOES fetch attachment text,
	// so the local counter is able to observe a download.
	srcPath := filepath.Join(dir, "source.db")
	src, err := store.Open(ctx, srcPath)
	require.NoError(t, err)
	_, err = New(client, src, nil).Sync(ctx, SyncOptions{Full: true, Concurrency: 1})
	require.NoError(t, err)
	require.NoError(t, src.Close())
	require.Equal(t, int64(2), assetGETs.Load(), "control: legacy writer fetches both attachments")
	assetGETs.Store(0)

	// 2) Policy (no pins registered, processed present and empty).
	registry := filepath.Join(dir, "releases.json")
	state := filepath.Join(dir, "state.json")
	policy := filepath.Join(dir, "policy.json")
	writeTestJSON(t, registry, map[string]any{"releases": []any{}})
	writeTestJSON(t, state, map[string]any{"processed": map[string]string{}})
	writeTestJSON(t, policy, map[string]any{
		"version": 1, "guild_ids": []string{guild}, "rolling_days": 30,
		"info": map[string]any{"guild_id": guild, "category_ids": []string{cat}, "post_transition_days": 90, "catalog_stale_after_hours": 72},
		"pins": map[string]any{"registry_path": registry, "state_path": state},
	})
	selPath := filepath.Join(dir, "selective.db")
	exp, err := memory.Export(ctx, memory.ExportOptions{Source: srcPath, Output: selPath, PolicyPath: policy, Now: now})
	require.NoError(t, err)
	require.Equal(t, int64(100+5), exp.OutputRows["messages"], "100 recent general + 5 INFO")
	_, err = memory.Prune(ctx, selPath, policy, now, false, false)
	require.NoError(t, err)

	// 3) Native writer with the real policy on the selective DB.
	sel, err := store.Open(ctx, selPath)
	require.NoError(t, err)
	defer func() { _ = sel.Close() }()
	cursorsBefore := syncStateDigest(t, sel)
	w, err := memory.InstallWriterRetention(ctx, sel, policy, now)
	require.NoError(t, err)
	svc := New(client, sel, nil)
	svc.SetChannelSinceFloor(w.SinceFloor)
	svc.SetAttachmentTextEnabled(false) // what `sync --memory-policy` does

	// New activity: 5 new recent messages in general and a new channel whose
	// history is entirely below the floor. The lone channel fails first
	// (injected stop), then resumes on the next cycle.
	for n := 1205; n >= 1201; n-- {
		client.messages[gen] = append([]*discordgo.Message{msg(gen, n, now.Add(-time.Duration(1205-n)*time.Minute))}, client.messages[gen]...)
	}
	client.channels[guild][1].LastMessageID = id(1205)
	var loneMsgs []*discordgo.Message
	for n := 3050; n >= 3001; n-- {
		m := msg(lone, n, now.AddDate(0, 0, -90).Add(time.Duration(n)*time.Minute))
		if n == 3010 {
			m = withAttachment(m)
		}
		loneMsgs = append(loneMsgs, m)
	}
	client.channels[guild] = append(client.channels[guild], &discordgo.Channel{ID: lone, GuildID: guild, Name: "old-room", Type: discordgo.ChannelTypeGuildText, LastMessageID: id(3050)})
	client.messages[lone] = loneMsgs
	client.messageErrors = map[string]error{lone: errors.New("injected stop")}

	_, _ = svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1}) // interrupted cycle
	client.messageErrors = nil
	_, err = svc.Sync(ctx, SyncOptions{Full: true, Concurrency: 1}) // resume
	require.NoError(t, err)

	count := func(q string, args ...any) int {
		var n int
		require.NoError(t, sel.DB().QueryRow(q, args...).Scan(&n))
		return n
	}
	require.Equal(t, 0, count(`select count(*) from messages where channel_id = ? and created_at < ?`, gen, now.AddDate(0, 0, -30).Format(time.RFC3339Nano)), "no old general history reimported")
	require.Equal(t, 105, count(`select count(*) from messages where channel_id = ?`, gen))
	require.Equal(t, 5, count(`select count(*) from messages where channel_id = ?`, info), "INFO room kept whole")
	require.Equal(t, 0, count(`select count(*) from messages where channel_id = ?`, lone), "below-floor channel imports nothing")
	require.Equal(t, 0, count(`select count(*) from embedding_jobs`))
	require.Equal(t, 0, count(`select count(*) from message_embeddings`))
	// general: history_complete was legitimately set by the legacy full
	// archive and is preserved by export (owner decision 1), unchanged.
	// lone: first seen in selective mode, bounded at the floor -> never set.
	genComplete, err := sel.GetSyncState(ctx, channelHistoryCompleteScope(gen))
	require.NoError(t, err)
	require.Contains(t, cursorsBefore, channelHistoryCompleteScope(gen)+"="+genComplete, "inherited cursor preserved")
	loneComplete, err := sel.GetSyncState(ctx, channelHistoryCompleteScope(lone))
	require.NoError(t, err)
	require.Empty(t, loneComplete, "bounded backfill must not fake history_complete")
	loneBackfill, err := sel.GetSyncState(ctx, channelBackfillScope(lone))
	require.NoError(t, err)
	require.Empty(t, loneBackfill, "no backfill cursor invented below the floor")
	loneLatest, err := sel.GetSyncState(ctx, channelLatestScope(lone))
	require.NoError(t, err)
	require.Empty(t, loneLatest, "legacy since-bootstrap semantics: page below floor, nothing stored, re-checked next cycle with one request")
	latest, err := sel.GetSyncState(ctx, channelLatestScope(gen))
	require.NoError(t, err)
	require.Equal(t, id(1205), latest)
	// Attachment metadata kept only for the retained message, text never fetched.
	require.Equal(t, 1, count(`select count(*) from message_attachments`))
	// The single row is the one inherited from the legacy archive by export (its text was
	// fetched by the LEGACY writer before; nothing new is fetched in selective mode).
	require.Equal(t, int64(0), assetGETs.Load(), "selective mode: zero asset GETs (retained and dropped)")

	// 4) R2-03: REAL expulsion. The clock moves forward so that messages
	// retained at export time (general n=1101..1151, older than 48h at
	// export) fall out of the 30-day window, including n=1150 with its
	// attachment child. The in-cycle retention pass must delete exactly
	// those, keep INFO and the 5 new ones, and leave every cursor intact;
	// the following native syncs (one interrupted, one resumed) must not
	// fetch them again nor fake history_complete.
	later := now.AddDate(0, 0, 30).Add(-48 * time.Hour)
	pol, err := memory.LoadPolicy(policy, later)
	require.NoError(t, err)
	cursorsPrePrune := syncStateDigest(t, sel)
	var expelled []string
	for n := 1101; n <= 1151; n++ {
		expelled = append(expelled, id(n))
	}
	require.Equal(t, 1, count(`select count(*) from message_attachments where message_id = ?`, id(1150)), "precondition: retained child")
	pr, err := memory.PruneDB(ctx, sel.DB(), selPath, pol, false, false, 5000)
	require.NoError(t, err)
	// Decision A: while the rollback window is OPEN, those messages were
	// re-upserted by the native writer after the boundary (step 3 updated
	// updated_at and appended upsert events), so they are HELD, not expelled.
	require.Equal(t, int64(0), pr.Deleted["messages"], "window open: post-boundary-touched messages are held")
	require.Equal(t, int64(len(expelled)), pr.RollbackWindow.HeldMessages)
	require.Equal(t, "open", pr.RollbackWindow.State)
	require.Equal(t, cursorsPrePrune, syncStateDigest(t, sel), "hold pass keeps every cursor")
	// Explicit close (after the final yes in the runbook), then the same
	// retention pass expels them for real.
	st, err := memory.Status(ctx, selPath, "", later)
	require.NoError(t, err)
	_, err = memory.CloseRollbackWindow(ctx, selPath, st.RollbackWindow.Boundary, later)
	require.NoError(t, err)
	pr, err = memory.PruneDB(ctx, sel.DB(), selPath, pol, false, false, 5000)
	require.NoError(t, err)
	require.Equal(t, int64(len(expelled)), pr.ExpelledPostBoundary, "expulsion after close is recorded for the reconcile guard")
	require.Equal(t, int64(len(expelled)), pr.Deleted["messages"], "exactly the aged-out retained messages are expelled")
	require.Equal(t, int64(1), pr.Deleted["message_attachments"], "with their children")
	require.Equal(t, pr.SyncStateBef, pr.SyncStateAft)
	for _, e := range expelled {
		require.Equal(t, 0, count(`select count(*) from messages where id = ?`, e), e)
	}
	require.Equal(t, 0, count(`select count(*) from message_attachments`))
	require.Equal(t, 105-len(expelled), count(`select count(*) from messages where channel_id = ?`, gen))
	require.Equal(t, 5, count(`select count(*) from messages where channel_id = ?`, info), "INFO room still kept whole")
	require.Equal(t, cursorsPrePrune, syncStateDigest(t, sel), "prune keeps every channel cursor byte-identical")

	// Native writer at the new clock; requests are recorded on the fake.
	w2, err := memory.InstallWriterRetention(ctx, sel, policy, later)
	require.NoError(t, err)
	rec := &recordingClient{fakeClient: client}
	svc2 := New(rec, sel, nil)
	svc2.SetChannelSinceFloor(w2.SinceFloor)
	svc2.SetAttachmentTextEnabled(false)
	// One new message makes general due for a forward poll again, so the
	// assertions below observe real requests on the pruned channel.
	client.messages[gen] = append([]*discordgo.Message{msg(gen, 1206, later.Add(-time.Minute))}, client.messages[gen]...)
	client.channels[guild][1].LastMessageID = id(1206)
	client.messageErrors = map[string]error{gen: errors.New("injected stop after prune")}
	_, _ = svc2.Sync(ctx, SyncOptions{Concurrency: 1}) // steady-state incremental cycle, interrupted
	require.Equal(t, cursorsPrePrune, syncStateDigest(t, sel), "interrupted cycle changes no cursor")
	require.NotZero(t, rec.requests(gen), "interrupted cycle did hit the pruned channel")
	client.messageErrors = nil
	_, err = svc2.Sync(ctx, SyncOptions{Concurrency: 1}) // resume
	require.NoError(t, err)

	latestScope := channelLatestScope(gen) + "=" + id(1205) + "\n"
	require.Contains(t, cursorsPrePrune, latestScope)
	require.Equal(t,
		strings.Replace(cursorsPrePrune, latestScope, channelLatestScope(gen)+"="+id(1206)+"\n", 1),
		syncStateDigest(t, sel), "resume advances ONLY the latest cursor of general (no reset, no fake complete)")
	for _, e := range expelled {
		require.Equal(t, 0, count(`select count(*) from messages where id = ?`, e), "expelled %s not reimported", e)
	}
	require.Equal(t, 1, count(`select count(*) from messages where id = ?`, id(1206)))
	require.Equal(t, 105-len(expelled)+1, count(`select count(*) from messages where channel_id = ?`, gen))
	require.Zero(t, rec.returned(expelled), "fake client never served an expelled ID again")
	require.Zero(t, rec.backfillRequests(gen), "no backward (history) request on the pruned channel")
	require.GreaterOrEqual(t, rec.requests(gen), 2, "general polled in both cycles, so the zeros above are meaningful")
	loneComplete, err = sel.GetSyncState(ctx, channelHistoryCompleteScope(lone))
	require.NoError(t, err)
	require.Empty(t, loneComplete, "still no fake history_complete below the floor")
	genComplete2, err := sel.GetSyncState(ctx, channelHistoryCompleteScope(gen))
	require.NoError(t, err)
	require.Equal(t, genComplete, genComplete2)
	require.Equal(t, 0, count(`select count(*) from embedding_jobs`))
	require.Equal(t, int64(0), assetGETs.Load())
	require.NotEqual(t, cursorsBefore, cursorsPrePrune, "cursors advanced for new activity (sanity)")
}

// recordingClient records every ChannelMessages request and the IDs served.
type recordingClient struct {
	*fakeClient
	mu   sync.Mutex
	reqs []recordedReq
}

type recordedReq struct {
	channel, before, after string
	served                 []string
}

func (r *recordingClient) ChannelMessages(ctx context.Context, channelID string, limit int, beforeID, afterID string) ([]*discordgo.Message, error) {
	msgs, err := r.fakeClient.ChannelMessages(ctx, channelID, limit, beforeID, afterID)
	req := recordedReq{channel: channelID, before: beforeID, after: afterID}
	for _, m := range msgs {
		req.served = append(req.served, m.ID)
	}
	r.mu.Lock()
	r.reqs = append(r.reqs, req)
	r.mu.Unlock()
	return msgs, err
}

func (r *recordingClient) returned(ids []string) int {
	set := map[string]bool{}
	for _, id := range ids {
		set[id] = true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, q := range r.reqs {
		for _, id := range q.served {
			if set[id] {
				n++
			}
		}
	}
	return n
}

func (r *recordingClient) requests(ch string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, q := range r.reqs {
		if q.channel == ch {
			n++
		}
	}
	return n
}

func (r *recordingClient) backfillRequests(ch string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, q := range r.reqs {
		if q.channel == ch && q.before != "" {
			n++
		}
	}
	return n
}

func writeTestJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, b, 0o600))
}

func syncStateDigest(t *testing.T, s *store.Store) string {
	t.Helper()
	rows, err := s.DB().Query(`select scope, coalesce(cursor,'') from sync_state where scope like 'channel:%' order by scope`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	out := ""
	for rows.Next() {
		var a, b string
		require.NoError(t, rows.Scan(&a, &b))
		out += a + "=" + b + "\n"
	}
	require.NoError(t, rows.Err())
	return out
}
