package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/giveme11us/discrawl-me/internal/config"
	"github.com/giveme11us/discrawl-me/internal/store"
	"github.com/stretchr/testify/require"
)

type metadataDiscord struct {
	*fakeDiscordClient
	messageCalls int
}

func (f *metadataDiscord) GuildChannels(_ context.Context, gid string) ([]*discordgo.Channel, error) {
	return []*discordgo.Channel{{ID: "700000000000000001", GuildID: gid, Name: "fixture", Type: discordgo.ChannelTypeGuildText}}, nil
}
func (f *metadataDiscord) ChannelMessages(context.Context, string, int, string, string) ([]*discordgo.Message, error) {
	f.messageCalls++
	return nil, nil
}
func TestDiscoverCLIUsesNativeMetadataOnly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "fixture.db")
	cfgPath := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(cfgPath, cfg))
	var out bytes.Buffer
	client := &metadataDiscord{fakeDiscordClient: &fakeDiscordClient{}}
	rt := &runtime{ctx: ctx, configPath: cfgPath, stdout: &out, stderr: &bytes.Buffer{}, json: true, logger: discardLogger(), newDiscord: func(config.Config) (discordClient, error) { return client, nil }}
	require.NoError(t, rt.dispatch([]string{"discover", "--guild", "422855027535642625"}))
	var result map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &result))
	require.Equal(t, "422855027535642625", result["guild_id"])
	require.Len(t, result["channels"], 1)
	require.Zero(t, client.messageCalls)
	db, err := store.Open(ctx, cfg.DBPath)
	require.NoError(t, err)
	defer db.Close()
	_, rows, err := db.ReadOnlyQuery(ctx, "select count(*) from messages")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
	rt.syncer = nil
	require.Equal(t, 2, ExitCode(rt.runDiscover(nil)))
	require.Equal(t, 2, ExitCode(rt.runDiscover([]string{"--guild", "422855027535642625", "--all"})))
}
