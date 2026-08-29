package userclient

import (
	"context"
	"strings"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/giveme11us/discrawl-me/internal/config"
	"github.com/giveme11us/discrawl-me/internal/discord"
	"github.com/stretchr/testify/require"
)

// noopHandler satisfies discord.EventHandler without doing anything.
type noopHandler struct{}

func (noopHandler) OnMessageCreate(context.Context, *discordgo.Message) error { return nil }
func (noopHandler) OnMessageUpdate(context.Context, *discordgo.Message) error { return nil }
func (noopHandler) OnMessageDelete(context.Context, *discordgo.MessageDelete) error {
	return nil
}
func (noopHandler) OnChannelUpsert(context.Context, *discordgo.Channel) error { return nil }
func (noopHandler) OnMemberUpsert(context.Context, string, *discordgo.Member) error {
	return nil
}
func (noopHandler) OnMemberDelete(context.Context, string, string) error { return nil }

var _ discord.EventHandler = noopHandler{}

// The Gateway is the only part of user mode that announces a presence to
// Discord. An announced presence suppresses the account's own mobile push
// notifications, so in user mode the Gateway must stay shut unless the
// operator opts in explicitly.
func TestTailRefusesWhenGatewayNotOptedIn(t *testing.T) {
	t.Parallel()

	client, err := New("token", config.UserConfig{})
	require.NoError(t, err)

	err = client.Tail(context.Background(), noopHandler{})

	require.Error(t, err)
	require.ErrorIs(t, err, ErrGatewayDisabled)
	require.Contains(t, strings.ToLower(err.Error()), "notification")
}

func TestTailRefusesWhenGatewayExplicitlyDisabled(t *testing.T) {
	t.Parallel()

	client, err := New("token", config.UserConfig{Gateway: config.GatewayDisabled})
	require.NoError(t, err)

	require.ErrorIs(t, client.Tail(context.Background(), noopHandler{}), ErrGatewayDisabled)
}

// Opting in must actually reach the dial path rather than the guard. A
// cancelled context makes Run unwind before any network traffic.
func TestTailProceedsWhenGatewayEnabled(t *testing.T) {
	t.Parallel()

	client, err := New("token", config.UserConfig{Gateway: config.GatewayEnabled})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NotErrorIs(t, client.Tail(ctx, noopHandler{}), ErrGatewayDisabled)
}

// A guarded client must not hold a presence-bearing connection, so closing it
// is always safe and always clean.
func TestCloseIsSafeWhenGatewayNeverStarted(t *testing.T) {
	t.Parallel()

	client, err := New("token", config.UserConfig{})
	require.NoError(t, err)

	require.NoError(t, client.Close())
}
