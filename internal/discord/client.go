package discord

import (
	"context"

	"github.com/bwmarrin/discordgo"
)

// EventHandler receives Gateway events from a Tail session.
type EventHandler interface {
	OnMessageCreate(context.Context, *discordgo.Message) error
	OnMessageUpdate(context.Context, *discordgo.Message) error
	OnMessageDelete(context.Context, *discordgo.MessageDelete) error
	OnChannelUpsert(context.Context, *discordgo.Channel) error
	OnMemberUpsert(context.Context, string, *discordgo.Member) error
	OnMemberDelete(context.Context, string, string) error
}

// PrivateChannelLister is optionally implemented by clients that can list
// DM and group DM channels (user-token mode only).
type PrivateChannelLister interface {
	PrivateChannels(ctx context.Context) ([]*discordgo.Channel, error)
}

// Client is the interface for interacting with the Discord API.
// Implementations include botclient.BotClient (bot token) and,
// in the future, userclient.UserClient (user token / self-bot).
type Client interface {
	Close() error
	Self(ctx context.Context) (*discordgo.User, error)
	Guilds(ctx context.Context) ([]*discordgo.UserGuild, error)
	Guild(ctx context.Context, guildID string) (*discordgo.Guild, error)
	GuildChannels(ctx context.Context, guildID string) ([]*discordgo.Channel, error)
	ThreadsActive(ctx context.Context, channelID string) ([]*discordgo.Channel, error)
	GuildThreadsActive(ctx context.Context, guildID string) ([]*discordgo.Channel, error)
	ThreadsArchived(ctx context.Context, channelID string, private bool) ([]*discordgo.Channel, error)
	GuildMembers(ctx context.Context, guildID string) ([]*discordgo.Member, error)
	ChannelMessages(ctx context.Context, channelID string, limit int, beforeID, afterID string) ([]*discordgo.Message, error)
	ChannelMessage(ctx context.Context, channelID, messageID string) (*discordgo.Message, error)
	Tail(ctx context.Context, handler EventHandler) error
}
