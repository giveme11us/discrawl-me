package userclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/steipete/discrawl/internal/config"
	"github.com/steipete/discrawl/internal/discord"
)

// UserClient implements discord.Client using a user token (self-bot).
// All requests go through a custom HTTP transport with rate limiting,
// super-properties headers, and read-only enforcement.
type UserClient struct {
	transport *transport
	gateway   *Gateway
}

// Compile-time check that UserClient satisfies the discord.Client interface.
var _ discord.Client = (*UserClient)(nil)

// New creates a new UserClient with the given user token and config.
func New(token string, cfg config.UserConfig) (*UserClient, error) {
	if token == "" {
		return nil, fmt.Errorf("user token is required")
	}
	tcfg := transportConfig{
		token:             token,
		userAgent:         cfg.UserAgent,
		locale:            cfg.Locale,
		superPropsEncoded: buildSuperProperties(cfg.UserAgent, cfg.BrowserVersion, cfg.Locale, cfg.ClientBuildNumber),
		minGap:            time.Duration(cfg.MinRequestGapMs) * time.Millisecond,
		jitterMin:         time.Duration(cfg.JitterMsMin) * time.Millisecond,
		jitterMax:         time.Duration(cfg.JitterMsMax) * time.Millisecond,
		proxy:             cfg.Proxy,
	}
	return &UserClient{
		transport: newTransport(tcfg),
		gateway:   NewGateway(token, tcfg.superPropsEncoded, slog.Default()),
	}, nil
}

func (c *UserClient) Close() error {
	return c.gateway.Close()
}

func (c *UserClient) Self(ctx context.Context) (*discordgo.User, error) {
	var user discordgo.User
	if err := c.getJSON(ctx, "/users/@me", &user); err != nil {
		return nil, fmt.Errorf("fetch self: %w", err)
	}
	return &user, nil
}

func (c *UserClient) Guilds(ctx context.Context) ([]*discordgo.UserGuild, error) {
	var out []*discordgo.UserGuild
	after := ""
	for {
		path := "/users/@me/guilds?limit=200"
		if after != "" {
			path += "&after=" + after
		}
		var page []*discordgo.UserGuild
		if err := c.getJSON(ctx, path, &page); err != nil {
			return nil, fmt.Errorf("fetch guilds: %w", err)
		}
		if len(page) == 0 {
			return out, nil
		}
		out = append(out, page...)
		after = page[len(page)-1].ID
		if len(page) < 200 {
			return out, nil
		}
	}
}

func (c *UserClient) Guild(ctx context.Context, guildID string) (*discordgo.Guild, error) {
	var guild discordgo.Guild
	if err := c.getJSON(ctx, "/guilds/"+guildID, &guild); err != nil {
		return nil, fmt.Errorf("fetch guild %s: %w", guildID, err)
	}
	return &guild, nil
}

func (c *UserClient) GuildChannels(ctx context.Context, guildID string) ([]*discordgo.Channel, error) {
	var channels []*discordgo.Channel
	if err := c.getJSON(ctx, "/guilds/"+guildID+"/channels", &channels); err != nil {
		return nil, fmt.Errorf("fetch channels for guild %s: %w", guildID, err)
	}
	return channels, nil
}

func (c *UserClient) ThreadsActive(ctx context.Context, channelID string) ([]*discordgo.Channel, error) {
	var list discordgo.ThreadsList
	if err := c.getJSON(ctx, "/channels/"+channelID+"/threads/active", &list); err != nil {
		return nil, fmt.Errorf("fetch active threads for channel %s: %w", channelID, err)
	}
	return list.Threads, nil
}

func (c *UserClient) GuildThreadsActive(ctx context.Context, guildID string) ([]*discordgo.Channel, error) {
	var list discordgo.ThreadsList
	if err := c.getJSON(ctx, "/guilds/"+guildID+"/threads/active", &list); err != nil {
		return nil, fmt.Errorf("fetch active threads for guild %s: %w", guildID, err)
	}
	return list.Threads, nil
}

func (c *UserClient) ThreadsArchived(ctx context.Context, channelID string, private bool) ([]*discordgo.Channel, error) {
	var out []*discordgo.Channel
	var before string
	kind := "public"
	if private {
		kind = "private"
	}
	for {
		path := "/channels/" + channelID + "/threads/archived/" + kind + "?limit=100"
		if before != "" {
			path += "&before=" + before
		}
		var list discordgo.ThreadsList
		if err := c.getJSON(ctx, path, &list); err != nil {
			return nil, fmt.Errorf("fetch archived %s threads for channel %s: %w", kind, channelID, err)
		}
		if len(list.Threads) == 0 {
			return out, nil
		}
		out = append(out, list.Threads...)
		if !list.HasMore {
			return out, nil
		}
		oldest := list.Threads[len(list.Threads)-1]
		if oldest.ThreadMetadata == nil {
			return out, nil
		}
		before = oldest.ThreadMetadata.ArchiveTimestamp.Format(time.RFC3339)
	}
}

func (c *UserClient) GuildMembers(ctx context.Context, guildID string) ([]*discordgo.Member, error) {
	// User tokens cannot use the bot GUILD_MEMBERS endpoint for large guilds.
	// Try the standard endpoint — it works for small guilds and guilds where
	// the user has admin perms. For large guilds this will return partial data.
	var out []*discordgo.Member
	after := ""
	for {
		path := "/guilds/" + guildID + "/members?limit=1000"
		if after != "" {
			path += "&after=" + after
		}
		var page []*discordgo.Member
		if err := c.getJSON(ctx, path, &page); err != nil {
			// Graceful degradation: if we can't list members, return what we have
			if len(out) > 0 {
				return out, nil
			}
			return nil, fmt.Errorf("fetch members for guild %s: %w", guildID, err)
		}
		if len(page) == 0 {
			return out, nil
		}
		out = append(out, page...)
		if page[len(page)-1].User == nil {
			return out, nil
		}
		after = page[len(page)-1].User.ID
		if len(page) < 1000 {
			return out, nil
		}
	}
}

func (c *UserClient) ChannelMessages(ctx context.Context, channelID string, limit int, beforeID, afterID string) ([]*discordgo.Message, error) {
	path := fmt.Sprintf("/channels/%s/messages?limit=%d", channelID, limit)
	if beforeID != "" {
		path += "&before=" + beforeID
	}
	if afterID != "" {
		path += "&after=" + afterID
	}
	var messages []*discordgo.Message
	if err := c.getJSON(ctx, path, &messages); err != nil {
		return nil, fmt.Errorf("fetch messages for channel %s: %w", channelID, err)
	}
	return messages, nil
}

func (c *UserClient) ChannelMessage(ctx context.Context, channelID, messageID string) (*discordgo.Message, error) {
	var msg discordgo.Message
	if err := c.getJSON(ctx, "/channels/"+channelID+"/messages/"+messageID, &msg); err != nil {
		return nil, fmt.Errorf("fetch message %s in channel %s: %w", messageID, channelID, err)
	}
	return &msg, nil
}

// Tail connects to the Discord Gateway and dispatches live events.
func (c *UserClient) Tail(ctx context.Context, handler discord.EventHandler) error {
	return c.gateway.Run(ctx, handler)
}

// PrivateChannels returns the user's DM and group DM channels.
// This endpoint is only available with user tokens.
func (c *UserClient) PrivateChannels(ctx context.Context) ([]*discordgo.Channel, error) {
	var channels []*discordgo.Channel
	if err := c.getJSON(ctx, "/users/@me/channels", &channels); err != nil {
		return nil, fmt.Errorf("fetch private channels: %w", err)
	}
	return channels, nil
}

// getJSON performs a GET request and unmarshals the JSON response into dest.
func (c *UserClient) getJSON(ctx context.Context, path string, dest any) error {
	resp, err := c.transport.do(ctx, "GET", path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("discord API error %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return json.NewDecoder(resp.Body).Decode(dest)
}
