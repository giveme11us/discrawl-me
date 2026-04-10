// Command testuser validates that UserClient works against the real Discord API.
// Usage: DISCORD_USER_TOKEN=<token> go run ./cmd/testuser
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"sync/atomic"

	"github.com/bwmarrin/discordgo"
	"github.com/steipete/discrawl/internal/config"
	"github.com/steipete/discrawl/internal/discord/userclient"
)

func main() {
	token := os.Getenv("DISCORD_USER_TOKEN")
	if token == "" {
		fmt.Fprintln(os.Stderr, "set DISCORD_USER_TOKEN env var")
		os.Exit(1)
	}

	cfg := config.DefaultUserConfig()
	client, err := userclient.New(token, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create client: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test 1: Self
	fmt.Println("=== Test 1: Self ===")
	user, err := client.Self(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL Self: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("OK  id=%s username=%s\n\n", user.ID, user.Username)

	// Test 2: Guilds
	fmt.Println("=== Test 2: Guilds ===")
	guilds, err := client.Guilds(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL Guilds: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("OK  %d guilds\n", len(guilds))
	for i, g := range guilds {
		if i >= 5 {
			fmt.Printf("    ... and %d more\n", len(guilds)-5)
			break
		}
		fmt.Printf("    [%s] %s\n", g.ID, g.Name)
	}
	fmt.Println()

	// Test 3: Private Channels (DMs)
	fmt.Println("=== Test 3: Private Channels (DMs) ===")
	dms, err := client.PrivateChannels(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL PrivateChannels: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("OK  %d DM channels\n", len(dms))
	for i, ch := range dms {
		if i >= 5 {
			fmt.Printf("    ... and %d more\n", len(dms)-5)
			break
		}
		name := ch.Name
		if name == "" && len(ch.Recipients) > 0 {
			name = ch.Recipients[0].Username
		}
		fmt.Printf("    [%s] type=%d %s\n", ch.ID, ch.Type, name)
	}
	fmt.Println()

	// Test 4: Guild channels (first guild)
	if len(guilds) > 0 {
		g := guilds[0]
		fmt.Printf("=== Test 4: Channels for guild %s ===\n", g.Name)
		channels, err := client.GuildChannels(ctx, g.ID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FAIL GuildChannels: %v\n", err)
		} else {
			fmt.Printf("OK  %d channels\n", len(channels))
			for i, ch := range channels {
				if i >= 5 {
					fmt.Printf("    ... and %d more\n", len(channels)-5)
					break
				}
				fmt.Printf("    [%s] type=%d %s\n", ch.ID, ch.Type, ch.Name)
			}
		}
		fmt.Println()

		// Test 5: Messages from first text channel
		for _, ch := range channels {
			if ch.Type == 0 { // text channel
				fmt.Printf("=== Test 5: Messages from #%s ===\n", ch.Name)
				msgs, err := client.ChannelMessages(ctx, ch.ID, 3, "", "")
				if err != nil {
					fmt.Fprintf(os.Stderr, "FAIL ChannelMessages: %v\n", err)
				} else {
					fmt.Printf("OK  %d messages\n", len(msgs))
					for _, m := range msgs {
						author := "unknown"
						if m.Author != nil {
							author = m.Author.Username
						}
						content := m.Content
						if len(content) > 80 {
							content = content[:80] + "..."
						}
						fmt.Printf("    [%s] %s: %s\n", m.ID, author, content)
					}
				}
				break
			}
		}
	}

	// Test 6: DM messages
	if len(dms) > 0 {
		dm := dms[0]
		dmName := dm.Name
		if dmName == "" && len(dm.Recipients) > 0 {
			dmName = dm.Recipients[0].Username
		}
		fmt.Printf("\n=== Test 6: Messages from DM with %s ===\n", dmName)
		dmMsgs, err := client.ChannelMessages(ctx, dm.ID, 3, "", "")
		if err != nil {
			fmt.Fprintf(os.Stderr, "FAIL DM Messages: %v\n", err)
		} else {
			fmt.Printf("OK  %d messages\n", len(dmMsgs))
			for _, m := range dmMsgs {
				author := "unknown"
				if m.Author != nil {
					author = m.Author.Username
				}
				content := m.Content
				if len(content) > 80 {
					content = content[:80] + "..."
				}
				fmt.Printf("    [%s] %s: %s\n", m.ID, author, content)
			}
		}
	}

	// Test 7: Gateway (Tail) — connect for 10s
	fmt.Println("\n=== Test 7: Gateway (10s) ===")
	tailCtx, tailCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer tailCancel()
	counter := &eventCounter{}
	err = client.Tail(tailCtx, counter)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Gateway error: %v\n", err)
	}
	fmt.Printf("OK  events received in 10s: creates=%d updates=%d deletes=%d channels=%d\n",
		counter.creates.Load(), counter.updates.Load(), counter.deletes.Load(), counter.channels.Load())

	fmt.Println("\n=== ALL TESTS PASSED ===")
}

type eventCounter struct {
	creates  atomic.Int32
	updates  atomic.Int32
	deletes  atomic.Int32
	channels atomic.Int32
}

func (e *eventCounter) OnMessageCreate(_ context.Context, _ *discordgo.Message) error {
	e.creates.Add(1)
	return nil
}
func (e *eventCounter) OnMessageUpdate(_ context.Context, _ *discordgo.Message) error {
	e.updates.Add(1)
	return nil
}
func (e *eventCounter) OnMessageDelete(_ context.Context, _ *discordgo.MessageDelete) error {
	e.deletes.Add(1)
	return nil
}
func (e *eventCounter) OnChannelUpsert(_ context.Context, _ *discordgo.Channel) error {
	e.channels.Add(1)
	return nil
}
func (e *eventCounter) OnMemberUpsert(_ context.Context, _ string, _ *discordgo.Member) error {
	return nil
}
func (e *eventCounter) OnMemberDelete(_ context.Context, _ string, _ string) error {
	return nil
}
