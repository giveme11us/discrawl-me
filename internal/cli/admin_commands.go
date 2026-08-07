package cli

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/giveme11us/discrawl-me/internal/config"
	"github.com/giveme11us/discrawl-me/internal/discord/botclient"
	"github.com/giveme11us/discrawl-me/internal/discord/userclient"
	"github.com/giveme11us/discrawl-me/internal/embedder"
	"github.com/giveme11us/discrawl-me/internal/mcp"
	"github.com/giveme11us/discrawl-me/internal/store"
	"github.com/giveme11us/discrawl-me/internal/syncer"
)

func (r *runtime) runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fromOpenClaw := fs.String("from-openclaw", "", "")
	account := fs.String("account", "", "")
	guildID := fs.String("guild", "", "")
	dbPath := fs.String("db", "", "")
	withEmbeddings := fs.Bool("with-embeddings", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	cfg := config.Default()
	if *fromOpenClaw != "" {
		cfg.Discord.OpenClawConfig = *fromOpenClaw
	}
	if *account != "" {
		cfg.Discord.Account = *account
	}
	if *dbPath != "" {
		cfg.DBPath = *dbPath
	}
	cfg.Search.Embeddings.Enabled = *withEmbeddings
	if err := cfg.Normalize(); err != nil {
		return configErr(err)
	}
	token, err := config.ResolveDiscordToken(cfg)
	if err != nil {
		return authErr(err)
	}
	discordFactory := r.newDiscord
	if discordFactory == nil {
		discordFactory = func(cfg config.Config) (discordClient, error) {
			if cfg.IsUserMode() {
				return userclient.New(token.Token, cfg.Discord.User)
			}
			return botclient.New(token.Token)
		}
	}
	client, err := discordFactory(cfg)
	if err != nil {
		return authErr(err)
	}
	defer func() { _ = client.Close() }()
	syncerFactory := r.newSyncer
	if syncerFactory == nil {
		syncerFactory = func(client syncer.Client, s *store.Store, logger *slog.Logger) syncService {
			return syncer.New(client, s, logger)
		}
	}
	syncerSvc := syncerFactory(client, nil, r.logger)
	guilds, err := syncerSvc.DiscoverGuilds(r.ctx)
	if err != nil {
		return authErr(err)
	}
	cfg.GuildIDs = make([]string, 0, len(guilds))
	for _, guild := range guilds {
		cfg.GuildIDs = append(cfg.GuildIDs, guild.ID)
	}
	if *guildID != "" {
		cfg.DefaultGuildID = *guildID
	} else if info, err := config.LoadOpenClawDiscord(cfg.Discord.OpenClawConfig, cfg.Discord.Account); err == nil {
		if len(info.GuildIDs) == 1 {
			cfg.DefaultGuildID = info.GuildIDs[0]
		}
	}
	if cfg.DefaultGuildID == "" && len(cfg.GuildIDs) == 1 {
		cfg.DefaultGuildID = cfg.GuildIDs[0]
	}
	if err := config.Write(r.configPath, cfg); err != nil {
		return configErr(err)
	}
	return r.print(map[string]any{
		"config_path":       r.configPath,
		"db_path":           cfg.DBPath,
		"token_source":      token.Source,
		"default_guild_id":  cfg.DefaultGuildID,
		"discovered_guilds": cfg.GuildIDs,
	})
}

func (r *runtime) runSync(args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	full := fs.Bool("full", false, "")
	all := fs.Bool("all", false, "")
	since := fs.String("since", "", "")
	channels := fs.String("channels", "", "")
	concurrency := fs.Int("concurrency", r.cfg.Sync.Concurrency, "")
	withEmbeddings := fs.Bool("with-embeddings", false, "")
	guildsFlag := fs.String("guilds", "", "")
	guildFlag := fs.String("guild", "", "")
	includeDMs := fs.Bool("include-dms", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	var sinceTime time.Time
	if *since != "" {
		parsed, err := time.Parse(time.RFC3339, *since)
		if err != nil {
			return usageErr(fmt.Errorf("invalid --since: %w", err))
		}
		sinceTime = parsed
	}
	guildIDs, err := r.resolveSyncGuildsAll(*guildFlag, *guildsFlag, *all)
	if err != nil {
		return usageErr(err)
	}
	opts := syncer.SyncOptions{
		Full:        *full,
		GuildIDs:    guildIDs,
		ChannelIDs:  csvList(*channels),
		Concurrency: *concurrency,
		Since:       sinceTime,
		Embeddings:  *withEmbeddings,
		IncludeDMs:  *includeDMs,
	}
	stats, err := r.syncer.Sync(r.ctx, opts)
	if err != nil {
		return err
	}
	return r.print(stats)
}

func (r *runtime) runTail(args []string) error {
	fs := flag.NewFlagSet("tail", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repairEvery := fs.Duration("repair-every", mustDuration(r.cfg.Sync.RepairEvery), "")
	guildsFlag := fs.String("guilds", "", "")
	guildFlag := fs.String("guild", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	ctx, stop := signal.NotifyContext(r.ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	return r.syncer.RunTail(ctx, r.resolveSyncGuilds(*guildFlag, *guildsFlag), *repairEvery)
}

func (r *runtime) runStatus(args []string) error {
	if len(args) != 0 {
		return usageErr(fmt.Errorf("status takes no arguments"))
	}
	dbPath, err := config.ExpandPath(r.cfg.DBPath)
	if err != nil {
		return configErr(err)
	}
	status, err := r.store.Status(r.ctx, dbPath, r.cfg.EffectiveDefaultGuildID())
	if err != nil {
		return err
	}
	return r.print(status)
}

func (r *runtime) runEmbed(args []string) error {
	fs := flag.NewFlagSet("embed", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	sub := fs.Args()
	if len(sub) == 0 {
		sub = []string{"status"}
	}
	switch sub[0] {
	case "run":
		provider := r.createEmbedProvider()
		worker := embedder.NewWorker(r.store, provider, r.cfg.Search.Embeddings.BatchSize, r.logger)
		total, err := worker.RunAll(r.ctx)
		if err != nil {
			return err
		}
		return r.print(map[string]any{
			"processed": total,
			"provider":  provider.Name(),
			"dim":       provider.Dim(),
		})
	case "status":
		provider := r.createEmbedProvider()
		worker := embedder.NewWorker(r.store, provider, r.cfg.Search.Embeddings.BatchSize, r.logger)
		backlog, err := worker.Backlog(r.ctx)
		if err != nil {
			return err
		}
		var embeddedCount int
		_ = r.store.DB().QueryRowContext(r.ctx, `select count(*) from message_embeddings`).Scan(&embeddedCount)
		return r.print(map[string]any{
			"backlog":  backlog,
			"embedded": embeddedCount,
			"provider": provider.Name(),
			"enabled":  r.cfg.Search.Embeddings.Enabled,
		})
	default:
		return usageErr(fmt.Errorf("unknown embed subcommand %q (use: run, status)", sub[0]))
	}
}

func (r *runtime) runMCP(_ []string) error {
	var tools *mcp.ToolHandler
	if r.cfg.Search.Embeddings.Enabled {
		tools = mcp.NewToolHandler(r.store, r.createEmbedProvider())
	} else {
		tools = mcp.NewToolHandler(r.store)
	}
	server := mcp.NewServer(tools, r.logger)
	return server.Run(r.ctx, os.Stdin, r.stdout)
}

func (r *runtime) createEmbedProvider() embedder.Provider {
	cfg := r.cfg.Search.Embeddings
	switch cfg.Provider {
	case "openai":
		return embedder.NewOpenAIWithEndpoint(cfg.APIKeyEnv, cfg.Model, 0, cfg.Endpoint)
	default:
		// Default to Ollama
		return embedder.NewOllama(cfg.Endpoint, cfg.Model, 0)
	}
}

func (r *runtime) runDoctor(args []string) error {
	if len(args) != 0 {
		return usageErr(fmt.Errorf("doctor takes no arguments"))
	}
	report := map[string]any{
		"config_path": r.configPath,
	}
	cfg, err := config.Load(r.configPath)
	if err != nil {
		report["config"] = err.Error()
		return r.print(report)
	}
	report["config"] = "ok"
	report["default_guild_id"] = cfg.EffectiveDefaultGuildID()
	token, err := config.ResolveDiscordToken(cfg)
	if err != nil {
		report["discord_token"] = err.Error()
	} else {
		report["discord_token"] = token.Source
		discordFactory := r.newDiscord
		if discordFactory == nil {
			discordFactory = func(cfg config.Config) (discordClient, error) {
				if cfg.IsUserMode() {
					return userclient.New(token.Token, cfg.Discord.User)
				}
				return botclient.New(token.Token)
			}
		}
		client, clientErr := discordFactory(cfg)
		if clientErr == nil {
			defer func() { _ = client.Close() }()
			self, authErr := client.Self(r.ctx)
			if authErr != nil {
				report["discord_auth"] = authErr.Error()
			} else {
				report["discord_auth"] = "ok"
				report["bot_user_id"] = self.ID
			}
			guilds, guildErr := client.Guilds(r.ctx)
			if guildErr != nil {
				report["guild_access"] = guildErr.Error()
			} else {
				report["guild_access"] = len(guilds)
			}
		}
	}
	dbPath, err := config.ExpandPath(cfg.DBPath)
	if err == nil {
		db, dbErr := store.Open(r.ctx, dbPath)
		if dbErr != nil {
			report["database"] = dbErr.Error()
		} else {
			report["database"] = "ok"
			ftsErr := db.CheckMessageFTS(r.ctx)
			if ftsErr != nil {
				report["fts"] = ftsErr.Error()
			} else {
				report["fts"] = "ok"
			}
			report["vector"] = "not configured"
			_ = db.Close()
		}
	}
	return r.print(report)
}
