package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/giveme11us/discrawl-me/internal/embedder"
	"github.com/giveme11us/discrawl-me/internal/mcp"
	"github.com/giveme11us/discrawl-me/internal/memory"
	"github.com/giveme11us/discrawl-me/internal/store"
)

// runMemory implements `memory export|status|prune|mcp`. None of these
// subcommands load the config, resolve a token or build a Discord client, and
// none of them creates a database except export (a NEW output file).
func (r *runtime) runMemory(args []string) error {
	if len(args) == 0 {
		return usageErr(errors.New("memory requires a subcommand: export, status, prune, reconcile, close-rollback-window, mcp, search, conversation, guilds, channels, sql, similar"))
	}
	fs := flag.NewFlagSet("memory "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	source := fs.String("source", "", "")
	output := fs.String("output", "", "")
	db := fs.String("db", "", "")
	policy := fs.String("policy", "", "")
	nowFlag := fs.String("now", "", "")
	hashSource := fs.Bool("hash-source", false, "")
	dryRun := fs.Bool("dry-run", false, "")
	vacuum := fs.Bool("vacuum", false, "")
	guild := fs.String("guild", "", "")
	channel := fs.String("channel", "", "")
	author := fs.String("author", "", "")
	mode := fs.String("mode", "fts", "")
	limit := fs.Int("limit", 20, "")
	messageID := fs.String("message-id", "", "")
	before := fs.Int("before", 5, "")
	after := fs.Int("after", 5, "")
	selectiveFlag := fs.String("selective", "", "")
	archiveFlag := fs.String("archive", "", "")
	sinceFlag := fs.String("since", "", "")
	confirmBoundary := fs.String("confirm-boundary", "", "")
	noRollbackWindow := fs.Bool("no-rollback-window", false, "")
	enqueueMissing := fs.Bool("enqueue-missing-embeddings", false, "")
	embedEndpoint := fs.String("embed-endpoint", "", "")
	embedModel := fs.String("embed-model", "", "")
	embedDim := fs.Int("embed-dim", 0, "")
	embedKeyEnv := fs.String("embed-api-key-env", "OPENAI_API_KEY", "")
	if err := fs.Parse(args[1:]); err != nil {
		return usageErr(err)
	}
	wantArgs := 0
	if args[0] == "search" || args[0] == "sql" {
		wantArgs = 1
	}
	if fs.NArg() != wantArgs {
		return usageErr(fmt.Errorf("memory %s: unexpected arguments: %v", args[0], fs.Args()))
	}
	now, err := memory.ParseNow(*nowFlag)
	if err != nil {
		return usageErr(fmt.Errorf("invalid --now: %w", err))
	}
	var provider embedder.Provider
	if *embedEndpoint != "" {
		// Optional query embedder for vector/hybrid search (explicit flags,
		// no config file). find_similar never needs it.
		if *embedModel == "" || *embedDim <= 0 {
			return usageErr(errors.New("--embed-endpoint requires --embed-model and --embed-dim"))
		}
		provider = embedder.NewOpenAIWithEndpoint(*embedKeyEnv, *embedModel, *embedDim, *embedEndpoint)
	}
	switch args[0] {
	case "export":
		if *source == "" || *output == "" || *policy == "" {
			return usageErr(errors.New("memory export requires --source, --output and --policy"))
		}
		rep, err := memory.Export(r.ctx, memory.ExportOptions{Source: *source, Output: *output, PolicyPath: *policy, Now: now, HashSource: *hashSource,
			NoRollbackWindow: *noRollbackWindow, EnqueueMissingEmbeddings: *enqueueMissing})
		if err != nil {
			return dbErr(err)
		}
		return r.printJSON(rep)
	case "status":
		if *db == "" {
			return usageErr(errors.New("memory status requires --db (explicit path; no DB is created)"))
		}
		rep, err := memory.Status(r.ctx, *db, *policy, now)
		if err != nil {
			return dbErr(err)
		}
		return r.printJSON(rep)
	case "prune":
		if *db == "" || *policy == "" {
			return usageErr(errors.New("memory prune requires --db and --policy"))
		}
		rep, err := memory.Prune(r.ctx, *db, *policy, now, *dryRun, *vacuum)
		if err != nil {
			return dbErr(err)
		}
		return r.printJSON(rep)
	case "close-rollback-window":
		// Explicit, separate and irreversible for rollback purposes: never
		// run by sync/prune. Requires the marker boundary echoed back.
		if *db == "" || *confirmBoundary == "" {
			return usageErr(errors.New("memory close-rollback-window requires --db and --confirm-boundary (the marker boundary shown by memory status)"))
		}
		rep, err := memory.CloseRollbackWindow(r.ctx, *db, *confirmBoundary, now)
		if err != nil {
			return dbErr(err)
		}
		return r.printJSON(rep)
	case "reconcile":
		// Rollback aid: apply post-boundary selective changes BY ID to a
		// PARKED full archive (collector stopped). Never on a live DB.
		var since time.Time
		if *sinceFlag != "" {
			if since, err = memory.ParseNow(*sinceFlag); err != nil {
				return usageErr(fmt.Errorf("invalid --since: %w", err))
			}
		}
		rep, err := memory.Reconcile(r.ctx, memory.ReconcileOptions{Selective: *selectiveFlag, Archive: *archiveFlag, Since: since, DryRun: *dryRun})
		if err != nil {
			return dbErr(err)
		}
		return r.printJSON(rep)
	case "mcp":
		tools, closeFn, err := r.openSelectiveTools(*db, *policy, now, provider)
		if err != nil {
			return err
		}
		defer closeFn()
		return mcp.NewServer(tools, r.logger).Run(r.ctx, os.Stdin, r.stdout)
	case "search", "conversation", "guilds", "channels", "sql", "similar":
		// Offline CLI readers over the SAME read-only selective handler as
		// `memory mcp`: explicit --db, mode=ro+query_only, no create/migrate,
		// no config/token/client; every output starts with the selective
		// notice; similar/vector use the embeddings kept for retained
		// messages (vector/hybrid by text need --embed-endpoint).
		tools, closeFn, err := r.openSelectiveTools(*db, *policy, now, provider)
		if err != nil {
			return err
		}
		defer closeFn()
		var name string
		var params map[string]any
		switch args[0] {
		case "search":
			name, params = "search_messages", map[string]any{"query": fs.Arg(0), "guild_id": *guild, "channel_id": *channel, "author": *author, "mode": *mode, "limit": *limit}
		case "conversation":
			if *messageID == "" {
				return usageErr(errors.New("memory conversation requires --message-id"))
			}
			name, params = "get_conversation", map[string]any{"message_id": *messageID, "before": *before, "after": *after}
		case "guilds":
			name, params = "list_guilds", map[string]any{}
		case "channels":
			if *guild == "" {
				return usageErr(errors.New("memory channels requires --guild"))
			}
			name, params = "list_channels", map[string]any{"guild_id": *guild}
		case "sql":
			name, params = "run_sql", map[string]any{"query": fs.Arg(0)}
		case "similar":
			name, params = "find_similar", map[string]any{"message_id": *messageID, "limit": *limit}
		}
		body, err := json.Marshal(params)
		if err != nil {
			return err
		}
		out, err := tools.CallTool(r.ctx, name, body)
		if err != nil {
			return dbErr(err)
		}
		_, err = fmt.Fprintln(r.stdout, out)
		return err
	default:
		return usageErr(fmt.Errorf("unknown memory subcommand %q", args[0]))
	}
}

// openSelectiveTools opens an EXPLICIT selective DB strictly read-only and
// returns the selective tool handler (notice + memory_status; vector search
// over the kept embeddings, with an optional query embedder).
func (r *runtime) openSelectiveTools(dbPath, policyPath string, now time.Time, provider embedder.Provider) (*mcp.ToolHandler, func(), error) {
	if dbPath == "" {
		return nil, nil, usageErr(errors.New("memory readers require --db (explicit path; no DB is created)"))
	}
	s, err := store.OpenReadOnly(r.ctx, dbPath)
	if err != nil {
		return nil, nil, dbErr(err)
	}
	closeFn := func() { _ = s.Close() }
	marker, err := memory.ReadMarker(r.ctx, s.DB())
	if err != nil {
		closeFn()
		return nil, nil, dbErr(err)
	}
	if marker == nil {
		closeFn()
		return nil, nil, dbErr(errors.New("memory readers: DB has no selective memory marker"))
	}
	var tools *mcp.ToolHandler
	if provider != nil {
		tools = mcp.NewToolHandler(s, provider)
	} else {
		tools = mcp.NewToolHandler(s)
	}
	tools.SetSelectiveMemory(memory.SelectiveNotice, func(ctx context.Context) (string, error) {
		rep, err := memory.Status(ctx, dbPath, policyPath, now)
		if err != nil {
			return "", err
		}
		body, err := json.MarshalIndent(rep, "", "  ")
		return string(body), err
	})
	return tools, closeFn, nil
}

func (r *runtime) printJSON(value any) error {
	enc := json.NewEncoder(r.stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}
