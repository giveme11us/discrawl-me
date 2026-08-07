# discrawl-me — Personal Discord archive with AI search and MCP

Hard fork of [steipete/discrawl](https://github.com/steipete/discrawl). Archives your Discord servers and DMs using a **user token** (no bot install needed), with semantic search and an MCP server for AI assistants.

> **Warning:** Using a user token (self-bot) violates Discord's Terms of Service. Your account may be suspended. Use a throwaway account. This tool is strictly read-only by design — it never sends messages, reacts, or modifies anything on Discord.

## What's different from upstream

| Feature | upstream (discrawl) | this fork (discrawl-me) |
|---------|-------------------|----------------------|
| Auth | Bot token only | **User token** (self-bot) + bot token |
| DMs | Not possible | **Full DM/group DM archiving** |
| Gateway | discordgo library | **Custom WebSocket client** (user-mode identify, heartbeat, resume) |
| Search | FTS5 keyword only | **Hybrid search** (FTS5 + vector cosine + RRF) |
| Embeddings | Scaffolded, not working | **Working pipeline** (Ollama, OpenAI providers) |
| MCP | None | **MCP server** for Claude Code / Cursor / AI assistants |
| Edit history | Not tracked | **Full edit snapshots** on every MESSAGE_UPDATE |
| Reactions | Not tracked | **Reaction timeline** (add/remove events) |
| Rate limiting | Discord library defaults | **Conservative** (1s gap + jitter, read-only enforced at transport layer) |
| Headers | Bot headers | **Real browser headers** (X-Super-Properties, Sec-Ch-Ua, etc.) |

## Requirements

- Go 1.26+
- A Discord user token (see [Getting your token](#getting-your-user-token))
- Optional: [Ollama](https://ollama.ai) for local embeddings

## Install

```bash
git clone https://github.com/giveme11us/discrawl-me.git
cd discrawl-me
go build -o discrawl-me ./cmd/discrawl
```

## Getting your user token

1. Open Discord in your browser (not the desktop app)
2. Open DevTools (`Cmd+Option+I` on Mac, `F12` on Windows)
3. Go to the **Network** tab, filter by **Fetch/XHR**
4. Click any request to `discord.com/api/...`
5. Find the `Authorization` header in Request Headers — that's your token

Or paste this in the browser Console:

```js
(webpackChunkdiscord_app.push([[''],{},e=>{m=[];for(let c in e.c)m.push(e.c[c])}]),m).find(m=>m?.exports?.default?.getToken!==void 0).exports.default.getToken()
```

## Quick start

```bash
# Set your token
export DISCORD_USER_TOKEN="your-token-here"

# Create config
mkdir -p ~/.discrawl-me
cat > ~/.discrawl-me/config.toml << 'EOF'
version = 1
db_path = "~/.discrawl-me/discrawl.db"
cache_dir = "~/.discrawl-me/cache"
log_dir = "~/.discrawl-me/logs"

[discord]
mode = "user"
token_source = "env"
token_env = "DISCORD_USER_TOKEN"

[sync]
concurrency = 1

[search]
default_mode = "fts"
EOF

# Sync a guild + your DMs
./discrawl-me --config ~/.discrawl-me/config.toml sync --guild <GUILD_ID> --include-dms

# Search
./discrawl-me --config ~/.discrawl-me/config.toml search "topic you remember"

# Live tail (Gateway)
./discrawl-me --config ~/.discrawl-me/config.toml tail
```

## Commands

All upstream commands work (`sync`, `tail`, `search`, `messages`, `mentions`, `sql`, `members`, `channels`, `status`, `doctor`). New commands:

### `sync --include-dms`

Archives DM and group DM channels under the synthetic guild `@me`.

```bash
discrawl-me sync --guild 123456 --include-dms
discrawl-me sync --all --include-dms
```

### `search --mode`

```bash
discrawl-me search "auth bug"                    # FTS (default)
discrawl-me search --mode=hybrid "auth bug"       # FTS + vector + RRF
discrawl-me search --mode=vector "auth bug"       # Pure semantic
```

### `embed`

```bash
discrawl-me embed status    # Show backlog and embedded count
discrawl-me embed run       # Process all pending embedding jobs
```

### `mcp`

Launches an MCP server over stdio for AI assistant integration.

```bash
discrawl-me mcp
```

## MCP server

The MCP server exposes your Discord archive as tools that AI assistants can use.

### Tools

| Tool | Description |
|------|-------------|
| `search_messages` | Keyword, semantic, or hybrid search with guild/channel/author filters |
| `get_conversation` | Messages around a target message (context window) |
| `list_guilds` | All archived guilds with message counts |
| `list_channels` | Channels in a guild with message counts |
| `find_similar` | Semantically similar messages (requires embeddings) |
| `run_sql` | Read-only SQL queries against the archive |

### Claude Code setup

Add to `~/.claude.json`:

```json
{
  "mcpServers": {
    "discrawl-me": {
      "type": "stdio",
      "command": "/path/to/discrawl-me",
      "args": ["--config", "/path/to/config.toml", "mcp"],
      "env": {
        "DISCORD_USER_TOKEN": "your-token"
      }
    }
  }
}
```

After restart, Claude Code can search your Discord history, get conversation context, and run SQL queries on your archive.

## Configuration

```toml
version = 1
db_path = "~/.discrawl-me/discrawl.db"
cache_dir = "~/.discrawl-me/cache"
log_dir = "~/.discrawl-me/logs"

[discord]
mode = "user"                    # "bot" or "user"
token_source = "env"             # "env" or "openclaw"
token_env = "DISCORD_USER_TOKEN"

[discord.user]                   # Only used when mode = "user"
client_build_number = 525145     # Bump when Discord updates
browser_version = "146.0.0.0"
locale = "it"
min_request_gap_ms = 1000        # Conservative rate limit
jitter_ms_min = 500
jitter_ms_max = 2000

[sync]
concurrency = 1                  # Keep low for user mode

[search]
default_mode = "fts"             # "fts", "vector", or "hybrid"

[search.embeddings]
enabled = false
provider = "ollama"              # "ollama" or "openai"
model = "nomic-embed-text"
batch_size = 32
```

## Embeddings

For semantic search, enable embeddings with a local Ollama instance:

```bash
# Install and start Ollama
ollama pull nomic-embed-text

# Enable in config
# [search.embeddings]
# enabled = true
# provider = "ollama"
# model = "nomic-embed-text"

# Sync with embeddings enabled, then process
discrawl-me sync --guild 123 --with-embeddings
discrawl-me embed run

# Now hybrid search works
discrawl-me search --mode=hybrid "what was that conversation about..."
```

## Architecture

```
cmd/discrawl/main.go
  internal/cli/              CLI dispatch
  internal/config/           TOML config + user auth settings
  internal/discord/
    client.go                Client interface + EventHandler
    botclient/               Bot-token impl (discordgo wrapper)
    userclient/              User-token impl (custom HTTP + Gateway)
      transport.go           Headers, rate limiter, read-only guard
      gateway.go             WebSocket, identify, heartbeat, resume
      userclient.go          discord.Client implementation
  internal/syncer/           Sync/tail loops, DM support
  internal/store/            SQLite schema, FTS5, migrations, queries
  internal/embedder/         Embedding pipeline (Ollama, OpenAI)
  internal/mcp/              MCP stdio server (6 tools)
```

## Safety measures

- **Read-only by design**: transport layer blocks all non-GET HTTP methods
- **Gateway whitelist**: only heartbeat, identify, and resume opcodes allowed
- **Conservative rate limiting**: 1s minimum gap + 500-2000ms jitter between requests
- **403 tolerance**: missing access on channels/threads is logged and skipped, not fatal
- **401 hard stop**: invalid token immediately stops all operations
- **No automation**: never sends messages, reacts, joins/leaves servers, or modifies any Discord state

## Schema

SQLite with 3 schema versions:

- **v1** (upstream): guilds, channels, members, messages, message_events, message_attachments, mention_events, sync_state, embedding_jobs, message_fts, member_fts
- **v2** (fork): + message_edits, reaction_events
- **v3** (fork): + message_embeddings

Migrations are automatic on startup.

## Development

```bash
go build ./...
go test ./...              # 11 packages, all must pass
go build -o discrawl-me ./cmd/discrawl
```

## License

MIT. See [LICENSE](LICENSE).

## Disclaimer

This software is provided for personal archival and research purposes only. Using a user token violates Discord's Terms of Service (Section II.E). The authors are not responsible for any account suspensions or other consequences. Use at your own risk with a throwaway account.
