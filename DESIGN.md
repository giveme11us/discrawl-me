# discrawl-me — Design Document

**Fork target:** `github.com/steipete/discrawl` (Go 1.26, SQLite/FTS5, discordgo-based)
**Fork name (proposed):** `discrawl-me`
**Fork relationship:** Hard fork. No upstream sync.
**Primary use case:** Personal Discord archive via **user token** (self-bot), multi-guild + DM, with semantic search and MCP server as killer features.

---

## 1. Goals

### In scope
1. **User-token authentication** against Discord REST + Gateway (no bot install required).
2. **DM + group DM archiving** (not possible with bot tokens).
3. **Multi-guild** from a single account.
4. **Semantic search** on messages via embeddings (Ollama/OpenAI/Voyage pluggable).
5. **MCP server** exposing archive as tools for Claude Code / Cursor / other MCP clients.
6. **Hybrid search** (FTS5 BM25 + vector cosine) as default query mode.
7. **OpenClaw token source** preserved (already present in upstream).

### Out of scope (for v1)
- Posting/replying/reacting (strictly read-only; safety measure).
- Voice recording.
- Dashboard web UI (CLI + MCP is enough for v1).
- Account warmup automation (document manually).
- Image OCR / audio transcription (backlog).

### Non-goals
- Being accepted upstream. Upstream explicitly targets bot tokens and discourages self-bots.
- Competing with bot-only tools (MEE6, Dyno) — different use case.

---

## 2. Risks and mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| Discord ToS §II.E prohibits self-bots | **Critical** | README disclaimer, recommend throwaway account, read-only strict enforcement, opt-in flag `--i-understand-tos-risk` |
| Account ban (Cloudflare challenge, suspension) | **High** | Conservative rate limiting (1 req/s sustained, jitter 500ms–2s), exponential backoff on 429, hard stop on 403/401, realistic super-properties, persistent session_id, presence updates |
| IP ban | **Medium** | Optional SOCKS5/HTTP proxy support, recommend separating archive traffic from normal browsing |
| discordgo API drift | **Low** | Pinned version in `go.mod`, used only for data struct types (not Session) |
| `message_search` endpoint deprecation | **Medium** | Fall back to pagination crawl via `GET /channels/{id}/messages` |
| SQLite file corruption on crash | **Low** | WAL mode, periodic `pragma optimize`, backup command |

**Ban is not eliminable, only reducible.** The README must be explicit.

---

## 3. Architecture

### 3.1 Current upstream (baseline)
```
cmd/discrawl/main.go
  └── internal/cli            ← cobra-like command dispatch
        └── internal/syncer   ← sync/tail loops, channel catalog, enrichment
              └── internal/discord  ← wrapper over discordgo.Session (BOT ONLY)
                    └── bwmarrin/discordgo
              └── internal/store    ← SQLite schema, FTS5, query layer
        └── internal/config    ← TOML config, OpenClaw token source
```

### 3.2 Target (discrawl-me)
```
cmd/discrawl-me/main.go
  └── internal/cli
        └── internal/syncer            ← unchanged (uses discordgo data types as DTO)
              └── internal/discord     ← now an INTERFACE, not a struct
                    ├── botclient/     ← old discordgo wrapper (kept, for compat)
                    └── userclient/    ← NEW: HTTP + Gateway custom, user-token
              └── internal/store       ← + message_embeddings, + message_edits, + reactions, + dm_channels
        └── internal/embedder          ← NEW: consumes embedding_jobs queue
        └── internal/mcp               ← NEW: MCP stdio server
        └── internal/config            ← + DM filters, + user auth section
  └── cmd/discrawl-me/mcp.go           ← subcommand `discrawl-me mcp`
```

### 3.3 Key design decision: discordgo data types as lingua franca

**Observation:** audit shows that outside `internal/discord/client.go`, nobody calls `session.*`. All other files consume `*discordgo.Channel`, `*discordgo.Message`, `*discordgo.Member`, `*discordgo.User`, `*discordgo.Guild` as plain data structs with JSON tags.

**Implication:** `UserClient` does not need to invent new types. It unmarshals Discord REST JSON directly into `discordgo.Channel`, `discordgo.Message`, etc. The `syncer` layer is completely untouched.

**Cost:** we keep `bwmarrin/discordgo` in `go.mod` as a *types-only dependency*, even though the `UserClient` never creates a `Session`. This is fine — discordgo's types are stable.

---

## 4. The `discord.Client` interface

Extracted from current `client.go`, unchanged signatures:

```go
// internal/discord/client.go
package discord

import (
    "context"
    "github.com/bwmarrin/discordgo"
)

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

// NEW methods specific to fork (added to interface):
type UserCapableClient interface {
    Client
    PrivateChannels(ctx context.Context) ([]*discordgo.Channel, error)         // DM list
    GuildMessagesSearch(ctx context.Context, guildID string, q SearchQuery) ([]*discordgo.Message, int, error)
}
```

The `EventHandler` interface stays byte-for-byte identical, so `syncer/tail.go` is untouched.

**Two implementations:**
- `discord/botclient` — wraps current discordgo code, zero behavior change
- `discord/userclient` — new, built from scratch

`config.Config.Discord` gains a new field:
```toml
[discord]
mode = "user"   # "bot" (default) | "user"
```

`main.go` selects the implementation based on this flag.

---

## 5. UserClient — REST layer

### 5.1 Transport

```go
type httpTransport struct {
    token       string
    ua          string
    superProps  string   // base64(json)
    locale      string
    client      *http.Client
    limiter     *rateLimiter
    proxy       string   // optional
}
```

**Headers on every request:**
```
Authorization: <token>                    // no "Bot " prefix
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ...
X-Super-Properties: <base64-encoded JSON>
X-Discord-Locale: en-US
Accept-Language: en-US,en;q=0.9
Accept: */*
Content-Type: application/json            // when body present
Origin: https://discord.com
Referer: https://discord.com/channels/@me
Sec-Fetch-Dest: empty
Sec-Fetch-Mode: cors
Sec-Fetch-Site: same-origin
```

**X-Super-Properties** payload (captured from a real Discord web client, fields approximated):
```json
{
  "os": "Mac OS X",
  "browser": "Chrome",
  "device": "",
  "system_locale": "en-US",
  "browser_user_agent": "<same as User-Agent>",
  "browser_version": "131.0.0.0",
  "os_version": "10.15.7",
  "referrer": "",
  "referring_domain": "",
  "referrer_current": "",
  "referring_domain_current": "",
  "release_channel": "stable",
  "client_build_number": 350000,
  "client_event_source": null
}
```

All configurable via `config.toml` under `[discord.user]` so the user can refresh without rebuild when Discord bumps `client_build_number`.

### 5.2 Rate limiter

```go
type rateLimiter struct {
    globalBucket     *bucket         // 50 req / 1s global (Discord doc)
    routeBuckets     map[string]*bucket  // per route from X-RateLimit-Bucket header
    conservativeGap  time.Duration   // 1s default
    jitter           time.Duration   // 500ms–2s random
}
```

- **Strict 429 handling:** on 429, read `retry_after` (seconds, float), sleep that + jitter.
- **Cloudflare ban (1015):** on response body containing `cf-ray`, stop entire sync for 24h and emit loud error.
- **403/401:** stop entirely, never auto-retry (likely token invalidated).
- **Conservative mode (default):** min 1s between requests regardless of bucket state.

### 5.3 Endpoints

| Method | Path | Purpose | Notes |
|---|---|---|---|
| GET | `/users/@me` | Self identity | Replaces `session.User("@me")` |
| GET | `/users/@me/guilds?limit=200&after=...` | Guild list | Paginated |
| GET | `/users/@me/channels` | DM list | **User-only, new endpoint** |
| GET | `/guilds/{id}` | Guild details | |
| GET | `/guilds/{id}/channels` | Guild channel list | |
| GET | `/guilds/{id}/threads/active` | Active threads | |
| GET | `/channels/{id}/threads/active` | Channel-scoped active threads | |
| GET | `/channels/{id}/threads/archived/public?before=...&limit=100` | Archived threads | Paginated |
| GET | `/channels/{id}/threads/archived/private?before=...&limit=100` | Private archived threads | |
| GET | `/guilds/{id}/members?limit=1000&after=...` | Member list | **Note**: user-token access is limited, may fail for large guilds. Fall back to `/guilds/{id}/members/search` or skip. |
| GET | `/channels/{id}/messages?limit=100&before=...&after=...` | Message crawl | Standard |
| GET | `/channels/{id}/messages/{id}` | Single message fetch | |
| GET | `/guilds/{id}/messages/search?channel_id=...&offset=...&limit=25` | **User-exclusive** | ⭐ Shortcut — avoids full crawl |

**All responses are JSON → `json.Unmarshal` into `discordgo.*` types.**

### 5.4 Search-first sync strategy (optimization)

Traditional sync walks every channel in chunks of 100 messages. With user token we can use `/guilds/{id}/messages/search?channel_id=X` which returns messages sorted newest-first with `total_results`. We paginate via `offset` (max 5000) and then fall back to crawl for the tail beyond offset 5000.

**Benefit:** fewer requests for incremental syncs, because search supports filters like `min_id` / `max_id` which skip already-archived ranges.

**Opt-in flag:** `--strategy=search` (default still `crawl` for safety).

---

## 6. UserClient — Gateway layer

### 6.1 WebSocket setup

```
wss://gateway.discord.gg/?encoding=json&v=10&compress=zlib-stream
```

- `gorilla/websocket` (already a dependency).
- **zlib-stream** inflate: single `zlib.NewReader` instance reused across frames, fed by a ring buffer. Reset only on `INVALID_SESSION`.
- Read loop in goroutine, write loop in goroutine (writes go through a channel to serialize).

### 6.2 Identify payload (op 2)

User-mode Identify differs from bot Identify. **No `intents`**, instead:

```json
{
  "op": 2,
  "d": {
    "token": "<user token>",
    "capabilities": 16381,
    "properties": {
      "os": "Mac OS X",
      "browser": "Chrome",
      "device": "",
      "system_locale": "en-US",
      "browser_user_agent": "...",
      "browser_version": "131.0.0.0",
      "os_version": "10.15.7",
      "referrer": "",
      "referring_domain": "",
      "referrer_current": "",
      "referring_domain_current": "",
      "release_channel": "stable",
      "client_build_number": 350000,
      "client_event_source": null
    },
    "presence": {
      "status": "unknown",
      "since": 0,
      "activities": [],
      "afk": false
    },
    "compress": false,
    "client_state": {
      "guild_versions": {},
      "highest_last_message_id": "0",
      "read_state_version": 0,
      "user_guild_settings_version": -1,
      "user_settings_version": -1,
      "private_channels_version": "0",
      "api_code_version": 0
    }
  }
}
```

The `capabilities` bitmask changes across Discord releases — keep it configurable.

### 6.3 Heartbeat

- Read `hello.d.heartbeat_interval` from op 10.
- Send `{"op": 1, "d": last_sequence}` every `interval * jitter(0.1, 0.9)` ms.
- On missed ACK (no op 11 within 2 intervals), close and resume.

### 6.4 Resume

On disconnect:
```json
{"op": 6, "d": {"token": "...", "session_id": "...", "seq": <last>}}
```

Resume URL from `READY.resume_gateway_url`. If resume fails (op 9 INVALID_SESSION with `d:false`), re-identify with fresh backoff (5–30s).

### 6.5 Lazy guild requests (op 14)

To receive member updates in channels we care about (without the bot-only GUILD_MEMBERS intent), send:
```json
{
  "op": 14,
  "d": {
    "guild_id": "...",
    "typing": true,
    "threads": true,
    "activities": true,
    "members": [],
    "channels": {
      "<channel_id>": [[0, 99], [100, 199]]
    },
    "thread_member_lists": []
  }
}
```
This is how the Discord web client requests member lists on-demand. Throttle to max 1 op-14 per 3s per guild.

### 6.6 Event dispatch

Gateway op 0 (DISPATCH) events we care about:
- `READY` → extract `session_id`, `resume_gateway_url`, `user.id`, `private_channels`
- `MESSAGE_CREATE`, `MESSAGE_UPDATE`, `MESSAGE_DELETE`
- `CHANNEL_CREATE`, `CHANNEL_UPDATE`, `CHANNEL_DELETE`
- `GUILD_MEMBER_ADD`, `GUILD_MEMBER_UPDATE`, `GUILD_MEMBER_REMOVE`
- `GUILD_CREATE` / `GUILD_DELETE`
- `MESSAGE_REACTION_ADD`, `MESSAGE_REACTION_REMOVE` (new in fork — for reaction history)

Each event JSON payload → unmarshal into `discordgo.MessageCreate`, etc. → dispatch to `EventHandler` (unchanged interface).

### 6.7 Read-only enforcement

**Hard guard** at the HTTP transport layer:
```go
func (t *httpTransport) Do(req *http.Request) (*http.Response, error) {
    if req.Method != "GET" {
        // Allow-list: only ACK read state, which is a POST
        if !isAllowedWrite(req) {
            return nil, errors.New("discrawl-me is read-only: method not allowed")
        }
    }
    return t.client.Do(req)
}
```

And in the Gateway layer, whitelist only op codes 1 (heartbeat), 2 (identify), 6 (resume), 14 (lazy). Refuse to send anything else.

---

## 7. Schema additions

```sql
-- Schema version bump: 1 → 2

-- 7.1 DM channel details (channel table already stores them but we want explicit flag + recipients)
alter table channels add column is_dm integer not null default 0;
alter table channels add column dm_recipients_json text;

-- 7.2 Message edit history (preserved across edits)
create table if not exists message_edits (
    message_id text not null,
    edited_at  text not null,
    content    text not null,
    raw_json   text not null,
    primary key (message_id, edited_at)
);

-- 7.3 Reaction events (timeline)
create table if not exists reaction_events (
    event_id    integer primary key autoincrement,
    message_id  text not null,
    channel_id  text not null,
    guild_id    text,
    user_id     text not null,
    emoji_name  text not null,
    emoji_id    text,
    action      text not null,   -- 'add' | 'remove'
    event_at    text not null
);
create index if not exists idx_reactions_message_id on reaction_events(message_id);
create index if not exists idx_reactions_user_id    on reaction_events(user_id, event_at);

-- 7.4 Message embeddings
create table if not exists message_embeddings (
    message_id text primary key,
    model      text not null,
    dim        integer not null,
    vec        blob not null,     -- float32 little-endian, len = dim * 4
    created_at text not null
);
create index if not exists idx_embeddings_model on message_embeddings(model);

-- 7.5 Conversation turns (for embedding granularity)
-- A "turn" = consecutive messages by same author in same channel within 5 min
create table if not exists turns (
    id              integer primary key autoincrement,
    channel_id      text not null,
    author_id       text,
    start_ts        text not null,
    end_ts          text not null,
    message_ids_json text not null,
    text            text not null
);
create index if not exists idx_turns_channel on turns(channel_id, start_ts);

create table if not exists turn_embeddings (
    turn_id    integer primary key,
    model      text not null,
    dim        integer not null,
    vec        blob not null,
    created_at text not null,
    foreign key (turn_id) references turns(id) on delete cascade
);
```

### 7.6 Vector similarity without sqlite-vec

The current DB driver is `modernc.org/sqlite` (pure Go, no C extensions → sqlite-vec cannot load). Options:

**Option A (chosen for v1):** Store vectors as BLOB and do cosine similarity in Go over candidate set filtered by FTS5.
- FTS5 narrows to ~1000 candidates.
- Go loop computes cosine over those, SIMD via `gonum.org/v1/gonum` or hand-rolled.
- Feasible up to ~500k messages with sub-500ms query time.

**Option B (future):** Switch driver to `mattn/go-sqlite3` + load `sqlite-vec` extension. CGO required. Deferred to v2.

**Option C (future):** External vector store (LanceDB, Qdrant). Over-engineered for personal use. Deferred.

---

## 8. Embedder worker

```go
// internal/embedder/embedder.go
package embedder

type Provider interface {
    Name() string
    Dim() int
    Embed(ctx context.Context, texts []string) ([][]float32, error)
}

type Worker struct {
    store     *store.Store
    provider  Provider
    batchSize int
    interval  time.Duration
}

func (w *Worker) Run(ctx context.Context) error {
    tick := time.NewTicker(w.interval)
    for {
        select {
        case <-ctx.Done():
            return nil
        case <-tick.C:
            if err := w.processBatch(ctx); err != nil {
                log.Printf("embed batch: %v", err)
            }
        }
    }
}

func (w *Worker) processBatch(ctx context.Context) error {
    jobs, err := w.store.ClaimEmbeddingJobs(ctx, w.batchSize)  // SELECT ... UPDATE state=processing
    if err != nil || len(jobs) == 0 {
        return err
    }
    texts := collectTexts(jobs)
    vecs, err := w.provider.Embed(ctx, texts)
    if err != nil {
        w.store.ReleaseEmbeddingJobs(ctx, jobs, err)
        return err
    }
    return w.store.StoreEmbeddings(ctx, jobs, vecs, w.provider.Name(), w.provider.Dim())
}
```

### 8.1 Providers

```go
// internal/embedder/ollama.go
type OllamaProvider struct {
    endpoint string  // http://localhost:11434
    model    string  // nomic-embed-text (768d), mxbai-embed-large (1024d)
}

// internal/embedder/openai.go
type OpenAIProvider struct {
    apiKey string
    model  string  // text-embedding-3-small (1536d)
}

// internal/embedder/voyage.go
type VoyageProvider struct {
    apiKey string
    model  string  // voyage-3 (1024d)
}
```

Default: Ollama with `nomic-embed-text` (local, free, 768d, fast).

### 8.2 Chunking strategy

For messages > 512 tokens, split at sentence boundaries with 50-token overlap. For turns, concatenate but skip if total > 2048 tokens (probably not a real conversation, maybe a code paste).

### 8.3 CLI commands

```
discrawl-me embed run          # one-shot: process all pending
discrawl-me embed daemon       # run worker as daemon
discrawl-me embed status       # show backlog, model, last run
discrawl-me embed reindex      # wipe all embeddings and re-enqueue
```

---

## 9. Hybrid search

```go
// internal/store/query.go (extended)

func (s *Store) HybridSearch(ctx context.Context, q HybridQuery) ([]SearchResult, error) {
    // 1. FTS5 candidates (top 1000 by BM25)
    candidates := s.ftsSearch(ctx, q.Text, 1000)

    // 2. Embed query
    qvec := q.QueryEmbedding

    // 3. Load candidate embeddings
    cvecs := s.loadEmbeddings(ctx, candidateIDs(candidates))

    // 4. Cosine similarity
    scored := cosineRank(qvec, cvecs)

    // 5. Reciprocal Rank Fusion
    return rrf(candidates, scored, q.Alpha), nil
}
```

**CLI:**
```
discrawl-me search "how did we fix the auth bug"         # hybrid (default)
discrawl-me search --mode=fts "auth bug"                 # FTS only
discrawl-me search --mode=vector "..."                   # vector only
discrawl-me search --guild=foo --channel=bar --author=@me --after=2026-01-01 "..."
```

---

## 10. MCP server

### 10.1 Transport

Stdio JSON-RPC per MCP spec. Runs via `discrawl-me mcp` subcommand. No network, no auth — meant to be invoked locally by Claude Code.

### 10.2 Tools exposed

| Tool | Params | Returns |
|---|---|---|
| `list_guilds` | — | `[{id, name, channel_count, message_count}]` |
| `list_channels` | `guild_id` | `[{id, name, type, topic, message_count}]` |
| `search_messages` | `query`, `guild_id?`, `channel_id?`, `author_id?`, `date_range?`, `mode?` (fts/vector/hybrid), `limit?` | `[{message_id, content, author, channel, created_at, score, excerpt}]` |
| `get_message` | `message_id` | full message with edit history, reactions, attachments |
| `get_conversation` | `message_id`, `before?`, `after?` (default 20 each) | window of messages around the target |
| `find_similar` | `message_id`, `limit?` (default 10) | top-k by cosine similarity |
| `summarize_channel` | `channel_id`, `start`, `end` | aggregates messages in range and returns them as context (LLM summarizes on the caller side) |
| `user_profile` | `user_id` | activity stats, top channels, recent messages |
| `run_sql` | `query` | read-only SQL; refused if not `SELECT` or `WITH` |

### 10.3 Resources exposed

```
discord://guild/{guild_id}
discord://guild/{guild_id}/channel/{channel_id}
discord://message/{message_id}
```

Clients can cite messages by URI.

### 10.4 Integration test

Include a `cmd/discrawl-me/mcp_test.go` that spawns the server, sends a few JSON-RPC calls over stdio pipes, validates responses.

---

## 11. Config additions

```toml
version = 2
db_path = "/Users/ivan/.discrawl-me/discrawl.db"
cache_dir = "/Users/ivan/.discrawl-me/cache"
log_dir = "/Users/ivan/.discrawl-me/logs"

[discord]
mode = "user"                    # NEW: "bot" | "user"
token_source = "openclaw"        # unchanged
openclaw_config = "/Users/ivan/.openclaw/openclaw.json"
account = "ivan-personal"

[discord.user]                   # NEW section, only used when mode = "user"
client_build_number = 350000     # bump when Discord web client updates
browser_version = "131.0.0.0"
user_agent = "Mozilla/5.0 ..."
locale = "en-US"
proxy = ""                       # optional SOCKS5/HTTP proxy
min_request_gap_ms = 1000        # conservative rate limit
jitter_ms_min = 500
jitter_ms_max = 2000
read_only_strict = true          # refuse all writes except ACK

[sync]
concurrency = 1                  # CRITICAL: lower default for user mode
strategy = "crawl"               # "crawl" | "search"
include_dms = true               # NEW
guild_whitelist = []             # NEW: if non-empty, only these guilds
guild_blacklist = []             # NEW
channel_blacklist = []           # NEW

[search]
default_mode = "hybrid"          # "fts" | "vector" | "hybrid"

[search.embeddings]
enabled = true
provider = "ollama"              # "ollama" | "openai" | "voyage"
model = "nomic-embed-text"
endpoint = "http://localhost:11434"
api_key_env = ""                 # for openai/voyage
batch_size = 32

[mcp]
enabled = true
stdio = true
```

---

## 12. File-by-file change map

### Files to create (new)

| Path | Purpose | Est. LOC |
|---|---|---|
| `internal/discord/client.go` | Interface definition (extract from current file) | 50 |
| `internal/discord/botclient/client.go` | Move current impl here, rename type | 400 |
| `internal/discord/userclient/client.go` | UserClient struct + REST methods | 500 |
| `internal/discord/userclient/transport.go` | HTTP transport, headers, rate limiter | 300 |
| `internal/discord/userclient/gateway.go` | WebSocket + identify + dispatch | 600 |
| `internal/discord/userclient/gateway_frames.go` | zlib-stream decode, frame parsing | 150 |
| `internal/discord/userclient/lazy_guild.go` | Op 14 lazy guild requests | 100 |
| `internal/discord/userclient/superprops.go` | Build X-Super-Properties | 80 |
| `internal/discord/userclient/readonly.go` | Write guard | 60 |
| `internal/discord/userclient/client_test.go` | Mock HTTP server + fixture tests | 400 |
| `internal/store/migrations.go` | Schema version 1 → 2 migration | 150 |
| `internal/store/reactions.go` | Reaction CRUD | 100 |
| `internal/store/edits.go` | Edit history CRUD | 80 |
| `internal/store/embeddings.go` | Embedding CRUD + cosine | 200 |
| `internal/store/turns.go` | Turn construction + CRUD | 150 |
| `internal/embedder/embedder.go` | Worker loop | 150 |
| `internal/embedder/ollama.go` | Ollama provider | 80 |
| `internal/embedder/openai.go` | OpenAI provider | 80 |
| `internal/embedder/voyage.go` | Voyage provider | 80 |
| `internal/embedder/chunk.go` | Chunking strategy | 100 |
| `internal/mcp/server.go` | JSON-RPC stdio loop | 200 |
| `internal/mcp/tools.go` | Tool implementations | 400 |
| `internal/mcp/resources.go` | Resource handlers | 150 |
| `cmd/discrawl-me/mcp.go` | `mcp` subcommand wiring | 60 |
| `cmd/discrawl-me/embed.go` | `embed` subcommand wiring | 80 |
| `DESIGN_FORK.md` | This document | — |
| `README.md` | Fork-specific README with disclaimers | — |

**New code total: ~5000 LOC** (plus tests).

### Files to modify (minimal)

| Path | Change |
|---|---|
| `go.mod` | Module path → `github.com/giveme11is/discrawl-me` |
| `internal/discord/client.go` | Extract interface, delete struct impl (moved) |
| `internal/config/config.go` | Add `mode`, `[discord.user]`, DM filters, embedding provider fields |
| `internal/cli/cli.go` | Wire `botclient.New` vs `userclient.New` based on `mode` |
| `internal/cli/admin_commands.go` | Add `embed`, `mcp`, `search --mode` flags |
| `cmd/discrawl/main.go` → `cmd/discrawl-me/main.go` | Rename + version strings |
| `internal/syncer/channel_catalog.go` | Recognize DM channel types (1, 3) when `include_dms=true` |
| `internal/syncer/tail.go` | Subscribe to reaction events |
| `internal/syncer/message_sync.go` | Write edit history to `message_edits`, enqueue embedding jobs |
| `internal/store/store.go` | Bump `storeSchemaVersion = 2`, apply migrations |
| `internal/store/write.go` | Write reactions, edits, turns |
| `internal/store/query.go` | Add `HybridSearch`, `FindSimilar`, `GetConversation` |

**Modified files: ~12, total diff ~1500 LOC.**

### Files unchanged

Everything else: `cli/output.go`, `cli/helpers.go`, `cli/messages.go`, `cli/mentions.go`, `cli/query_sync.go`, `store/members_profile.go`, `store/mentions.go`, `store/messages.go`, `syncer/enrichment.go`, `syncer/records.go`, `syncer/errors.go`, `syncer/syncer.go` (mostly), etc.

---

## 13. Implementation order (PRs)

Even in a hard fork, we still land changes as coherent commits/PRs for reviewability.

### PR 1 — Interface refactor (no behavior change)
- Extract `discord.Client` interface
- Move current impl to `botclient/`
- Update all callers to use interface
- All tests pass unchanged
- **Acceptance:** `go test ./...` green, `discrawl-me sync` works identically to upstream

### PR 2 — Fork rename + config v2
- Rename module, binary, config dir, env vars
- Add `version = 2` config with migration from v1
- Add `mode = "bot"` as default
- **Acceptance:** existing OpenClaw bot config still works

### PR 3 — UserClient REST (read-only, no Gateway yet)
- HTTP transport with headers, rate limiter, read-only guard
- All REST endpoints from interface
- Unit tests with httptest mock server
- **Acceptance:** `discrawl-me sync --mode=user --dry-run` lists guilds/channels/messages without Gateway

### PR 4 — DM support
- `PrivateChannels()` method
- Schema migration for `is_dm` flag
- Syncer recognizes DM channel types
- **Acceptance:** `discrawl-me sync --include-dms` archives DMs from user account

### PR 5 — UserClient Gateway
- WebSocket client, zlib-stream, identify, heartbeat, resume
- Event dispatch to existing `EventHandler`
- Lazy guild op 14
- **Acceptance:** `discrawl-me tail --mode=user` receives live events

### PR 6 — Reactions + edit history
- Schema migration for `reaction_events`, `message_edits`
- Gateway handlers for reaction add/remove
- Syncer writes edit history on MESSAGE_UPDATE
- **Acceptance:** DB contains reaction timeline and edit trail

### PR 7 — Embedder worker
- `internal/embedder` with Ollama provider
- `embed run` / `embed daemon` commands
- Schema migration for `message_embeddings`, `turns`
- Turn construction from messages
- **Acceptance:** `discrawl-me embed run` produces vectors for backlog

### PR 8 — Hybrid search
- `HybridSearch` in store
- `search --mode=hybrid` CLI
- Cosine similarity in pure Go
- **Acceptance:** semantic queries return relevant results

### PR 9 — MCP server
- `internal/mcp` stdio JSON-RPC
- Tool implementations
- Resource URIs
- `discrawl-me mcp` command
- **Acceptance:** Claude Code can list guilds, search messages, get conversations via MCP

### PR 10 — Hardening
- Proxy support
- Warmup heuristics (randomized initial delays)
- Encrypted DB at rest (optional, via sqlcipher or file-level)
- GDPR delete command
- README + safety docs
- **Acceptance:** all safety features functional

---

## 14. Testing strategy

### 14.1 Unit tests
- `userclient/transport_test.go` — mock HTTP, verify headers, rate limiter, 429 handling, read-only guard
- `userclient/gateway_test.go` — in-process WebSocket server emitting fixture frames, verify dispatch
- `userclient/superprops_test.go` — snapshot test on generated JSON
- `embedder/*_test.go` — mock providers
- `store/*_test.go` — sqlite in temp dir, migrations, hybrid search
- `mcp/*_test.go` — stdio round-trip

### 14.2 Integration tests
- `integration/sync_user_mode_test.go` — against a **test guild** owned by a throwaway account (gated by env var `DISCRAWL_ME_TEST_TOKEN`)
- `integration/mcp_test.go` — spawn MCP subprocess, send real queries

### 14.3 Fuzz/stress
- Rate limiter under burst load
- zlib-stream decoder with corrupted frames
- JSON parser with malformed messages

### 14.4 Manual smoke test checklist
1. `init` with user token
2. `sync --dry-run` lists guilds and DMs
3. `sync` a small guild
4. `tail` for 60s, verify live events
5. `embed run`, verify backlog drains
6. `search` in hybrid mode
7. `mcp` launched from Claude Code, list tools

---

## 15. Open questions (to decide during impl)

1. **client_build_number auto-refresh:** Discord bumps it every few weeks. Auto-detect by scraping `https://discord.com/assets/...` build manifest, or leave manual?
2. **Encrypted DB:** worth the complexity or rely on FileVault?
3. **Whisper/OCR:** defer to v2 or include in v1 as optional build tag?
4. **Multi-account:** support archiving from multiple user tokens in one DB, or separate DBs per account?
5. **Session resume persistence:** store `session_id` + `sequence` to disk so a restart resumes instead of re-identifying?
6. **Graceful degradation:** if `GUILD_MEMBERS` search fails, skip member archive or fail loudly?

---

## 16. Success metrics (v1)

- Archives a 10k-message DM history in < 30 min without ban.
- Archives a 100k-message guild in < 4h with `concurrency=1`.
- Hybrid search returns relevant results for fuzzy queries ("the auth bug conversation").
- MCP server responds to `search_messages` in < 500ms for a 500k-message DB.
- Zero posts/writes emitted over the lifetime of the process (verified by transport audit log).
- README clearly communicates ToS risk; no user can claim they weren't warned.

---

## Appendix A — Why not discord.js / selfcord.py?

Python (`selfcord.py`, `discord.py-self`) has more mature self-bot support. But:
1. The upstream is Go — rewriting in Python loses the schema, FTS5 integration, and existing test coverage.
2. Go's deployment story (single static binary) is better for a personal tool you want to run on a Pi or server.
3. The modernc.org/sqlite pure-Go driver means zero CGO headaches.
4. MCP server in Go is straightforward with stdio.

## Appendix B — References

- Discord API docs: https://discord.com/developers/docs (bot-focused but REST endpoints are same)
- Discord web client reverse-engineering notes (community): search for "discord-api-docs user endpoints"
- Gateway v10 spec: https://discord.com/developers/docs/topics/gateway
- MCP spec: https://modelcontextprotocol.io
- Similar projects for reference (Python): discord.py-self, selfcord.py
- discordgo source: https://github.com/bwmarrin/discordgo

## Appendix C — Legal / ethical stance

This fork exists for **personal archival of one's own Discord history**, including servers and DMs the user is a legitimate member of. It does not:
- Scrape content the user cannot already see.
- Mass-harvest data across accounts.
- Enable automated moderation or engagement.
- Post, react, or modify anything on Discord.

The fork is **read-only by design** (enforced at transport layer). Discord's ToS §II.E prohibits self-bots regardless of intent; using this tool means accepting that account suspension is possible. The README and CLI must display a disclaimer on first run.

---

*Document version: 1.0 — initial design after audit of upstream at commit HEAD*
