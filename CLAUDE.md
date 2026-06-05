# discrawl-me — Claude bridge

Durable discrawl-me context lives in Ivan's Obsidian vault, not only in this repository.
Before planning non-trivial work, read:

- `/Users/ivansposato/Documents/Vault/AGENTS.md` — shared agent rules
- `/Users/ivansposato/Documents/Vault/Work/Projects/discrawl-me/_index.md` — discrawl-me source-of-truth overview
- `/Users/ivansposato/Documents/Vault/Work/Projects/discrawl-me/design.md` — architecture/design when touching storage, sync, MCP, embeddings, or user-token flow

If code work changes durable decisions, architecture, token handling, sync behavior, schema, MCP tools, or search semantics, update the relevant vault note as part of handoff.
Do not copy secrets, Discord tokens, DB contents, or private message excerpts from the vault/repo into chat/output.

## Project posture

`discrawl-me` is Ivan's personal Discord archive/search/MCP fork. It is ToS-sensitive because it supports user-token/self-bot archival; keep warnings visible and preserve read-only behavior by default.

## Local workflow

- Prefer focused Go changes with tests around touched packages.
- Keep SQLite/FTS/vector migrations compatible and explicit.
- Do not treat upstream `discrawl` assumptions as source of truth when they conflict with Ivan's vault design.
- If external services are required for validation, report the blocker exactly instead of fabricating runtime output.
