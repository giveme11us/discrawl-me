# AGENTS.md

## Vault bridge
Durable discrawl-me context lives in Ivan's Obsidian vault, not only in this repository.
Before planning non-trivial work, read:

- `/Users/ivansposato/Documents/Vault/AGENTS.md` — shared agent rules
- `/Users/ivansposato/Documents/Vault/Work/Projects/discrawl-me/_index.md` — discrawl-me source-of-truth overview
- `/Users/ivansposato/Documents/Vault/Work/Projects/discrawl-me/design.md` — architecture/design when touching storage, sync, MCP, embeddings, or user-token flow

If code work changes durable decisions, architecture, token handling, sync behavior, schema, MCP tools, or search semantics, update the relevant vault note as part of handoff.
Do not copy secrets, Discord tokens, DB contents, or private message excerpts from the vault/repo into chat/output.

## Scope
This file defines Codex behavior for `discrawl-me`.

## Product constraints
- Treat the project as read-only Discord archival/search infrastructure unless Ivan explicitly asks for write behavior.
- User-token/self-bot behavior is ToS-sensitive; preserve explicit warnings and avoid normalizing it as safe.
- Keep token handling isolated to config/runtime; never commit tokens or extracted private message content.
- SQL tools exposed through MCP must remain SELECT/read-only unless Ivan explicitly changes the design.

## Implementation rules
- Prefer focused Go changes with tests around sync, store, embedder, and MCP behavior.
- Preserve SQLite/FTS/vector migration compatibility; schema changes need a clear migration path.
- Keep upstream fork context in mind, but Ivan's local design/vault notes win over upstream assumptions.

## Validation
- Run stack-appropriate Go tests for touched packages before finalizing.
- If external services are required (Discord Gateway, Ollama/OpenAI/Voyage), report exact blockers instead of faking runtime validation.
