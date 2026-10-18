# Selective memory (opt-in) — policy JSON v1

Status: LOCAL candidate (t_6e2da362). Not installed, not adopted. The default
writer and every legacy command are unchanged unless the commands/flags below
are used explicitly.

A selective memory is a discrawl-me SQLite file with the SAME schema
(user_version 3, same tables) that keeps:

- the full catalog: `guilds` (roles in raw_json), `channels`, `members`;
- EVERY `sync_state` row (latest / backfill / history_complete / rotation /
  dedup / sync:last_success ...). Cursors are never derived from the retained
  messages, so expelling payload never restarts history;
- only the messages selected by the policy, plus their attachments (metadata
  rows only, no download), edits, events, reactions and mentions;
- the `message_embeddings` vectors and `embedding_jobs` rows of the retained
  messages (byte-identical; none for expelled messages); FTS rebuilt for
  retained messages including embed fields/footer/author and forwarded
  `message_snapshots` text (raw_json kept byte-identical).

It is marked by `sync_state['memory:selective']` (JSON marker with policy
sha256, creation time, cutoff, last prune). INFO bookkeeping lives in
`memory:info_seen:<channel>` / `memory:info_left:<channel>`.

## Final policy (owner decision, 2026-10-18)

```json
{
  "version": 1,
  "guild_ids": ["539258061081018378", "1347333602794541066"],
  "rolling_days": 30
}
```

Sneaker Development + Dustify only, strict rolling 30 days, no exceptions:
`info` and `pins` are OPTIONAL (omitted or `null` = disabled; when present
they are validated exactly as below). Messages of guilds outside `guild_ids`
(EASYCOP) are dropped by export (counted in
`messages_dropped_out_of_policy_by_guild`) and by every retention pass; their
catalog rows and cursors are still copied (see "Out-of-policy guilds").
Export it with `--no-rollback-window` (marker written with the window
already closed at the export instant) so retention expels from the first
cycle after cutover.

## Policy JSON v1 (full shape, all sections)

```json
{
  "version": 1,
  "guild_ids": ["<easycop>", "<dustify>", "<sneaker-development>"],
  "rolling_days": 30,
  "info": {
    "guild_id": "<easycop>",
    "category_ids": ["1465792933512089782", "1465791988799504687", "1466013707363684488"],
    "post_transition_days": 90,
    "catalog_stale_after_hours": 72
  },
  "pins": {
    "registry_path": "/home/hermes/.hermes/easycop-copy-learning/releases.json",
    "state_path": "/home/hermes/.hermes/easycop-copy-learning/state.json"
  }
}
```

Validation is strict and fail-closed: unknown keys, non-snowflake IDs,
duplicate guilds, info guild outside `guild_ids`, empty category list,
registry/state missing or unparsable, `since` not RFC3339 **UTC** (`Z` or
`+00:00`; a bare `DD-MM` is rejected), registry guild outside policy, a pinned
channel absent from the catalog or in a different guild (cross-guild), an INFO
category that is not a category of `info.guild_id`, and source messages in a
guild not declared in `guild_ids`. Registry and state are only READ (sha256 in
every report); they are never copied, reset or rewritten.

Cutoff = `now - rolling_days` in UTC (`--now` accepts only RFC3339 UTC).

Retention decision per message (first match wins):

1. guild not in policy → not retained (export refuses such a source);
2. message id in `state.processed` → retained (pin, no auto-retire);
3. `created_at >= cutoff` → retained (rolling window);
4. channel registered in the pin registry and `created_at >= since` → retained;
5. channel currently under an INFO category (any depth, public, same guild) →
   retained for the whole stay;
6. channel previously seen under INFO (`memory:info_seen`):
   - catalog row stale (`updated_at` older than `catalog_stale_after_hours`)
     → retained (a stale cache never authorizes expulsion);
   - transition not yet confirmed by a FRESH observation → retained;
   - confirmed at T (`memory:info_left` = observation time) → retained until
     T + `post_transition_days`;
7. otherwise expired. Unparseable timestamps are retained.

## Commands (offline: no config, no token, no Discord client)

    discrawl memory export --source DB --output NEW_DB --policy POLICY [--now T] [--hash-source] [--no-rollback-window] [--enqueue-missing-embeddings]
    discrawl memory status --db DB [--policy POLICY] [--now T]
    discrawl memory prune  --db SELECTIVE_DB --policy POLICY [--now T] [--dry-run] [--vacuum]
    discrawl memory mcp    --db SELECTIVE_DB [--policy POLICY]
    discrawl memory search       --db SELECTIVE_DB [--guild G] [--channel C] [--author A] [--limit N] QUERY
    discrawl memory conversation --db SELECTIVE_DB --message-id ID [--before N] [--after N]
    discrawl memory guilds       --db SELECTIVE_DB
    discrawl memory channels     --db SELECTIVE_DB --guild G
    discrawl memory sql          --db SELECTIVE_DB "SELECT ..."
    discrawl memory similar      --db SELECTIVE_DB --message-id ID [--limit N]   (stored vectors only, no API call)
    discrawl memory search --mode vector|hybrid --embed-endpoint URL --embed-model M --embed-dim N [--embed-api-key-env VAR] QUERY
    discrawl memory reconcile --selective SELECTIVE_DB --archive PARKED_FULL_DB [--since T] [--dry-run]
    discrawl sync ... --memory-policy POLICY [--memory-prune-max N] [--with-embeddings]   (native writer, selective DB only)
    discrawl discover --guild G --thread-parents P1,P2 [--archive-pages N]

- export: source opened `mode=ro` + `query_only`, one read transaction (single
  consistent snapshot), refuses symlink/non-regular source, source == output,
  existing output or sidecars, unknown tables. Writes `<output>.partial-<pid>`,
  VACUUM + checkpoint + integrity_check, then publishes with a hard link
  (atomic no-overwrite). Any error removes the partial file: no apparently
  valid target is left. The JSON report contains only counts, sizes
  (bytes/page_size/page_count/freelist/WAL, measured), coverage reasons and
  sha256 digests — never message content. `--hash-source` hashes the source
  before and after (expensive on a large archive).
- status: `OpenReadOnly` (no creation, no chmod, no migration; schema must be
  exactly v3). With `--policy` it also reports `would_prune`.
- prune: checks schema and marker on a strictly read-only handle FIRST
  (`mode=ro`, `query_only`, no PRAGMA that writes): a DB without the marker
  (the full archive) is refused byte-identical, journal mode unchanged, no
  sidecar created. Only a verified selective DB is then opened writable; updates only `memory:*` bookkeeping; deletes expired messages with
  their dependent rows and FTS rows (except those held by an open rollback
  window, see below); asserts the non-memory `sync_state` rows
  are unchanged. `--dry-run` opens read-only.
- mcp: read-only stdio server on the selective DB. Every result is prefixed
  with the selective-memory notice ("absence ≠ nonexistence"), `memory_status`
  is exposed; `find_similar` and vector/hybrid search answer from the kept
  embeddings (only retained messages that already have a vector).
  Note: SQLite still opens `-wal`/`-shm` sidecars of a WAL-mode file.
- readers (`search|conversation|guilds|channels|sql|similar`): the same
  read-only selective handler as `mcp` (explicit `--db`, `mode=ro` +
  `query_only`, no create/migrate, no config/token/client, marker required);
  every output starts with the notice; `similar` uses stored vectors only;
  `--mode vector|hybrid` needs the explicit `--embed-*` query embedder.
- reconcile: rollback aid. Applies to a PARKED full archive (collector
  stopped) every change of the selective DB after `--since` (default: marker
  `created_at` = export snapshot time) BY ID: created or updated messages
  (edits, soft deletes/tombstones of pre-existing IDs), their attachments and
  edit history, message/reaction/mention events (deduplicated), legacy FTS
  rows, catalog rows and non-memory `sync_state` cursors. Newest `updated_at`
  wins (a newer archive row is kept); rows expired by retention are never
  deleted from the archive; one archive transaction (error = unchanged);
  refuses a marked archive, an unmarked selective DB, the same file.
  `--dry-run` opens the archive `mode=ro` and only counts.
- `sync --memory-policy`: refused on a DB without the marker. With
  `--with-embeddings` retained messages are enqueued in `embedding_jobs`
  exactly like on a full archive (dropped ones never are); `embed run`
  processes them. Never downloads assets (attachment text fetch is
  disabled for retained and dropped messages; metadata/raw are kept). After
  the sync commits, the same process runs ONE bounded retention pass
  (`--memory-prune-max`, default 5000, oldest first) that also persists INFO
  entry/exit bookkeeping from the fresh catalog: retention stays bounded inside
  the existing cycle, no extra job. Drops non-retained payload before the write
  transaction, indexes embed/snapshot text, and bounds backfill per channel
  (INFO: unbounded; pin: since; other: cutoff; with the final policy: cutoff
  everywhere). Reaching the floor stores the backfill cursor and never sets
  `history_complete`; a backfill cursor already older than the floor issues
  no request at all (`snowflakeOlderThan`), so expelled history is never
  downloaded again.
- export `--enqueue-missing-embeddings`: in the OUTPUT only, a pending
  `embedding_jobs` row for every retained message with neither a vector nor a
  job (count in `embedding_jobs_enqueued`).
- close-rollback-window: see "Rollback window" below.

## Out-of-policy guilds

Export keeps `guilds`, `channels`, `members`, `member_fts` and every
`sync_state` row of all guilds, including one removed from the policy: the
catalog is small, the cursors cannot cause downloads (the runner no longer
targets the guild and the writer drops its messages), and the invariant
"sync_state copied verbatim" used by prune/reconcile is preserved. Messages,
children, embeddings and jobs of that guild are never kept.

## Rollback window (owner decision A — NOT used by the final policy)

The final export uses `--no-rollback-window`: the marker is written
`rollback_window: "closed"` with `rollback_window_closed_at` = export instant,
so nothing is held. The guard code below is unchanged for other exports.

    discrawl memory close-rollback-window --db SELECTIVE_DB --confirm-boundary T [--now T]

Export writes `rollback_window: "open"` in the marker; the boundary is the
marker `created_at` (export snapshot). While the window is open, retention
(`memory prune` and the in-cycle pass of `sync --memory-policy`) does NOT
expel an expired message whose latest change (creation, `updated_at`, edit,
delete, attachment, message/reaction/mention event) is at/after the boundary:
it is held so that `memory reconcile` can transfer it to a parked archive
without loss. Unchanged expired messages are expelled normally. The native
writer also accepts records created/edited/deleted at/after the boundary that
the rolling policy would skip (out-of-scope guilds excepted).

- Growth is proportional to post-boundary activity for the duration of the
  window; there is no size cap and no automatic close. `memory status` (and
  the prune report) show `rollback_window`: state, boundary, held messages,
  rows per table, stored payload bytes (column lengths, excluding FTS/index/
  page overhead; file size is measured separately) and warnings.
- Conservative reading: a marker without the field, an unknown state, a
  "closed" without a valid `closed_at` or a "closed" whose boundary (marker
  `created_at`) is unusable is treated as OPEN (warning); an unusable
  boundary holds EVERY expired message and `close-rollback-window` refuses.
- Closing is explicit and separate: never done by sync, prune or the runner.
  `--confirm-boundary` must denote the marker boundary instant. Read-only
  preflight (refusals write nothing), then one transaction on the marker
  only: an error or interruption leaves the window open; replay on a closed
  window is a no-op (`already_closed: true`).
- After closing, the next retention pass expels held messages and records
  them (`expelled_post_boundary`); `memory reconcile` then refuses any
  boundary covering that loss, with no override flag. Rollback after close is
  a historical restore from the cold backup, which does not contain the
  expelled post-boundary delta.
- Not covered: delete/reaction events received by ID only (gateway tail) for
  messages never present in the selective DB are dropped as orphans.

## Policy pin state (fail-closed)

`pins.state_path` must be a JSON object with a mandatory `processed` object
mapping message ID -> non-empty fingerprint string. Missing, `null`, array,
scalar or mistyped values are errors (never an implicit revocation of pins).
Export fails without publishing when any registered processed ID is absent
from the source (`processed pin coverage incomplete (k/n)`).

## Bounded public archive discovery

`discover --archive-pages N` scans at most N pages of each explicit parent's
PUBLIC archive per run, resuming from the cursor persisted in
`sync_state` (`discovery:<guild>:public_archive:<parent>:before`) after every
page; threads of completed pages are stored immediately. Coverage per parent:
`observed` (pass complete, cursor cleared), `partial` (cursor kept),
`unavailable` (request error, cursor kept). Private archives are never
requested; private threads are never stored. Without the flag the legacy
single call is unchanged.

## Limits

- FTS of the selective DB includes embed/snapshot text. A FUTURE bump of
  `messageFTSVersion` would rebuild FTS from `normalized_content` only.
- `get_conversation` now coalesces missing neighbours (sparse channels) instead
  of returning a false empty result.
- Tail/gateway edits of an already-expelled message are dropped by the same
  write policy.
