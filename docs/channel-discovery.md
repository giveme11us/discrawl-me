# Channel discovery in the existing collector

Status: local candidate for independent review. No deployment or live Discord
validation is implied. Only the giveme11us/discrawl-me fork is used.

## Native metadata phase

`discrawl-me --config CONFIG --json --quiet discover --guild GUILD_ID`

This is a new command in the SAME collector. It uses the existing Discord
client/proxy and store: guild channel metadata, the guild active-thread route,
then channel upserts and a separate `discovery:GUILD:last_success` marker.
It fetches no messages, members, DMs, attachments or Gateway events. It does
write metadata to the configured archive, so this is not a read-only DB command.
A guild must be explicit; there is no `--all` fallback.

The JSON distinguishes current `channels` from public `cached_threads` that
were not observed this cycle. Missing catalog IDs are not deleted. Active-route
failure is `coverage.active_threads=unavailable`, not success/full coverage.
`--thread-parents CSV` scans public active threads AND public archives for those
current text, news or forum parents only. These per-parent routes recover posts
missing from the guild active list, including an already archived public post
never cached. Private archives are not requested; private threads returned by
any route are discarded. `coverage.parent_active_threads` and `public_archives`
record each requested parent's observed/unavailable status. Missing keys mean
not requested, not success. `unavailable_reasons` records only bounded
`http_STATUS`, cancellation/deadline or `request_failed` reasons; raw error
bodies/URLs are never copied into coverage. A status does not identify why the
API/identity denied access. `coverage.complete` always remains false; successful
routes mean observations, not a guarantee of all accessible threads.
The runner enables these scans in the metadata phase for admitted parents only.
The discovery-owned plan is the only message allowlist for EasyCop/Dustify;
implicit selected-forum crawling is disabled for these invocations.

## Runner sequence and policy

The existing run-selected-sync.py keeps its lifecycle lock, external-sync guard,
serial child handling, 180-second selected budget and 45-second slices.
Before any messages, it runs TWO metadata stages in EasyCop and Dustify:

1. discover current top-level/guild-active inventory in both guilds;
2. compute a provisional plan using the same manifest/category/type/role rules;
   only current admitted text/news/forum parents authorize further metadata;
3. call native `discover --thread-parents CSV` for those explicit parents in
   each guild, collecting public active/archive metadata (never messages);
4. validate the resulting current reports, rebuild the final message plan and
   atomically save `selected-sync-state.json.discovery.json`.

Top-level children retain a 120-second limit. Parent metadata children have a
bounded `min(1800, 120 + 10 * number_of_unique_parents)` seconds window, because
at least two paced requests per parent cannot fit a fixed 120-second limit for
108 parents at the native default minimum pacing. Pagination/permissions/live
duration are NOT attested by offline tests. The lock remains held throughout;
a later scheduled invocation skips rather than overlapping a long cycle. This
may delay freshness and must be measured before any authorized installation.
The 180/45-second selected-message budgets are unchanged. A parent appearing
between metadata stages is only scanned on a following cycle if still admitted.
Invalid/missing inventory, failed/timed-out parent discovery or an empty EasyCop
selection stops with exit 4, preserving the old plan and rotation state. There
is never `--channels ''` or all-Dustify fallback. Endpoint-specific denials in a
successful report retain explicit partial coverage; they are not a failed child
and do not promote cached metadata to fresh.

The APPROVED INPUT manifest remains strictly validated at 35 SD / 108 Dustify
seed channels. Those counts are not a cap or a hardcoded list of future targets.
No live manifest change is required for a new eligible channel. Sneaker
Development's exact target list is unchanged and is not dynamically discovered.

Dustify baseline targets remain admitted only with current category and type
matching the manifest. Renames within that category keep identity; category
moves/renames, duplicate category labels or type changes are held for review.

NEW Dustify targets require all of:

- a current category anchored by an existing approved seed, with the same
  normalized category name as in the manifest and no duplicate label;
- an editorial category demonstrated by the manifest (release/TCG/Pokemon/
  important), not a new category or a generic community/monitor category;
- the same channel type as approved examples in that category;
- a demonstrated editorial name role: dated item, important-*, *-releases,
  *-upcoming-leaks or *-links-info; a role not present in the category's seeds
  is not enabled;
- no topps/funko/vinyl subject in new channel/category names. Pokemon and bricks
  remain eligible through their admitted categories/roles.

The 108 legacy archive seeds include topps/funko/vinyl. They are PRESERVED as
historically approved archive coverage, not added to the editorial radar. The
later radar exclusion is not interpreted as permission to delete archive data.
No new channel in these subjects is inferred. This distinction is deliberate.
Unknown categories/roles are recorded with `collect=false` and a review reason;
there is no automatic notification or new bot/radar routine.

NEW Dustify public/news threads require an admitted parent AND pass the same
subject exclusion as new top-level targets, before inheriting that parent.
The 108 explicit archive seeds keep their existing treatment. Cached public threads have
`freshness=cached_unconfirmed`; this does not prove they still exist or are
accessible. Private threads/DMs are not newly selected. The runner passes
`sync --exact-channels` for EasyCop/Dustify: only explicitly named IDs can enter
the native catalog/message phase, whether targets are cached or resolved live.
A selected forum does NOT authorize its posts, even if a post appears after
discovery. Known eligible posts are separate explicit targets; unknown or
excluded posts cannot bypass the plan via forum ancestry. Missing explicit
threads are not resolved by a broad archive crawl during message sync. Already
archived public posts not cached are now discoverable BEFORE the plan through
the metadata-only parent routes above, including when guild-active is unavailable
or omits them. A post created after its parent's metadata scan waits for another
cycle if still observable. Private/API-denied/unreturned posts and parents not
admitted this cycle remain uncovered; cached observations are not fresh. This
restores public parent discovery without reintroducing the old implicit/private
message-phase expansion. Coverage remains partial, never complete. The native
legacy default and SD forum expansion are unchanged.
`--exact-channels` requires a non-empty channel list and DMs disabled.

EasyCop retains its all-guild editorial policy but the runner now passes an
explicit non-empty list of current text/news and known public/news thread IDs,
with `--include-dms=false`. Categories/voice/forum containers are metadata, not
message targets. Past releases are still retained for dedup, with
`room_state=historical`, distinct from `archived_thread` and
`active_or_unclassified`. A label alone is not a semantic active-release test.
`--refresh-members` on EasyCop preserves the prior native throttled member
refresh even though sync now has explicit channel IDs. SD/Dustify do not opt in.

Selection changes reconcile the runner's existing attempts/cursor and retain
historical target entries; they never reset native message/backfill checkpoints.
Fresh `last_message_id` is restored from stored channel JSON for targeted sync.
A formerly empty completed channel can bootstrap its first messages without
clearing `history_complete`. This fixes the discovered empty-channel regression.
No schema migration, deletes, message parser changes, or FTS rebuild is needed.
Text, embed fields and message_snapshots remain in native raw message JSON.

## Offline checks

- `GOTOOLCHAIN=go1.26.5 go test -count=1 -timeout 180s ./...`
- `python3 -m unittest -v test_selected_discovery.py`
- `GOTOOLCHAIN=go1.26.5 CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o OUTPUT ./cmd/discrawl`
- `python3 run-selected-sync.py --dry-run --discovery-fixture FIXTURE.json`

`--discovery-fixture` is rejected unless `--dry-run` is set. Fixture dry-run
prints exact decisions/commands but launches nothing and writes no state, log,
lock, token or DB. Without fixtures, dry-run is explicitly a seed-only preview;
it prints metadata commands and marks EasyCop selection unresolved, not a
claimed current plan. The fixture tests run the actual runner CLI in an isolated
DISCRAWL_HOME, with synthetic full-sized seed input, without live credentials.

## Application and rollback (not executed)

After independent Verify, a separately authorized live bounded cycle is still
required to check real API coverage, selection and duration. Do not enable radar,
cron, tunnels, Gateway or Tailscale as part of applying this candidate. Do not run
an extra collector alongside the existing service or bypass its lock.

Apply the collector binary and runner as a MATCHED pair during an authorized
maintenance window using the existing service management lane. Keep the current
config, manifest, archive and Linux shell wrapper; no credentials change. Before
replacement check installed hashes and preserve one verified rollback artifact
according to the owning operator's backup policy. Build from the exact reviewed
commit and record the new binary/runner hashes. Do not install a runner that
expects `discover` against an older binary.

Rollback BOTH binary and runner to their exact pre-apply versions. Do not
restore an old SQLite file, remove channel metadata, clear checkpoints or erase
new messages merely to undo collection scheduling. The old runner ignores the
additive discovery marker/sidecar plan. Candidate and rollback retain the
existing archive schema. No production install/restart is authorized by tests
or by a review verdict alone.
