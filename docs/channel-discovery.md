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
`--thread-parents CSV` optionally scans public archives for those current text,
news or forum parents only. Private archives are not requested. Each archive
parent has its own observed/unavailable coverage result. `coverage.complete`
remains false: API identity/access and unsurveyed parents prevent a full claim.
The runner does not enable archive scans in the metadata phase by default.
Existing native selected-forum crawling remains in the message phase.

## Runner sequence and policy

The existing run-selected-sync.py keeps its lifecycle lock, external-sync guard,
serial child handling, 180-second selected budget and 45-second slices.
Before any messages, it now runs metadata discovery for EasyCop and Dustify
(maximum 120 seconds per discovery child), validates both JSON reports, then
atomically saves `selected-sync-state.json.discovery.json`. Invalid/missing
inventory or an empty EasyCop selection stops with exit 4, preserving the old
plan and rotation state. There is never `--channels ''` or all-Dustify fallback.

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

Public/news threads inherit only an admitted parent. Cached public threads have
`freshness=cached_unconfirmed`; this does not prove they still exist or are
accessible. Private threads/DMs are not newly selected. Forum-parent targets keep
the native public-post expansion already present in the collector; a parent
slice's exit zero is not proof that all its posts were discovered or completed.

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
