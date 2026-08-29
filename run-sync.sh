#!/bin/bash
# Periodic REST sync for discrawl-me.
#
# Replaces the persistent `tail` collector. A user-token Gateway session
# announces a presence for the account, and Discord withholds that account's
# own mobile push notifications for as long as it is connected (verified
# 2026-08-29: stopping the tail restored notifications within minutes).
#
# `sync` never opens the Gateway — it only issues REST GETs — so this archives
# the same content while leaving mobile notifications intact. The process runs
# and exits; launchd restarts it on StartInterval.
set -euo pipefail

repo_dir="/Users/ivansposato/GitHub/tools/discrawl-me"
config_file="/Users/ivansposato/.config/discrawl-me/config.toml"
token_file="/Users/ivansposato/.config/discrawl-me/token"
guild_id="422855027535642625"

# Never overlap with a long backfill, and never overlap with a previous run
# that is still going — concurrency=1 plus rate limiting makes a full pass slow.
if pgrep -f "discrawl-me --config ${config_file} sync" >/dev/null; then
  echo "A sync is already running; skipping this interval."
  exit 0
fi

if [[ ! -r "$token_file" ]]; then
  echo "Discord token is not configured." >&2
  exit 1
fi

DISCORD_USER_TOKEN="$(<"$token_file")"
export DISCORD_USER_TOKEN

exec "$repo_dir/discrawl-me" --config "$config_file" sync --guild "$guild_id"
