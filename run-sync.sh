#!/bin/bash
# Periodic REST sync for discrawl-me.
#
# Replaces the persistent tail collector. A user-token Gateway session
# announces a presence for the account, and Discord withholds that account's
# own mobile push notifications for as long as it is connected (verified
# 2026-08-29: stopping the tail collector restored notifications within minutes).
#
# `sync` never opens the Gateway — it only issues REST GETs — so this archives
# the same content while leaving mobile notifications intact. The process runs
# and exits; launchd restarts it on StartInterval.
#
# Since 2026-09-08 this wrapper no longer execs the native binary directly. It
# execs run-selected-sync.py, which runs the same unchanged EasyCop command
# first (no time limit) and then spends a bounded budget on the approved
# selective backfill of the two new guilds. Token handling is unchanged: the
# token is read from disk into the environment and never printed.
#
# The "a sync is already running" guard moved into the scheduler, which does the
# same pgrep match inside its own flock, skips the whole cycle when another
# native sync is live, and never kills it.
set -euo pipefail

repo_dir="/Users/ivansposato/GitHub/tools/discrawl-me"
token_file="/Users/ivansposato/.config/discrawl-me/token"
# The config path (~/.config/discrawl-me/config.toml) and the guild ids now live
# in run-selected-sync.py, which builds every native command.
python_bin="/usr/bin/python3"
scheduler="$repo_dir/run-selected-sync.py"

if [[ ! -r "$scheduler" ]]; then
  echo "Selective sync scheduler is missing: $scheduler" >&2
  exit 1
fi

# --dry-run prints the planned commands only: no native launch, no token read,
# no state/log/lock writes.
for arg in "$@"; do
  if [[ "$arg" == "--dry-run" ]]; then
    exec "$python_bin" "$scheduler" "$@"
  fi
done

if [[ ! -r "$token_file" ]]; then
  echo "Discord token is not configured." >&2
  exit 1
fi

DISCORD_USER_TOKEN="$(<"$token_file")"
export DISCORD_USER_TOKEN

exec "$python_bin" "$scheduler" "$@"
