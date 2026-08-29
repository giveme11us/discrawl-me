#!/bin/bash
# DISABLED 2026-08-29 — do not use.
#
# This script started a persistent `tail`, which opens a user-token Gateway
# session. Such a session announces a presence for the account, and Discord
# withholds that account's own mobile push notifications for as long as it is
# connected. Verified: stopping the tail restored notifications within minutes.
#
# Archiving now runs through run-sync.sh + eu.easycop.discrawl-me-sync, a
# periodic one-shot REST sync that never opens the Gateway.
#
# The tool refuses live tail anyway unless discord.user.gateway = "enabled",
# so this script is kept only as a record of what used to run here.
#
# See: Vault/Work/Projects/discrawl-me/2026-08-29-gateway-notifiche-mobile.md
set -euo pipefail

cat >&2 <<'EOF'
run-tail.sh is disabled.

A persistent user-token Gateway session suppresses this account's Discord
mobile push notifications. Use run-sync.sh (LaunchAgent
eu.easycop.discrawl-me-sync) instead — it archives the same content over REST
without opening the Gateway.

To knowingly accept the trade-off, set discord.user.gateway = "enabled" in
~/.config/discrawl-me/config.toml and run the tail command directly.
EOF
exit 1
