#!/bin/bash
set -euo pipefail

token_file="/Users/ivansposato/.config/discrawl-me/token"

printf 'Paste the Discord token for the throwaway/read-only archive account: '
IFS= read -r -s discord_token
printf '\n'

if [[ -z "$discord_token" ]]; then
  echo 'No token entered; nothing was changed.' >&2
  exit 1
fi

umask 077
printf '%s' "$discord_token" > "$token_file"
chmod 600 "$token_file"
unset discord_token

echo 'Token saved securely. You can close this window and return to Codex.'
