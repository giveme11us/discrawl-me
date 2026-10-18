#!/bin/bash
# Periodic REST sync for discrawl-me — versione container (LXC hermes-gw).
#
# Gemella di run-sync.sh sul Mac, con due differenze che contano.
#
# 1. Il traffico Discord esce dal SOCKS5 sulla 1080, aperto dal Mac verso qui
#    (LaunchAgent eu.easycop.discrawl-me-exit-tunnel). Il token utente ha sempre
#    fatto login da casa; un login dall'IP del datacenter e' il pattern che manda
#    un account in revisione. La configurazione lo impone, e discrawl si rifiuta
#    di partire se il proxy non e' leggibile invece di andare diretto.
#
# 2. Prima di ogni ciclo questo wrapper controlla che il tunnel ci sia davvero.
#    discrawl fallirebbe comunque in connessione, ma fallirebbe dopo aver
#    provato: meglio non provare affatto e dirlo chiaro nel log, perche' un
#    tunnel giu' significa Mac spento o addormentato, non un guasto qui.
#
# Come sul Mac: `sync` non apre mai il Gateway, fa solo GET REST, quindi le
# notifiche push dell'account restano intatte.
set -euo pipefail

repo_dir="/home/hermes/GitHub/tools/discrawl-me"
token_file="/home/hermes/.config/discrawl-me/token"
python_bin="/usr/bin/python3"
scheduler="$repo_dir/run-selected-sync.py"
proxy_host="127.0.0.1"
proxy_port="1080"

if [[ ! -r "$scheduler" ]]; then
  echo "Selective sync scheduler is missing: $scheduler" >&2
  exit 1
fi

# --dry-run non tocca Discord: niente token, niente controllo del tunnel.
for arg in "$@"; do
  if [[ "$arg" == "--dry-run" ]]; then
    exec "$python_bin" "$scheduler" "$@"
  fi
done

if [[ ! -r "$token_file" ]]; then
  echo "Discord token is not configured." >&2
  exit 1
fi

# Il tunnel e' su? Una connessione TCP basta: se la porta accetta, il forward
# remoto e' vivo. Bash lo fa senza installare niente.
if ! timeout 5 bash -c "exec 3<>/dev/tcp/$proxy_host/$proxy_port" 2>/dev/null; then
  echo "Exit tunnel down: no SOCKS5 on $proxy_host:$proxy_port." >&2
  echo "Il Mac lo apre con il LaunchAgent eu.easycop.discrawl-me-exit-tunnel;" >&2
  echo "se il Mac e' spento o addormentato questo e' il comportamento atteso." >&2
  echo "Non procedo: un sync diretto uscirebbe dall'IP del datacenter." >&2
  exit 75   # EX_TEMPFAIL: condizione transitoria, riprovare al prossimo giro
fi

DISCORD_USER_TOKEN="$(<"$token_file")"
export DISCORD_USER_TOKEN

"$python_bin" "$scheduler" --memory-policy /home/hermes/.config/discrawl-me/memory-policy-final.json --with-embeddings --budget-seconds 300 "$@"
rc=$?
# Ricerca per significato: indicizza i messaggi nuovi messi in coda dal sync
# (memoria selettiva 30 giorni, 18/10). Errori qui non fanno fallire il ciclo.
if [[ $rc -eq 0 && " $* " != *" --dry-run "* ]]; then
  DISCRAWL_EMBED_KEY="${DISCRAWL_EMBED_KEY:-proxy}" timeout 300 \
    /home/hermes/GitHub/tools/discrawl-me/discrawl-me --config /home/hermes/.config/discrawl-me/config.toml embed run \
    || echo "embed run fallito (rc=$?), riprovo al prossimo ciclo" >&2
fi
exit $rc
