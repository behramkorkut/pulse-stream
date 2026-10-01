#!/usr/bin/env bash
# Génère un trafic varié vers le collector pendant N secondes, pour alimenter le dashboard Grafana.
# Mélange : humains (Chrome, iPhone, Firefox), robots, quelques JSON cassés et des événements envoyés deux fois.
# C'est un outil de démonstration (un processus curl par requête : quelques dizaines de requêtes par seconde au mieux) ;
# le vrai générateur de charge, en Go, viendra au palier suivant.
#
# Prérequis : make up, make topics, collector + processor + aggregator lancés.
# Usage : bash scripts/traffic.sh [secondes] [adresse du collector]    (défaut : 60 s)

set -u

duration="${1:-60}"
base="${2:-http://localhost:8080}"
run="$(date +%s)"
end=$(( run + duration ))

agents=(
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
  "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0"
  "Googlebot/2.1 (+http://www.google.com/bot.html)"
)
types=(pageview pageview pageview click)

sent=0; invalid=0; repeated=0; last_body=""
echo "Trafic pendant ${duration} s vers ${base} (Ctrl+C pour arrêter)..."

while [ "$(date +%s)" -lt "$end" ]; do
  roll=$(( RANDOM % 100 ))

  if [ "$roll" -lt 3 ]; then                 # 3 % : JSON cassé
    body='{oops'
    invalid=$(( invalid + 1 ))
  elif [ "$roll" -lt 8 ] && [ -n "$last_body" ]; then   # 5 % : le même événement renvoyé
    body="$last_body"
    repeated=$(( repeated + 1 ))
  else
    sent=$(( sent + 1 ))
    site="site-$(( RANDOM % 3 + 1 ))"
    visitor="v-$(( RANDOM % 50 ))"
    agent="${agents[$(( RANDOM % ${#agents[@]} ))]}"
    type="${types[$(( RANDOM % ${#types[@]} ))]}"
    ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    body="{\"id\":\"traffic-${run}-${sent}\",\"type\":\"${type}\",\"site_id\":\"${site}\",\"visitor_id\":\"${visitor}\",\"url\":\"https://example.com/page-$(( RANDOM % 20 ))\",\"user_agent\":\"${agent}\",\"timestamp\":\"${ts}\"}"
    last_body="$body"
  fi

  curl -sS -o /dev/null --max-time 2 -X POST "${base}/collect" -d "$body" || { echo "collector injoignable"; exit 1; }
  sleep 0.05
done

echo "Terminé : ${sent} événements distincts, ${repeated} renvois du même événement, ${invalid} JSON cassés."
