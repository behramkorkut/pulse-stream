#!/usr/bin/env bash
# Démonstration des sessions : envoie au collector 5 événements d'un même visiteur, espacés dans le
# temps (le temps de l'événement, pas celui de l'envoi), puis explique comment relire le résultat.
#
# Attendu : 2 sessions. Les 3 premiers événements (0, 10 et 25 min) forment la première ; le 4e
# arrive 45 min après le 3e (plus de 30 min d'inactivité) : il ouvre la seconde, que le 5e rejoint.
#
# Prérequis : make up, make topics, collector et processor lancés (make run-collector / run-processor).
# Usage : bash scripts/sessions-demo.sh [adresse du collector]

set -eu

base="${1:-http://localhost:8080}"
visitor="demo-$(date +%s)"
start=$(( $(date +%s) - 3 * 3600 )) # il y a 3 heures
chrome="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# iso <secondes-epoch> : date UTC ISO 8601, compatible macOS (BSD) et Linux (GNU)
iso() { date -u -r "$1" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ; }

send() {
  local n="$1" offset_min="$2" page="$3"
  local ts
  ts="$(iso $(( start + offset_min * 60 )))"
  printf '  événement %s : +%3s min (%s) -> ' "$n" "$offset_min" "$ts"
  curl -sS -o /dev/null -w "HTTP %{http_code}\n" -X POST "$base/collect" \
    -d "{\"id\":\"$visitor-$n\",\"type\":\"pageview\",\"site_id\":\"site-42\",\"visitor_id\":\"$visitor\",\"url\":\"https://example.com/$page\",\"user_agent\":\"$chrome\",\"timestamp\":\"$ts\"}"
}

echo "Visiteur : $visitor"
send 1 0 accueil
send 2 10 produits
send 3 25 panier
send 4 70 accueil
send 5 75 merci

echo
echo "Pour voir les sessions attribuées, lance (Ctrl+C pour quitter) :"
echo "  make consume TOPIC=enriched-events | grep $visitor"
echo "Cherche session_id (identique pour les événements 1 à 3, différent pour 4 et 5) et new_session."
echo "Et l'état dans Redis : make sessions"
