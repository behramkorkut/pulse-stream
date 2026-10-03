#!/usr/bin/env bash
# Démonstration des sessions : envoie au collector 5 événements d'un même visiteur, espacés dans le
# temps (le temps de l'événement, pas celui de l'envoi), puis explique comment relire le résultat.
#
# Attendu : 2 sessions. Les 3 premiers événements (0, 10 et 18 min) forment la première ; le 4e
# arrive 35 min après le 3e (plus de 30 min d'inactivité) : il ouvre la seconde, que le 5e rejoint.
# Tous les instants restent dans l'heure écoulée : au-delà du retard maximal toléré (1 h), le processor
# enverrait ces événements en dead-letter (raison too_late).
#
# Le script relit ensuite le topic enriched-events (les 2 dernières minutes seulement) et affiche les sessions
# attribuées : pas besoin de parcourir tout le topic à la main.
#
# Prérequis : make up, make topics, collector et processor lancés (make run-collector / run-processor).
# Usage : bash scripts/sessions-demo.sh [adresse du collector]

set -eu

base="${1:-http://localhost:8080}"
visitor="demo-$(date +%s)"
start=$(( $(date +%s) - 58 * 60 )) # il y a 58 min, dans la limite du retard toléré
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
send 3 18 panier
send 4 53 accueil
send 5 56 merci

# recent <topic> : les messages des 2 dernières minutes, puis arrêt (@-2m:end), au lieu de tout le topic.
recent() { docker compose exec -T redpanda rpk topic consume "$1" -o @-2m:end -f '%v\n' 2>/dev/null || true; }

echo
echo "Attente du traitement par le processor (3 s)..."
sleep 3

echo "Sessions attribuées (topic enriched-events). Attendu : une session pour 1 à 3, une autre pour 4 et 5,"
echo "new_session=true sur 1 et 4 seulement :"
recent enriched-events | grep "\"visitor_id\":\"$visitor\"" |
  sed -E 's/.*"id":"[^"]*-([0-9]+)".*"timestamp":"([^"]*)".*"session_id":"([^"]*)","new_session":(true|false).*/  événement \1  \2  \3  new_session=\4/' || true

rejected="$(recent dead-letter | grep -c "$visitor" || true)"
echo "Rejetés dans dead-letter : $rejected (attendu : 0)"
echo
echo "État du visiteur dans Redis : make sessions"
