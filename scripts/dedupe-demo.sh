#!/usr/bin/env bash
# Démonstration du dédoublonnage : le MÊME événement (même id) est envoyé 3 fois au collector.
# Attendu : MongoDB ne compte qu'UN affichage de page pour ce site, pas trois.
#
# Prérequis : make up, make topics, et collector + processor + aggregator lancés.
# Usage : bash scripts/dedupe-demo.sh [adresse du collector]

set -eu

base="${1:-http://localhost:8080}"
site="dedupe-$(date +%s)"
chrome="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
body="{\"id\":\"evt-$site\",\"type\":\"pageview\",\"site_id\":\"$site\",\"visitor_id\":\"v-1\",\"url\":\"https://example.com/\",\"user_agent\":\"$chrome\",\"timestamp\":\"$ts\"}"

echo "Site de démonstration : $site (événement evt-$site envoyé 3 fois)"
for n in 1 2 3; do
  printf '  envoi %s -> ' "$n"
  curl -sS -o /dev/null -w "HTTP %{http_code}\n" -X POST "$base/collect" -d "$body"
done

echo "Attente du traitement (3 s)..."
sleep 3

echo "Compteurs dans MongoDB pour ce site (attendu : \"pageviews\":1) :"
bash "$(dirname "$0")/aggregates.sh" "$site"
