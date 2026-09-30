#!/usr/bin/env bash
# Test de fumée du collector : envoie quelques requêtes et affiche réponse + code HTTP.
# Prérequis : le collector tourne (make run-collector dans un autre terminal).
# Usage : bash scripts/smoke-collector.sh [adresse]   (défaut : http://localhost:8080)

base="${1:-http://localhost:8080}"
now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
run="smoke-$(date +%s)"

call() {
  local label="$1"
  shift
  echo "--- $label"
  curl -sS -w "HTTP %{http_code}\n" "$@"
  echo
}

chrome="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
iphone="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1"

# event <suffixe-id> <visiteur> <page> [user-agent]
# Sans user-agent dans le corps, le collector retient celui de la requête HTTP (ici : curl, donc un robot).
event() {
  local ua=""
  if [ -n "${4:-}" ]; then ua=",\"user_agent\":\"$4\""; fi
  printf '{"id":"%s-%s","type":"pageview","site_id":"site-42","visitor_id":"%s","url":"https://example.com/%s","timestamp":"%s"%s}' \
    "$run" "$1" "$2" "$3" "$now" "$ua"
}

call "healthz (attendu : 200)" "$base/healthz"

call "humain Chrome, visiteur v-1 (attendu : 202)" -X POST "$base/collect" -d "$(event 1 v-1 accueil "$chrome")"
call "humain Chrome, visiteur v-1 encore (attendu : 202)" -X POST "$base/collect" -d "$(event 2 v-1 panier "$chrome")"
call "humain iPhone, visiteur v-2 (attendu : 202)" -X POST "$base/collect" -d "$(event 3 v-2 accueil "$iphone")"
call "sans user-agent dans le corps : curl, un robot (attendu : 202)" -X POST "$base/collect" -d "$(event 4 v-3 accueil)"

call "événement invalide (attendu : 422)" -X POST "$base/collect" -d '{"id":"x","type":"scroll"}'

call "JSON cassé (attendu : 400)" -X POST "$base/collect" -d '{oops'

call "mauvaise méthode (attendu : 405)" "$base/collect"
