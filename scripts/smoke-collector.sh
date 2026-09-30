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

# event <suffixe-id> <visiteur> <page>
event() {
  printf '{"id":"%s-%s","type":"pageview","site_id":"site-42","visitor_id":"%s","url":"https://example.com/%s","timestamp":"%s"}' \
    "$run" "$1" "$2" "$3" "$now"
}

call "healthz (attendu : 200)" "$base/healthz"

call "événement valide, visiteur v-1 (attendu : 202)" -X POST "$base/collect" -d "$(event 1 v-1 accueil)"
call "événement valide, visiteur v-1 encore (attendu : 202)" -X POST "$base/collect" -d "$(event 2 v-1 panier)"
call "événement valide, visiteur v-2 (attendu : 202)" -X POST "$base/collect" -d "$(event 3 v-2 accueil)"

call "événement invalide (attendu : 422)" -X POST "$base/collect" -d '{"id":"x","type":"scroll"}'

call "JSON cassé (attendu : 400)" -X POST "$base/collect" -d '{oops'

call "mauvaise méthode (attendu : 405)" "$base/collect"
