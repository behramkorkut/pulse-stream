#!/usr/bin/env bash
# Test de fumée du collector : envoie quelques requêtes et affiche réponse + code HTTP.
# Prérequis : le collector tourne (make run-collector dans un autre terminal).
# Usage : bash scripts/smoke-collector.sh [adresse]   (défaut : http://localhost:8080)

base="${1:-http://localhost:8080}"
now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
id="smoke-$(date +%s)"

call() {
  local label="$1"
  shift
  echo "--- $label"
  curl -sS -w "HTTP %{http_code}\n" "$@"
  echo
}

call "healthz (attendu : 200)" "$base/healthz"

call "événement valide (attendu : 202)" -X POST "$base/collect" \
  -d "{\"id\":\"$id\",\"type\":\"pageview\",\"site_id\":\"site-42\",\"visitor_id\":\"v-1\",\"url\":\"https://example.com/\",\"timestamp\":\"$now\"}"

call "événement invalide (attendu : 422)" -X POST "$base/collect" \
  -d '{"id":"x","type":"scroll"}'

call "JSON cassé (attendu : 400)" -X POST "$base/collect" -d '{oops'

call "mauvaise méthode (attendu : 405)" "$base/collect"
