#!/usr/bin/env bash
# Publie directement dans raw-events deux messages inexploitables (sans passer par le collector),
# pour observer le processor les envoyer dans dead-letter.
# Prérequis : make up et make topics

set -eu

printf '%s\n' \
  '{oops ceci n est pas du JSON' \
  '{"id":"incomplet","type":"scroll"}' \
  | docker compose exec -T redpanda rpk topic produce raw-events

echo "2 messages empoisonnés publiés. Voir : make consume TOPIC=dead-letter"
