#!/usr/bin/env bash
# Affiche quelques sessions actives dans Redis : leur état et le temps de vie restant de la clé.
# Prérequis : make up

set -eu

redis() { docker compose exec -T redis redis-cli "$@"; }

keys="$(redis --scan --pattern 'pulse:session:*' | head -n 10)"
if [ -z "$keys" ]; then
  echo "Aucune session dans Redis (le processor tourne-t-il, avec des événements humains ?)"
  exit 0
fi

echo "$keys" | while read -r key; do
  echo "== $key   (expire dans $(redis ttl "$key" | tr -d '\r') s)"
  redis hgetall "$key" | tr -d '\r' | paste -d' ' - - | sed 's/^/   /'
done
