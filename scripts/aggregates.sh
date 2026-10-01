#!/usr/bin/env bash
# Affiche les compteurs par minute stockés dans MongoDB (les 10 dernières minutes, tous sites confondus,
# ou celles d'un site : bash scripts/aggregates.sh site-42).
# Prérequis : make up, et l'aggregator qui a déjà traité des événements.

set -eu

site="${1:-}"
if [ -n "$site" ]; then
  filter="{site_id: '$site'}"
else
  filter="{}"
fi

docker compose exec -T mongo mongosh --quiet pulse --eval "
  const docs = db.minute_stats.find($filter).sort({minute: -1}).limit(10).toArray();
  if (docs.length === 0) { print('Aucun compteur dans MongoDB (l aggregator tourne-t-il ?)'); }
  // Les compteurs sont des entiers 64 bits (Long) : JSON.stringify les afficherait {high, low, unsigned}.
  // Le remplaçant les convertit en nombres ordinaires pour l'affichage.
  const plain = (k, v) => (v !== null && typeof v === 'object' && typeof v.toNumber === 'function') ? v.toNumber() : v;
  docs.forEach(d => print(JSON.stringify(d, plain)));
"
