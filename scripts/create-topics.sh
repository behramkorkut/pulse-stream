#!/usr/bin/env bash
# Crée les topics Kafka de pulse-stream dans Redpanda (idempotent : peut être relancé sans risque).
# Prérequis : make up

set -eu

rpk() {
  docker compose exec -T redpanda rpk "$@"
}

existing="$(rpk topic list 2>/dev/null || true)"

create() {
  local name="$1" partitions="$2"
  if echo "$existing" | awk 'NR > 1 { print $1 }' | grep -qx "$name"; then
    echo "  [existe]  $name"
  else
    rpk topic create "$name" --partitions "$partitions" --replicas 1 >/dev/null
    echo "  [créé]    $name ($partitions partitions)"
  fi
}

echo "Topics :"
# 6 partitions = jusqu'à 6 consommateurs en parallèle dans un même groupe (palier processor).
create raw-events 6
create enriched-events 6
# Les rejets sont rares : peu de partitions suffisent.
create dead-letter 3

echo
rpk topic describe raw-events --print-partitions 2>/dev/null | head -n 20 || true
