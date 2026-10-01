#!/usr/bin/env bash
# Affiche les métriques "pulse_*" de chaque programme (ceux qui ne tournent pas sont signalés, pas bloquants).
# Usage : bash scripts/metrics.sh [filtre]   ex. bash scripts/metrics.sh batch

set -u

filter="${1:-}"

show() {
  local name="$1" port="$2"
  echo "== $name (http://localhost:$port/metrics)"
  if ! out="$(curl -sS --max-time 2 "http://localhost:$port/metrics" 2>&1)"; then
    echo "   injoignable (le programme tourne-t-il ?)"
    return
  fi
  echo "$out" | grep '^pulse_' | grep -v '_bucket' | grep -e "$filter" | sed 's/^/   /'
}

show collector 9101
show processor 9102
show aggregator 9103
