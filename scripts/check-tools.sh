#!/usr/bin/env bash
# Vérifie que les outils nécessaires à pulse-stream sont installés et affiche leurs versions.
# Compatible avec le bash 3.2 de macOS.

missing=0

check() {
  local name="$1"
  shift
  if command -v "$name" >/dev/null 2>&1; then
    printf "  [ok]      %-16s %s\n" "$name" "$("$@" 2>&1 | head -n 1)"
  else
    printf "  [absent]  %-16s\n" "$name"
    missing=1
  fi
}

echo "Outils :"
check go go version
check git git --version
check make make --version
check docker docker --version
check colima colima version
check uv uv --version

echo
echo "Docker Compose :"
if docker compose version >/dev/null 2>&1; then
  printf "  [ok]      %-16s %s\n" "docker compose" "$(docker compose version | head -n 1)"
else
  printf "  [absent]  %-16s (installer avec : brew install docker-compose)\n" "docker compose"
  missing=1
fi

echo
echo "Démon Docker :"
if docker info >/dev/null 2>&1; then
  echo "  [ok]      accessible (Colima démarré)"
else
  echo "  [arrêté]  lancer : colima start --cpu 4 --memory 6 --disk 30"
  missing=1
fi

echo
if [ "$missing" -eq 0 ]; then
  echo "Tout est prêt."
else
  echo "Il manque des éléments : voir les lignes [absent] / [arrêté] ci-dessus."
  exit 1
fi
