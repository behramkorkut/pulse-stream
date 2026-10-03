#!/usr/bin/env bash
# Test de panne : pendant une charge VÉRIFIÉE, tue des pods d'un programme et regarde si les compteurs
# MongoDB restent exacts (ni perte, ni double comptage).
#
# Usage : bash scripts/k8s-chaos.sh <programme> <mode> <nombre> <secondes-entre-deux> [options de loadgen...]
#   programme : processor | aggregator | collector
#   mode      : graceful = suppression du pod, arrêt demandé poliment (SIGTERM, 30 s de délai de grâce) : le
#                          programme finit son lot, valide ses offsets et quitte le groupe de consommateurs.
#               crash    = le processus du conteneur reçoit SIGKILL à l'instant (comme un plantage ou une coupure
#                          de courant) : aucune chance de finir un lot ni de valider quoi que ce soit.
#                          Kubernetes redémarre ensuite le conteneur dans le même pod.
#
# Pourquoi pas `kubectl delete pod --grace-period=0 --force` pour simuler un crash ? Parce que ce n'en est pas un :
# le kubelet impose un délai de grâce minimal de 2 secondes et envoie d'abord SIGTERM. Un programme qui gère bien
# SIGTERM (le nôtre) s'arrête proprement dans ce délai : on teste alors un arrêt rapide, pas un crash.
# Prérequis : cluster kind avec le chart installé, bin/loadgen compilé, docker (pour atteindre le nœud kind).

set -u

APP="${1:?programme à tuer (processor, aggregator...)}"
MODE="${2:?mode : graceful ou force}"
KILLS="${3:?nombre de pods à tuer}"
INTERVAL="${4:?secondes entre deux pannes}"
shift 4

NS="${NAMESPACE:-pulse}"
FIRST_DELAY="${FIRST_DELAY:-15}"   # laisser la charge monter avant la première panne

NODE="${KIND_NODE:-${KIND_CLUSTER:-pulse}-control-plane}"   # le conteneur Docker qui EST le nœud kind

case "$MODE" in graceful | crash) ;; *) echo "mode inconnu : $MODE (graceful ou crash)" >&2; exit 2 ;; esac

# Garde-fou : le cluster doit faire tourner le code local. Un run de panne sur des images périmées mesure l'ancienne
# version. C'est arrivé : un processor sans la règle too_late comptait les événements trop en retard, et l'écart
# (+1 832) ressemblait à du double comptage. Chaque programme écrit sa version dans son journal au démarrage.
expected="$(git describe --tags --always --dirty 2>/dev/null || echo dev)"
stale=""
for app in collector processor aggregator; do
  running="$(kubectl logs --namespace "$NS" "deployment/$app" 2>/dev/null | grep -o '"version":"[^"]*"' | head -n 1 | cut -d'"' -f4)"
  [ "$running" = "$expected" ] || stale="$stale $app=${running:-inconnue}"
done
if [ -n "$stale" ] && [ "${ALLOW_STALE:-0}" != 1 ]; then
  echo "Le cluster ne fait pas tourner le code local ($expected) :$stale" >&2
  echo "Mettre à jour : make docker-build kind-load, make helm-install HELM_ARGS=\"...\", puis" >&2
  echo "  kubectl rollout restart --namespace $NS deployment/collector deployment/processor deployment/aggregator" >&2
  echo "Pour lancer quand même : ALLOW_STALE=1 make k8s-chaos ..." >&2
  exit 3
fi

# Un tunnel vers le MongoDB du cluster, le temps du test.
kubectl port-forward --namespace "$NS" mongo-0 27018:27017 >/dev/null 2>&1 &
PF=$!

killer() {
  sleep "$FIRST_DELAY"
  local i=1
  while [ "$i" -le "$KILLS" ]; do
    if [ "$MODE" = crash ]; then
      # Les conteneurs tournent dans le nœud kind, géré par containerd : on lui parle avec crictl.
      # `stop -t 0` = délai nul = SIGKILL immédiat. Le conteneur (pas le pod) est tué puis redémarré par le kubelet.
      local ids n id
      ids="$(docker exec "$NODE" crictl ps --name "^${APP}\$" -q 2>/dev/null)"
      n="$(printf '%s\n' "$ids" | grep -c .)"
      if [ "$n" -gt 0 ]; then
        id="$(printf '%s\n' "$ids" | sed -n "$((RANDOM % n + 1))p")"
        echo "[$(date +%T)] panne $i/$KILLS (crash, SIGKILL) : conteneur $id"
        docker exec "$NODE" crictl stop -t 0 "$id" >/dev/null 2>&1
      else
        echo "[$(date +%T)] panne $i/$KILLS : aucun conteneur $APP en cours d'exécution (en attente de redémarrage), ignorée"
      fi
    else
      local pods pick
      pods="$(kubectl get pods --namespace "$NS" --selector "app.kubernetes.io/name=$APP" --field-selector status.phase=Running -o name)"
      n="$(printf '%s\n' "$pods" | grep -c .)"
      if [ "$n" -gt 0 ]; then
        pick="$(printf '%s\n' "$pods" | sed -n "$((RANDOM % n + 1))p")"
        echo "[$(date +%T)] panne $i/$KILLS (graceful, SIGTERM) : $pick"
        kubectl delete --namespace "$NS" "$pick" --wait=false >/dev/null 2>&1
      else
        echo "[$(date +%T)] panne $i/$KILLS : aucun pod $APP en cours d'exécution, ignorée"
      fi
    fi
    i=$((i + 1))
    sleep "$INTERVAL"
  done
  echo "[$(date +%T)] fin des pannes"
}

sleep 3
killer &
KP=$!

./bin/loadgen -url http://localhost:18080/collect -mongo-uri 'mongodb://localhost:27018/?directConnection=true' "$@"
status=$?

kill "$KP" "$PF" 2>/dev/null
wait "$KP" "$PF" 2>/dev/null

echo
echo "Pods $APP après le test :"
# Code de sortie 137 = 128 + 9 : le processus a été tué par SIGKILL (preuve d'un vrai crash).
kubectl get pods --namespace "$NS" --selector "app.kubernetes.io/name=$APP" \
  -o custom-columns='POD:.metadata.name,AGE:.metadata.creationTimestamp,REDEMARRAGES:.status.containerStatuses[0].restartCount,DERNIERE_SORTIE:.status.containerStatuses[0].lastState.terminated.exitCode' 
exit "$status"
