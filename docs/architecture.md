# Architecture de pulse-stream

## Vue d'ensemble

| Composant | Rôle | Techno |
|---|---|---|
| `cmd/loadgen` | Génère des événements synthétiques (pageviews, clics) à débit réglable | Go |
| `cmd/collector` | Reçoit les événements en HTTP, valide, publie dans `raw-events` | Go + Kafka |
| `cmd/processor` | Consomme `raw-events`, enrichit, filtre les bots, calcule les sessions | Go + Kafka + Redis |
| `cmd/aggregator` | Agrège par minute, écrit les statistiques | Go + Kafka + MongoDB |
| Observabilité | Métriques (événements/s, lag, latence), dashboards | Prometheus + Grafana |

## Topics Kafka

| Topic | Contenu |
|---|---|
| `raw-events` | Événements bruts acceptés par le collector |
| `enriched-events` | Événements validés, enrichis, avec identifiant de session |
| `dead-letter` | Événements rejetés, avec la raison du rejet |

## Principes de conception

- **Au moins une fois** côté Kafka, rendu sûr par l'**idempotence** (un identifiant unique par événement).
- **Contre-pression** : quand un maillon sature, on ralentit l'amont plutôt que de perdre ou de gonfler la mémoire.
- **Arrêt propre** : chaque programme termine les traitements en cours avant de s'arrêter.
- **Observable dès le départ** : chaque composant expose `/metrics`.

## Structure du dépôt

```
cmd/          un dossier par programme (un main.go chacun)
internal/     code partagé, non importable depuis l'extérieur du module
deploy/       manifestes Kubernetes / chart Helm (plus tard)
docs/         documentation
scripts/      scripts utilitaires
```
