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

## Clé de partitionnement de `raw-events`

La clé est `site_id/visitor_id`, hachée avec Murmur2 (comme le client Java officiel).

- Kafka garantit l'ordre **à l'intérieur d'une partition** : tous les événements d'un visiteur restent donc ordonnés.
- Le processor pourra calculer les sessions sans coordination entre workers : un visiteur = une partition = un consommateur.
- Une clé par `site_id` seul créerait des partitions chaudes (un gros site sature une seule partition).
- Le topic a 6 partitions : jusqu'à 6 consommateurs en parallèle dans un même groupe.

## Garanties du processor

Ordre des opérations pour chaque lot de messages : transformer, écrire toutes les sorties, puis seulement
valider (commit) les offsets d'entrée. Un crash entre l'écriture et le commit provoque une relecture du lot :
des doublons possibles, jamais de perte (**au moins une fois**). Le dédoublonnage par `id` se fera en aval.

- Les lots sont transformés en parallèle par des workers, avec un shard par hachage de la clé : un même
  visiteur est toujours traité séquentiellement, des visiteurs différents en parallèle.
- Un message invalide ne bloque jamais le flux : il est publié dans `dead-letter` avec la raison,
  le message d'origine et sa position (topic, partition, offset).
- Les robots sont conservés dans `enriched-events` avec `is_bot: true` : on ne détruit pas d'information,
  c'est à l'agrégation de décider quoi compter.
- Le processor est tolérant aux champs inconnus (contrairement au collector, strict à la frontière).

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
