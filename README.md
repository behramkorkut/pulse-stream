# pulse-stream

Chaîne de collecte d'événements d'analytics web **en temps réel**, écrite en Go.
Projet portfolio pour monter en compétence sur les pipelines temps réel
(Kafka, Redis, MongoDB, observabilité, Kubernetes).

> Les événements sont **synthétiques** et les performances mesurées sont celles
> d'un laptop. Le but est de démontrer les mécanismes (contre-pression, sessions,
> idempotence, observabilité), pas de prétendre à une échelle de production.

## Architecture cible

```
générateur de charge ──HTTP──▶ collector ──▶ Kafka/Redpanda [raw-events]
                                                     │
                                  processor (consumer group)
                       valide → enrichit → filtre bots → sessions (Redis)
                                                     │
                                Kafka [enriched-events] + [dead-letter]
                                                     │
                                       aggregator ──▶ MongoDB (stats/minute)
                                                     │
                         /metrics Prometheus ──▶ Grafana
```

Détails : voir [docs/architecture.md](docs/architecture.md).

## Prérequis

- Go (version récente), Docker via Colima, `docker compose`, `make`, `git`
- `uv` (uniquement pour les scripts Python de benchmark)

Vérification de l'environnement : `make doctor`

## Commandes utiles

```
make help     # liste des commandes
make doctor   # vérifie les versions des outils
make test     # tests Go (avec détection de data races)
make build    # compile les binaires dans ./bin
make run-collector  # lance le collector sur :8080
make smoke    # requêtes de test sur le collector
make topics   # crée les topics Kafka
make consume  # lit un topic (TOPIC=enriched-events, TOPIC=dead-letter)
make run-processor  # lance le processor
make run-aggregator # lance l'aggregator (compteurs par minute dans MongoDB)
make poison   # publie des messages invalides
make group    # état du groupe de consommateurs
make demo-sessions  # événements espacés dans le temps, pour voir les sessions
make sessions # sessions actives dans Redis
make demo-dedupe    # le même événement envoyé 3 fois n'est compté qu'une fois
make traffic       # trafic varié pendant N secondes (N=120)
make dashboard      # ouvre le dashboard Grafana (http://localhost:3000)
make metrics       # métriques Prometheus des 3 programmes (F=filtre)
make aggregates     # compteurs par minute dans MongoDB (SITE=site-42 pour filtrer)
make test-integration  # tests avec un vrai broker
make up       # démarre Redpanda, Redis, MongoDB
make down     # arrête l'infrastructure
```

## Journaux et ports

Les programmes écrivent des journaux JSON. Le niveau se règle avec `LOG_LEVEL` (`debug`, `info`, `warn`, `error` ;
`info` par défaut). Les lignes « un lot traité » sont au niveau `debug` : pour les voir pendant une démonstration,
`LOG_LEVEL=debug make run-processor`. Sous charge, les métriques remplacent ces lignes.

| Port | Programme | Chemins |
|---|---|---|
| 8080 | collector | `POST /collect`, `GET /healthz` (la racine `/` renvoie 404, c'est normal) |
| 9101 / 9102 / 9103 | collector / processor / aggregator | `GET /metrics` |
| 9090 | Prometheus | interface web |
| 3000 | Grafana | dashboard `pulse-stream` |

## Avancement

Le journal de développement détaillé est dans `journaldedev.md`.

| Palier | Contenu | État |
|---|---|---|
| 0 | Squelette, outillage, infrastructure locale | terminé |
| 1 | Collector HTTP : réception, validation, arrêt propre | terminé |
| 2 | Producer Kafka, topics, clé de partitionnement | terminé |
| 3 | Processor : consommateur en groupe, enrichissement, dead-letter | terminé |
| 4 | Sessions des visiteurs dans Redis (script Lua atomique) | terminé |
| 5 | Aggregator : dédoublonnage par `id`, compteurs par minute, upserts MongoDB | terminé |
| 6 | Observabilité : métriques Prometheus, Grafana et dashboard versionné dans Git | en cours |
