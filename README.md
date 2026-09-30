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
make poison   # publie des messages invalides
make group    # état du groupe de consommateurs
make demo-sessions  # événements espacés dans le temps, pour voir les sessions
make sessions # sessions actives dans Redis
make test-integration  # tests avec un vrai broker
make up       # démarre Redpanda, Redis, MongoDB
make down     # arrête l'infrastructure
```

## Avancement

Le journal de développement détaillé est dans `journaldedev.md`.

| Palier | Contenu | État |
|---|---|---|
| 0 | Squelette, outillage, infrastructure locale | terminé |
| 1 | Collector HTTP : réception, validation, arrêt propre | terminé |
| 2 | Producer Kafka, topics, clé de partitionnement | terminé |
| 3 | Processor : consommateur en groupe, enrichissement, dead-letter | terminé |
| 4 | Sessions des visiteurs dans Redis (script Lua atomique) | en cours |
