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
des doublons possibles, jamais de perte (**au moins une fois**). Le dédoublonnage par `id` est fait en aval, par l'aggregator.

- Les lots sont transformés en parallèle par des workers, avec un shard par hachage de la clé : un même
  visiteur est toujours traité séquentiellement, des visiteurs différents en parallèle.
- Un message invalide ne bloque jamais le flux : il est publié dans `dead-letter` avec la raison,
  le message d'origine et sa position (topic, partition, offset).
- Les robots sont conservés dans `enriched-events` avec `is_bot: true` : on ne détruit pas d'information,
  c'est à l'agrégation de décider quoi compter.
- Le processor est tolérant aux champs inconnus (contrairement au collector, strict à la frontière).

## Sessions

Une session regroupe les événements d'un visiteur (par site) espacés d'au plus 30 minutes. Trois choix structurants :

- **Temps de l'événement**, pas de l'horloge : retraiter un arriéré donne les mêmes sessions qu'en temps réel.
- **Identifiant déterministe** (empreinte du site, du visiteur et du premier événement) et drapeau
  `new_session` calculé en comparant avec le premier événement mémorisé : traiter deux fois le même événement
  donne exactement le même résultat (idempotence, indispensable en "au moins une fois").
- **Atomicité** : la lecture et la mise à jour de l'état du visiteur se font dans un script Lua exécuté d'un
  bloc par Redis, sans course possible entre deux instances du processor.

Un événement en retard est rattaché à la session courante sans faire reculer le dernier instant vu. Les
robots n'ont pas de session. Si Redis est indisponible, le lot est retenté puis le processor s'arrête sans
avoir écrit ni validé : mieux vaut un retard qu'un événement publié sans session.

Le magasin `Memory` sert de référence exécutable de la règle : les mêmes tests de contrat s'appliquent à
`Memory` et à `Redis`.

## Aggregator : dédoublonner, compter, écrire

L'aggregator lit `enriched-events` (groupe `pulse-aggregator`) et maintient, dans MongoDB, un document par
site et par minute (`_id = site|2026-09-30T11:00Z`) avec les compteurs `pageviews`, `clicks`, `bot_events`,
`sessions` et les répartitions `devices.*` / `browsers.*`. Chaque lot suit cet ordre précis :

1. décoder (un message inexploitable est écarté et journalisé, jamais bloquant) et écarter les doublons du lot ;
2. demander à Redis quels identifiants ont déjà été comptés (`MGET`) et les écarter ;
3. cumuler les compteurs dans MongoDB (un seul `BulkWrite` d'upserts avec `$inc`) ;
4. **seulement alors**, mémoriser les identifiants comptés dans Redis (`SET ... EX`, TTL d'une heure) ;
5. puis valider les offsets Kafka.

Pourquoi cet ordre : un crash entre 3 et 4 fait recompter le lot (au pire des doublons), un crash entre 4 et 5
ne recompte rien (les identifiants sont déjà mémorisés). Mémoriser avant d'écrire ferait l'inverse : si
l'écriture échoue, la relecture verrait les identifiants comme « déjà comptés » et les événements seraient
perdus. Entre deux maux, on choisit le doublon rare plutôt que la perte silencieuse.

Le temps utilisé est celui de l'**événement** : un événement en retard tombe dans la bonne minute. Les robots
n'alimentent que `bot_events`. Les clés des répartitions sont filtrées sur une liste blanche (sinon `other`) :
le contenu du topic ne doit jamais devenir un nom de champ MongoDB.

Limites assumées, à documenter honnêtement :

- `$inc` n'est pas idempotent : si `Apply` échoue *après* avoir appliqué une partie du lot puis est retenté,
  ces compteurs peuvent être comptés deux fois. L'éviter demanderait des transactions (Mongo en jeu de
  répliques) ou un état idempotent par construction (par exemple stocker les identifiants d'événements par bucket).
- Fenêtre Apply -> Mark : un crash pile entre les deux fait recompter un lot une fois (démontré par
  `TestKnownLimitCrashBetweenApplyAndMarkCountsTheBatchTwice`). Les tests de panne (`docs/resilience.md`) ont montré
  qu'une seconde cause existe : la vérification des doublons n'est pas atomique entre instances, donc deux instances
  qui traitent les mêmes messages pendant un rééquilibrage peuvent les compter chacune. Écarts mesurés : de 0,0006 %
  à 0,013 %, jamais de perte.
- La mémoire Redis du dédoublonnage croît avec le débit x le TTL. À grande échelle : fenêtre plus courte,
  filtre de Bloom, ou état local par partition.

## Boucle de consommation commune (`internal/batch`)

Le processor et l'aggregator partagent la même boucle : une goroutine lit Kafka, un canal tamponné alimente
l'assemblage des lots (taille ou délai), un `Handler` fournit le métier, et la boucle valide les offsets
uniquement après un succès. La garantie « au moins une fois » et l'arrêt propre sont donc écrits et testés une
seule fois.

## Observabilité

Chaque programme expose ses métriques en texte sur `GET /metrics`, sur un port dédié (collector `:9101`,
processor `:9102`, aggregator `:9103`, réglables avec `METRICS_ADDR`). Prometheus vient les lire toutes les 5 s.
Le trafic de supervision est ainsi séparé du trafic métier. Les métriques utilisent un registre explicite
(pas le registre global) : un test crée son registre et lit exactement ce qu'il a produit.

| Métrique | Type | Sens |
|---|---|---|
| `pulse_collector_requests_total{code}` | compteur | requêtes `/collect` par code HTTP (202, 400, 422, 503...) |
| `pulse_collector_request_duration_seconds` | histogramme | durée d'une requête |
| `pulse_collector_publish_duration_seconds` | histogramme | part passée à publier dans Kafka |
| `pulse_batches_total{consumer,result}` | compteur | lots traités (ok / error) |
| `pulse_batch_messages_total{consumer}` | compteur | messages dont le lot est traité et validé |
| `pulse_batch_size{consumer}` / `pulse_batch_duration_seconds{consumer}` | histogrammes | taille et durée des lots |
| `pulse_consumer_lag{consumer}` | jauge | messages publiés mais pas encore validés par le groupe, **somme de toutes les partitions** |
| `pulse_processor_events_total{outcome}` | compteur | écrits : `enriched` ou `dead_letter` |
| `pulse_processor_dead_letters_total{reason}` | compteur | rejets par raison |
| `pulse_aggregator_events_total{outcome}` | compteur | `counted`, `duplicate`, `skipped` |
| `pulse_aggregator_buckets_written_total` | compteur | documents (site, minute) mis à jour |
| `pulse_aggregator_store_duration_seconds` | histogramme | durée d'un appel MongoDB |

Les séries étiquetées (par code, par issue, par raison) sont créées à zéro au démarrage. Une série qui n'existe
pas affiche « No data » au lieu de 0 ; pire, `rate()` a besoin de deux points pour mesurer une variation : une
série qui apparaît directement à 1 voit sa toute première incrémentation ignorée (gênant pour des événements rares
comme les rejets). Des tests vérifient que ces séries existent dès la création des métriques.

Règles suivies : un compteur n'avance qu'une fois l'action réellement réussie (rien n'est compté si l'écriture
échoue) ; les labels n'ont que quelques valeurs possibles (jamais d'identifiant ni d'URL : chaque valeur
distincte crée une série en mémoire, c'est l'« explosion de cardinalité »). Les débits se déduisent des
compteurs (`rate(...)` dans Prometheus), on n'expose pas de débit déjà calculé.

**Mesure du retard.** Le retard d'un groupe est `fin de partition − offset validé`, additionné sur toutes les
partitions (`internal/kafkautil/lag.go`, via l'API d'administration de Kafka, toutes les 5 s en arrière-plan).
Une première version lisait `Reader.Stats().Lag` de `kafka-go` : en test de charge, elle affichait environ 90 000
alors que `rpk group describe` annonçait plus de 290 000, soit un rapport proche du nombre de partitions (6) : cette
valeur ne reflète qu'une partition à la fois. Leçon : valider une métrique contre une source indépendante avant de
s'y fier.

## Grafana et dashboard

Grafana (`http://localhost:3000`, accès libre : usage local uniquement) interroge Prometheus par le réseau
`pulse-net` (`http://prometheus:9090`). Tout est déclaré par fichiers, rien n'est configuré à la main :

- `deploy/grafana/provisioning/datasources/` : la source de données Prometheus ;
- `deploy/grafana/provisioning/dashboards/` : où trouver les dashboards ;
- `deploy/grafana/dashboards/pulse-stream.json` : le dashboard (24 panneaux : synthèse, collector, consommateurs,
  processor, aggregator/MongoDB, runtime Go).

Le dashboard est en lecture seule dans l'interface (`allowUiUpdates: false`) : on le modifie dans le JSON, donc
dans Git, et il se recharge seul en 10 s. Un dashboard cliqué à la main n'existe que dans le volume de Grafana :
perdu au premier `make down -v`, impossible à relire en revue de code. `make traffic` l'alimente.

## Réseau Docker

Le `docker-compose.yml` déclare un réseau explicite `pulse-net` auquel tous les services sont rattachés. Sur un
même réseau, les conteneurs se trouvent par le nom du service (`redpanda:9092`, `redis:6379`, `mongo:27017`) :
Docker fournit un DNS interne. Sans déclaration, Compose crée un réseau par défaut équivalent ; le déclarer
rend l'architecture lisible et prépare la suite (conteneurs des programmes Go, puis Kubernetes, où la notion
de réseau et de nom de service est centrale).

Par défaut (`make run-collector` & co), nos programmes Go tournent sur le Mac, hors de ce réseau : ils joignent l'infrastructure par les
ports publiés (`localhost:19092`, `6379`, `27017`), et Prometheus les joint dans l'autre sens par
`host.docker.internal`. Avec `make app-up`, les trois programmes tournent en conteneurs *dans* ce réseau et joignent l'infrastructure par
le nom des services ; leurs ports de métriques restent publiés sur le Mac, donc Prometheus les lit comme avant. Sous
Kubernetes, ce sera la découverte automatique des pods qui remplacera ces adresses fixes.

**Images.** Un seul `Dockerfile`, deux étapes : compilation (`golang:alpine`, `CGO_ENABLED=0`) puis image finale
`distroless/static:nonroot`. Le binaire est statique, donc l'image finale n'a besoin ni de bibliothèque C ni de
shell : moins de surface d'attaque, une image de quelques Mo au lieu de quelques centaines. Les dépendances Go sont
téléchargées dans une couche à part, reconstruite seulement quand `go.mod` change.

## Générateur de charge (`cmd/loadgen`)

Le générateur envoie un trafic réaliste au collector, **à un débit imposé**, par paliers (`200,500,1000` req/s par
défaut), puis vérifie que le pipeline a compté exactement ce que le collector a accepté.

- **Débit imposé (open-loop).** Les envois sont planifiés sur une grille régulière (`début + i × intervalle`),
  indépendamment des réponses. Un générateur « boucle fermée » attend la réponse avant d'envoyer la suivante : quand
  le serveur ralentit, il envoie moins, et la lenteur disparaît des mesures (*coordinated omission*).
- **Latence depuis l'instant prévu.** Elle inclut l'attente en file : un serveur qui ne suit plus se voit tout de suite
  dans le p95/p99. Si le client lui-même sature (plus de worker libre, file pleine), la requête est comptée « perdue »
  côté client, et non masquée.
- **Trafic mélangé.** Plusieurs sites et visiteurs, pageviews et clics, ~5 % de robots, ~2 % de doublons (même `id`
  rejoué), ~1 % de messages invalides (400 / 422). Graine fixe (`-seed`) : un même run est reproductible.
- **Vérification de bout en bout.** Chaque événement *accepté* (202, invalide exclu, un seul par `id`) est compté ;
  le générateur interroge ensuite MongoDB (sites préfixés `load-<run>-`) jusqu'à stabilisation. Un compteur qui
  **dépasse** l'attendu est un double comptage (échec immédiat, les compteurs ne font que croître) ; un total qui se
  stabilise **en dessous** est une perte. Le code de sortie est non nul en cas d'échec.
- **Limites.** Le client et le collector tournent sur le même Mac : les chiffres valent pour cette machine, pas pour un
  cluster. L'agrégation `$inc` n'est pas idempotente en cas de crash entre l'écriture et le commit (voir plus haut) :
  la vérification elle-même ne simule pas de panne ; `make k8s-chaos` le fait (voir `docs/resilience.md`).

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
