# Benchmarks

Ce document explique comment `pulse-stream` a été mesuré, ce que les mesures montrent, et ce qu'elles ne montrent pas.
Tous les chiffres viennent de runs réels du 1er octobre 2026, sur une seule machine ; ils sont reproductibles avec
les commandes ci-dessous.

## Environnement

| Élément | Valeur |
|---|---|
| Machine | MacBook Pro Apple Silicon, 8 cœurs, 16 Go de RAM |
| Docker | Colima : machine virtuelle de **4 CPU et 6 Go** (Redpanda, Redis, MongoDB, Prometheus, Grafana) |
| Programmes Go | collector, processor(s), aggregator et **générateur de charge, tous sur le même Mac** |
| Topics | `raw-events` et `enriched-events` : 6 partitions chacun, 1 réplique |
| Mélange de trafic | 20 % de clics, 5 % de robots, 2 % de renvois (même `id`), 1 % d'invalides (400/422) |

Conséquence importante : le générateur, le collector et les consommateurs **se partagent les mêmes 8 cœurs**. Les
chiffres absolus sont donc plus bas que ce que chaque maillon ferait seul. Les **comparaisons entre configurations**
sont fiables ; les valeurs absolues ne se transposent pas à un cluster.

## Protocole

Le générateur (`cmd/loadgen`) travaille à **débit imposé** : il planifie les envois sur une grille de temps régulière,
sans attendre les réponses, et mesure la latence depuis l'instant où la requête aurait dû partir. Un générateur qui
attend chaque réponse ralentirait avec le serveur et masquerait justement la saturation (*coordinated omission*).

À la fin d'un run, il vérifie le résultat **de bout en bout** : chaque événement accepté par le collector (202,
une fois par `id`) doit se retrouver exactement une fois dans les compteurs MongoDB. Un compteur trop haut est un
double comptage, un total qui se fige trop bas est une perte.

```bash
make up && make topics
make run-processor      # terminaux séparés
make run-aggregator
make run-collector

make load RATES=200,500,1000 DURATION=20s                     # montée en charge douce
make load RATES=12000 DURATION=40s WORKERS=256                # charge forte
```

Pour surveiller le retard indépendamment du dashboard : `make group` (somme sur `TOTAL-LAG`).

Plusieurs processors : lancer des instances supplémentaires avec un port de métriques distinct, par exemple
`METRICS_ADDR=:9112 ./bin/processor`. Elles partagent le groupe `pulse-processor` et Kafka répartit les 6 partitions
entre elles (`make group` montre l'attribution).

## Résultat 1 : le collector tient très haut

Un processor, un aggregator. Débit visé croissant, 20 à 30 s par palier.

| Visé (req/s) | Acceptés (/s) | Perdus côté client | Erreurs | p50 | p99 |
|---:|---:|---:|---:|---:|---:|
| 200 | 198 | 0 | 0 | 12,4 ms | 15,2 ms |
| 1 000 | 989 | 0 | 0 | 9,3 ms | 14,3 ms |
| 4 000 | 3 960 | 0 | 0 | 7,1 ms | 12,5 ms |
| 6 000 | 5 938 | 0 | 0 | 6,5 ms | 12,2 ms |
| 8 000 | 7 918 | 133 | 0 | 6,8 ms | 14,6 ms |
| 12 000 | 11 874 | 162 | 0 | 7,1 ms | 15,8 ms |
| 16 000 | 15 812 | 820 | 0 | 7,5 ms | 20,0 ms |

Les « acceptés » valent environ 99 % du visé : le 1 % manquant est le trafic volontairement invalide (400/422), pas
une perte. La colonne « perdus » compte les envois que **le générateur** n'a pas pu tenir (plus de worker libre) :
elle mesure la saturation du Mac, pas celle du collector. Le collector ne renvoie aucun 503 jusqu'à 16 000 req/s.

La latence médiane **baisse** quand la charge monte (12,4 ms à 200 req/s, 7 ms à 4 000) : à faible débit, chaque
requête attend le délai de regroupement du writer Kafka (10 ms) ; à fort débit, les lots se remplissent plus vite.

## Résultat 2 : le goulot est derrière le collector

À 12 000 req/s, le collector accepte ~11 900 événements/s, mais un seul processor en traite environ 6 300/s une fois
la charge arrêtée (≈ 4 500/s pendant la charge, où il se dispute le processeur avec le générateur). Le surplus
s'accumule **dans Kafka** : jusqu'à 291 000 messages en retard après 40 s. Il n'y a ni perte ni refus : le collector
n'est pas ralenti, et le retard se résorbe en 52 s une fois la charge terminée. C'est exactement le rôle du broker :
absorber les pointes que les consommateurs ne suivent pas en temps réel.

## Résultat 3 : ajouter des processors

Même charge à chaque fois : 12 000 req/s pendant 40 s, soit ~474 700 messages acceptés. Une mesure par
configuration (deux pour un processor : 51,1 s et 52,1 s, soit ±1 %).

| Processors | Aggregators | Retard max (≈) | Rattrapage après la charge | Débit moyen de bout en bout |
|---:|---:|---:|---:|---:|
| 1 | 1 | 291 000 | 52 s | 5 200 /s |
| 2 | 1 | 190 000 | 18 s | 8 200 /s |
| 3 | 1 | 82 000 | 10 s | 9 500 /s |

Le « débit moyen de bout en bout » est le nombre de messages divisé par le temps total (40 s de charge + rattrapage) :
c'est la comparaison la plus honnête, parce que le temps de rattrapage seul est trompeur. Il chute plus vite que le
nombre de processors n'augmente, car avec plus de consommateurs le retard accumulé *pendant* la charge est déjà plus
petit.

Lecture :

- Passer de 1 à 2 processors multiplie le débit par **1,6**. Le processor traite chaque lot de façon séquentielle
  (transformation, appel Redis par événement, écriture Kafka synchrone, validation des offsets) : une instance passe
  beaucoup de temps à attendre des réponses réseau, et une deuxième occupe ce temps mort.
- Passer de 2 à 3 n'apporte plus que **+16 %**. Le maillon suivant prend le relais : l'aggregator, une seule
  instance, plafonne autour de 8 500 à 9 500 événements/s (pic de 8 600 sur le dashboard).
- Deux événements d'un même visiteur restent dans la même partition (clé de partitionnement) : l'ordre par visiteur,
  nécessaire aux sessions, est préservé quel que soit le nombre d'instances.
- Dans les trois runs, la vérification de bout en bout est exacte : aucun événement perdu, aucun double comptage, y
  compris pendant les réattributions de partitions quand on a ajouté un processor.

## Limites de ces mesures

- **Une seule machine**, partagée avec le générateur ; **une mesure par configuration** (pas de médiane sur plusieurs
  runs) ; les écarts de 1 à 2 % ne sont pas significatifs.
- **Prometheus ne scrape qu'une instance de processor** (port 9102). Sur le dashboard, le débit « processor » représente
  donc 1/N du total, alors que la courbe de l'aggregator (une seule instance) montre le total. Le retard, lui, est
  celui du groupe entier. Avec plusieurs instances, il faudra les déclarer toutes (découverte automatique des pods
  sous Kubernetes).
- **Le retard a d'abord été mal mesuré** : `Reader.Stats().Lag` de `kafka-go` ne voit qu'une partition à la fois et
  sous-estimait d'un facteur ~6. Corrigé par la somme sur les partitions, et recoupé avec `rpk group describe`.
- Le test ne simule **aucune panne**. L'agrégation par `$inc` n'est pas idempotente si un crash survient entre
  l'écriture MongoDB et la validation des offsets (limite documentée dans `architecture.md`).

## Pistes pour aller plus haut

Dans l'ordre où je les tenterais :

1. **Plusieurs aggregators.** Le topic `enriched-events` a 6 partitions : jusqu'à 6 instances en parallèle. C'est la
   mesure suivante : 3 processors + 2 aggregators doit montrer si le pipeline suit 12 000 événements/s.
2. **Un aller-retour Redis par lot, pas par événement** (pipeline) dans le processor, et chevaucher l'écriture Kafka
   du lot courant avec le traitement du suivant.
3. **Taille de lot** (`BATCH_SIZE`) : les lots restent à 200 sous charge ; l'augmenter amortit les coûts fixes, au prix
   d'une latence plus grande à faible débit.
4. **Isoler le générateur sur une autre machine**, pour séparer ce que vaut le pipeline de ce que vaut le Mac.
