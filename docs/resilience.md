# Résilience : ce que les pannes ont montré

Ce document rapporte les tests de panne faits sur le déploiement Kubernetes (kind) : ce qui a été mesuré, ce qui a
été corrigé, et ce qui reste faux ou non démontré. Les chiffres viennent de runs réels du 2 et du 5 octobre 2026.

## Question posée

Quand un programme du pipeline est arrêté en plein trafic, **chaque événement accepté par le collector est-il compté
exactement une fois dans MongoDB ?** Les garanties visées sont : jamais de perte (« au moins une fois » : on ne
valide un offset Kafka qu'après traitement) et pas de double comptage (dédoublonnage par `id` dans Redis).

## Méthode

Environnement : cluster kind d'un nœud dans une machine virtuelle de 4 CPU et 6 Go ; générateur de charge sur le même
Mac ; 3 processors et 3 aggregators (sauf mention) ; 2 000 à 2 500 req/s pendant 100 à 150 s, soit environ 364 000
événements comptés par run. À la fin, `loadgen` compare les compteurs MongoDB à ce que le collector a accepté et
sort en erreur au moindre écart. Pendant la charge, `scripts/k8s-chaos.sh` arrête un pod au hasard toutes les 20 à
45 s. Trois façons de le faire :

- **graceful** : suppression du pod, SIGTERM, 30 s de délai de grâce. Le programme finit son lot, valide, quitte le groupe.
- **arrêt rapide** : `kubectl delete pod --grace-period=0 --force`. **Ce n'est pas un crash** : le kubelet impose un
  délai de grâce minimal de 2 s et envoie d'abord SIGTERM, que nos programmes gèrent. J'ai d'abord pris cette option pour
  un arrêt brutal ; les trois premiers runs ci-dessous ne testent donc qu'un arrêt très rapide.
- **crash** : `crictl stop -t 0` dans le nœud kind, soit SIGKILL immédiat. Le tableau final des pods affiche le code de
  sortie `137` (128 + 9), qui prouve qu'il s'agit bien d'un SIGKILL.

## Résultats

| # | Programme | Mode | Délai de session Kafka | Résultat |
|---|---|---|---|---|
| 1 | aggregator (1 réplica) | arrêt propre, 3 pannes | 30 s | exact |
| 2 | processor (3) | arrêt rapide, 3 pannes | 30 s | exact |
| 3 | aggregator (1) | arrêt rapide, 3 pannes | 30 s | exact |
| 4 | processor (3) | crash, 6 pannes | 30 s | exact |
| 5 | aggregator (3) | crash, 6 pannes | 30 s | exact |
| 6 | aggregator (3) | crash, 6 pannes | 10 s | **+49 événements** (+31 pageviews, +12 clics, +6 robots), soit 0,013 % |
| 7 | aggregator (3) | arrêt propre, 6 pannes | 10 s | **+2 pageviews**, soit 0,0006 % |
| 8 | aggregator (3) | crash, 6 pannes | 10 s | exact |
| 9 | processor (3) | crash, 6 pannes | 10 s | exact |

Neuf runs consignés (un run de plus sur les processors n'a pas été conservé, seuls ses codes de sortie le sont).
**Aucun événement perdu dans aucun run.** Deux runs sur neuf montrent un surcomptage, toujours petit.

Effets sur le retard (lu sur le dashboard, un pic par type de programme) :

| | Session 30 s | Session 10 s |
|---|---|---|
| Retard max, processors | ~175 000 messages | ~16 000 |
| Retard max, aggregators | ~160 000 | ~11 000 |
| Redémarrages sans rapport avec un crash | oui (code de sortie 1) | aucun, tous les arrêts observés portent `137` |

## Ce que cela montre

**1. L'ordre « compter, mémoriser, valider » tient.** Aucune perte, même sous SIGKILL répété. Quand le commit d'offsets
échoue après le traitement du lot, le lot est relu, et la mémoire des doublons l'écarte.

**2. Un crash n'est pas gratuit : les survivants souffraient.** Avec les délais par défaut de `kafka-go` (session de
30 s), un membre tué reste inscrit dans le groupe pendant 30 s. Pendant ce temps ses partitions ne sont lues par
personne et les survivants ne peuvent plus valider d'offsets ; leur `CommitMessages` dépasse les 15 s accordées à un
lot, le programme sort avec le code 1 et Kubernetes le redémarre. Résultat : jusqu'à 175 000 messages de retard et des
redémarrages en cascade. Passer la session à 10 s (battement de cœur 2 s, rejoin 15 s ; Redpanda refuse moins de
6 s) a divisé le retard par environ 10 et supprimé la cascade (`internal/batch/batch.go`).

**3. Le décompte n'est pas exactement une fois.** Les runs 6 et 7 montrent du double comptage. Deux mécanismes sont
possibles :

- *La fenêtre Apply → Mark.* Un crash entre l'écriture MongoDB et la mémorisation des identifiants dans Redis laisse
  le lot non validé ; il est relu, non reconnu, et compté une seconde fois. Démontré de façon déterministe par
  `TestKnownLimitCrashBetweenApplyAndMarkCountsTheBatchTwice`. Estimée à 2-4 % du cycle d'un lot par crash.
- *Une course entre instances pendant un rééquilibrage.* Quand une partition passe d'un survivant à un autre, le
  nouveau propriétaire repart du dernier offset validé pendant que l'ancien termine encore son lot. Les deux
  consultent Redis avant que l'un ait mémorisé, et les deux écrivent. Aucun crash n'est nécessaire.

Le run 7 (arrêt propre, +2) ne peut pas être la fenêtre Apply → Mark, puisqu'un arrêt propre finit le lot. C'est
l'indice le plus net en faveur de la seconde cause. **Elle n'a pas été observée directement** : c'est l'explication
qui s'accorde avec les mesures, pas une preuve.

**4. Raccourcir la session a peut-être augmenté le surcomptage.** Avec 30 s de session, les survivants restaient
bloqués en attendant la fin du rééquilibrage et traitaient moins de lots en parallèle ; avec 10 s ils reprennent vite,
ce qui laisse plus de place à la course. Aucun surcomptage sur les 5 runs à 30 s, deux sur les 3 runs d'aggregator à 10 s :
c'est cohérent avec cette hypothèse, mais 9 runs ne permettent pas de conclure, et un crash est de toute façon aléatoire.

## Limites de ces mesures

- Une machine, un nœud, charge et cluster en concurrence pour les mêmes cœurs.
- Peu de runs par configuration, hasard dans le moment de chaque panne : un run exact ne prouve pas l'absence du défaut.
- La vérification porte sur les pageviews, les clics et les robots. **Les compteurs de sessions ne sont pas vérifiés.**
- Les retards sont lus à l'œil sur le dashboard (une mesure toutes les 5 s, fenêtre glissante d'une minute) : bons pour
  un ordre de grandeur, pas pour une durée de panne précise.

## Erreurs de mon propre raisonnement

- J'ai d'abord pris `--grace-period=0 --force` pour un crash (voir Méthode).
- J'avais estimé la fenêtre Apply → Mark à 25 % du cycle ; la latence d'écriture MongoDB (p50 < 1 ms pour un lot de
  15 ms) la situe plutôt à 2-4 %. Un résultat exact sur 6 crashs n'était donc pas une surprise.
- J'attendais un blocage de 30 s après un arrêt brutal et ne l'ai pas vu avec l'arrêt rapide, pour la raison ci-dessus.

## Correction (3 octobre 2026) : écriture idempotente

Les deux causes de surcomptage décrites plus haut sont fermées par le même mécanisme (détail dans
`architecture.md`, « Aggregator ») :

- l'offset traité de chaque partition est enregistré **dans la même transaction MongoDB** que les compteurs, et un
  événement n'est compté que si son offset dépasse cette position : un lot rejoué après un crash n'ajoute rien, et
  de deux instances qui traitent le même lot pendant un rééquilibrage, MongoDB n'en laisse écrire qu'une (conflit
  d'écriture sur le document de position) ;
- la mémoire des doublons ne « marque » plus après coup : elle **réserve** chaque événement pour le message qui le
  porte, avant l'écriture. Un message relu retrouve sa propre réservation (rien n'est perdu), une copie de
  l'événement à un autre offset est écartée.

MongoDB tourne désormais en jeu de répliques d'un nœud (les transactions l'exigent). Le test qui *mesurait* la
fenêtre Apply → Mark (`TestKnownLimitCrashBetweenApplyAndMarkCountsTheBatchTwice`) est remplacé par un test qui
exige un seul comptage, plus un test de deux instances concurrentes et un test de deux transactions concurrentes
sur un vrai MongoDB.

**Premiers runs avec la correction (5 octobre 2026)** : même protocole, 3 processors et 3 aggregators, 2 500 req/s
pendant 150 s, environ 362 000 événements comptés par run.

| # | Programme | Mode | Résultat |
|---|---|---|---|
| 10 | aucun (`make k8s-load-verify`, 2 000 req/s, 30 s) | sans panne | exact |
| 11 | aggregator (3) | crash, 6 pannes | **+20 événements** (+13 pageviews, +7 clics), soit 0,005 % |
| 12 | aggregator (3) | arrêt propre, 6 pannes | **+75 événements** (+61 pageviews, +12 clics, +2 robots), soit 0,02 % |

**Le double comptage n'était pas éliminé.** Les tests unitaires, le test d'intégration (deux transactions
concurrentes sur un vrai MongoDB) et la simulation comptaient juste : quelque chose qu'ils ne modélisaient pas
restait en jeu.

**Audit du run 12.** `make k8s-audit` relit le topic `enriched-events` du cluster depuis le début de la charge et le
compare aux compteurs MongoDB, document (site, minute) par document :

| | pageviews | clics | robots |
|---|---|---|---|
| attendu par le générateur | 274 877 | 69 086 | 18 113 |
| `enriched-events`, chaque événement une fois | 274 877 | 69 086 | 18 113 |
| MongoDB | 274 938 | 69 098 | 18 115 |

Le topic concorde exactement avec le générateur : l'écart naît dans l'aggregator. Il touche 10 documents sur les
trois sites : les trois minutes des pannes (11:51 à 11:53 UTC) et deux minutes d'une heure plus tôt, qui ne reçoivent
que des événements en retard (+1 chacune). 369 406 messages pour 362 076 événements ; aucun événement sur deux
partitions.

**Cause : le même message deux fois dans un lot.** À chaque rééquilibrage, kafka-go reprend chaque partition à
l'offset *validé*. Or la boucle de lecture a de l'avance : un canal de 200 messages se remplit pendant que le lot
précédent s'écrit. Les messages déjà lus mais pas encore validés sont donc livrés une seconde fois. Quand les deux
livraisons tombent dans des lots différents, la position enregistrée couvre la seconde. Quand elles tombent dans le
**même** lot, elles ont le même propriétaire (même partition, même offset) : la réservation les accepte toutes les
deux, la position lue au début de la transaction ne couvre ni l'une ni l'autre, et le message est compté deux fois.
Aucun de mes tests ni la simulation ne livrait deux fois un offset dans le même lot. Les +1 isolés dans des minutes
anciennes s'expliquent de la même façon : un événement en retard livré deux fois dans un lot compte dans sa minute.

**Correction (5 octobre 2026)** : le décodage ne garde qu'un exemplaire de chaque position (partition, offset) dans
un lot, compte les autres dans une nouvelle issue `redelivered` de `pulse_aggregator_events_total`, et l'écrit dans le
journal (`messages delivered twice in the same batch`). Le test `TestRunnerCountsOnceAMessageRedeliveredInTheSameBatch`
reproduit le cas : 8 pageviews pour 5 avant la correction, 5 après.

**Runs avec cette correction** (version `86eba6c`, même protocole) :

| # | Programme | Mode | Résultat |
|---|---|---|---|
| 13 | aggregator (3) | crash, 6 pannes | exact (274 262 pageviews, 68 947 clics, 18 073 robots) |
| 14 | aggregator (3) | arrêt propre, 6 pannes | exact (274 664 pageviews, 69 023 clics, 18 108 robots) |

La prédiction, écrite avant la mesure, était : écart nul, et `redelivered` non nul pendant les pannes (un écart qui
persiste avec `redelivered` à zéro aurait voulu dire que la cause était ailleurs). Les deux se vérifient. Deux relevés
de `sum by (outcome) (increase(pulse_aggregator_events_total[5m]))` pendant ces runs donnent environ 30 et 58 messages
`redelivered` (valeurs extrapolées par `increase`, sur des fenêtres de 5 min qui ne coïncident pas exactement avec les
runs) : le même ordre de grandeur que les surcomptages des runs 11 et 12 (+20 et +75). Avec l'ancien code, chacun de
ces messages aurait été compté deux fois.

Ce que cela démontre, et pas plus : la cause trouvée était réelle, et elle est fermée. Deux runs exacts ne prouvent
pas l'absence d'une cause plus rare : les runs 5 et 8 étaient exacts eux aussi, avec l'ancien code. Reste à refaire
le run processor en crash (le processor a changé depuis le run 9 : mémoire des sessions, règle `too_late`).

Un run intermédiaire, écarté : le cluster faisait encore tourner les images de la veille. L'écart (+1 832) était
exactement le nombre d'événements « trop en retard » que l'ancien processor comptait au lieu de les rejeter. Depuis,
`scripts/k8s-chaos.sh` refuse de tester un cluster qui ne fait pas tourner le code local.

## Pistes, par ordre de priorité

1. ~~**Écriture idempotente.**~~ Faite, puis complétée (message livré deux fois dans un lot) : runs 13 et 14 exacts.
   Reste le run processor en crash.
2. **Rééquilibrage coopératif.** Avec le protocole actuel de `kafka-go`, un rééquilibrage retire toutes les partitions
   à tous les membres ; un client prenant en charge le protocole coopératif (par exemple `franz-go`) ne déplace que le
   nécessaire, ce qui réduit les occasions de course.
3. **Mesure du retard indépendante des consommateurs.** Le retard de l'aggregator est publié par l'aggregator : quand
   le seul pod tombe, la courbe disparaît au moment où elle serait utile. Un exportateur dédié lèverait cet angle mort.
4. **Plus de runs et vérifier les sessions**, pour mettre des intervalles de confiance sur les fréquences ci-dessus.

## Reproduire

```bash
make kind-up                          # une fois
make docker-build kind-load           # après chaque changement de code
make helm-install HELM_ARGS="--set apps.processor.replicas=3 --set apps.aggregator.replicas=3"
# Les images gardent le tag latest : un pod dont la spécification n'a pas changé ne redémarre pas tout seul.
kubectl rollout restart --namespace pulse deployment/collector deployment/processor deployment/aggregator

make k8s-chaos APP=aggregator MODE=crash KILLS=6 INTERVAL=20 RATES=2500 DURATION=150s
make k8s-chaos APP=aggregator MODE=graceful KILLS=6 INTERVAL=20 RATES=2500 DURATION=150s
make k8s-chaos APP=processor MODE=crash KILLS=6 INTERVAL=20 RATES=2500 DURATION=150s

# Un run en écart : où naît-il ? (topic enriched-events face à MongoDB, document par document)
make k8s-audit
```

Pendant les pannes, `sum by (outcome) (increase(pulse_aggregator_events_total[5m]))` dans Prometheus montre les
messages livrés deux fois dans un lot (`redelivered`) et les lots rejoués (`replayed`).
