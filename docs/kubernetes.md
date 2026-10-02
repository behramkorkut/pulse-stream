# Déploiement sur Kubernetes (kind + Helm)

Ce document décrit le chart Helm `deploy/helm/pulse-stream` et les choix qui y sont faits. Il se complète au fil des
paliers (observabilité et métriques automatiques, intégration continue du chart).

## Vue d'ensemble

```
Mac ──:18080──► NodePort 30080 ──► Service collector ──► pod collector ──► Redpanda ──► processor ──► Redpanda
                                                                                                         │
                                                                              Redis ◄── processor        ▼
                                                                              Redis ◄── aggregator ◄─────┘
                                                                              MongoDB ◄─ aggregator
```

Tout vit dans le namespace `pulse` d'un cluster `kind` d'un seul nœud : trois Deployments (collector, processor,
aggregator), une infrastructure de développement (Redpanda et MongoDB en StatefulSet, Redis en Deployment) et un Job
qui crée les topics.

## Choix notables

- **Un chart, une boucle.** Les trois programmes ont la même forme ; `templates/apps.yaml` génère leurs Deployments
  par une boucle sur `apps` dans `values.yaml`. Ajouter un programme, c'est ajouter un bloc de valeurs.
- **Sondes.** Seul le collector expose `/healthz`. Pour le processor et l'aggregator, la sonde interroge `/metrics` :
  elle prouve que le processus tourne et répond, pas que Kafka ou MongoDB sont joignables. Un vrai `/readyz` qui
  vérifie les dépendances est une amélioration identifiée.
- **Ressources.** Les *requests* (réservation) sont modestes ; les *limits* ne portent que sur la mémoire. Une limite
  de processeur bride le programme par tranches de temps même quand la machine est libre, ce qui dégrade la latence
  sans protéger grand-chose sur un cluster à un nœud.
- **Sécurité.** Utilisateur 65532, système de fichiers racine en lecture seule (un `emptyDir` sur `/tmp`),
  `allowPrivilegeEscalation: false`, toutes les capabilities Linux retirées, profil seccomp par défaut. L'infrastructure
  (Redpanda, MongoDB, Redis) n'est pas durcie de la même façon : ce sont des images tierces de développement.
- **Ordre de démarrage.** Les programmes qui utilisent Kafka ont un `initContainer` qui attend que les topics existent
  (créés par le Job `create-topics`). Aucun ordre n'est garanti entre pods : c'est chaque pod qui attend ce dont il a besoin.
- **Découverte automatique des cibles.** Chaque pod de programme porte les annotations `prometheus.io/scrape`, `/port` et
  `/path`. Prometheus interroge l'API de Kubernetes (`kubernetes_sd_configs`, rôle `pod`, limité au namespace) et garde
  les pods annotés : lancer 3 processors ajoute 3 cibles sans toucher à la configuration. Cela lève la limite du palier 8
  (un seul processor lu). Pour que Prometheus puisse lister les pods, il a son propre `ServiceAccount` et un `Role`
  limité à `get/list/watch` sur les pods du namespace.
- **Étiquettes.** `job` = nom du programme, `instance` = nom du pod. Chaque processor publie le retard du groupe entier
  (pas le sien) : le dashboard l'agrège avec `max by (consumer)`, sinon les N courbes identiques se superposeraient.
- **Le dashboard n'est pas copié dans le chart** : `make helm-install` l'injecte depuis `deploy/grafana/dashboards`
  (`--set-file`), ce qui garde une seule source pour le compose et pour Kubernetes.
- **Redémarrage automatique sur changement de configuration** : les Deployments de Prometheus et Grafana portent une
  annotation `checksum/config` (empreinte du ConfigMap) ; modifier la configuration change le pod, donc le redémarre.

## Vérifié à chaque push

Deux jobs de la CI (`.github/workflows/ci.yml`) couvrent le chart :

- **`helm`** : `helm lint --strict`, puis le rendu de trois combinaisons de valeurs (par défaut ; sans infrastructure,
  supervision ni topics ; 3 processors sans Service). Le chart a des branches conditionnelles : que les valeurs par
  défaut produisent du YAML valide ne prouve rien sur les autres.
- **`kubernetes`** : crée un cluster kind avec le même fichier de configuration qu'en local, construit et charge les
  images, installe le chart avec 2 processors (`--wait`), puis vérifie que le collector répond, que Prometheus a
  découvert exactement 4 pods (collector, 2 processors, aggregator), et que 500 req/s pendant 15 s se retrouvent
  **exactement** dans MongoDB (`make k8s-load-verify`). En cas d'échec, l'état des pods et leurs journaux sont affichés.

## Limites connues

- Noms de Services fixes (`redpanda`, `redis`, `mongo`) : une seule installation par namespace.
- Pas de disque persistant : les données disparaissent avec les pods.
- Le Job `create-topics` est un objet ordinaire ; Kubernetes refuse de modifier le modèle d'un Job existant. Si on change
  les topics après coup : `make helm-uninstall` puis `make helm-install`.
- Un seul nœud : on ne teste ni la répartition des pods entre machines ni les pannes de nœud.
