.DEFAULT_GOAL := help

MODULE  := github.com/behramkorkut/pulse-stream
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
TOPIC   ?= raw-events
RATES    ?= 200,500,1000
DURATION ?= 20s
WORKERS  ?= 128
IMAGES   := collector processor aggregator loadgen
KIND_CLUSTER ?= pulse
KUBE_IMAGES  := collector processor aggregator
NAMESPACE    ?= pulse
APP          ?= aggregator
MODE         ?= crash
KILLS        ?= 3
INTERVAL     ?= 40
RELEASE      ?= pulse
CHART        := deploy/helm/pulse-stream
# Le dashboard Grafana reste dans deploy/grafana (source unique, partagee avec docker compose) : il est injecte dans le chart.
HELM_FILES   := --set-file monitoring.dashboards.pulse-stream=deploy/grafana/dashboards/pulse-stream.json
LDFLAGS := -X $(MODULE)/internal/version.Version=$(VERSION)

.PHONY: help doctor fmt fmt-check vet test cover ci test-integration build clean run-collector run-processor run-aggregator smoke poison sessions demo-sessions aggregates demo-dedupe metrics traffic load dashboard topics consume group group-aggregator up down ps logs docker-build docker-images app-up app-down app-logs kind-up kind-down kind-load kind-status helm-lint helm-template helm-install helm-uninstall k8s-pods k8s-smoke k8s-mongo k8s-dashboard k8s-load k8s-load-verify k8s-chaos k8s-redeploy

help: ## Affiche cette aide
	grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

doctor: ## Verifie les outils installes et leurs versions
	bash scripts/check-tools.sh

fmt: ## Formate le code Go
	go fmt ./...

fmt-check: ## Verifie le formatage sans rien modifier (utilise par la CI)
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "Fichiers mal formates :"; echo "$$out"; echo "Corrige avec : make fmt"; exit 1; fi

vet: ## Analyse statique du code Go (avec et sans les tests d'integration)
	go vet ./...
	go vet -tags=integration ./...

test: ## Lance les tests avec le detecteur de data races
	go test -race -count=1 ./...

cover: ## Tests avec detecteur de data races et mesure de couverture (coverage.out)
	go test -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out | tail -1

ci: fmt-check vet cover build ## Les memes verifications que la CI (hors tests d'integration)

test-integration: ## Tests d'integration (necessite make up)
	go test -race -count=1 -tags=integration ./internal/...

build: ## Compile les binaires dans ./bin
	@mkdir -p bin
	go build -ldflags "$(LDFLAGS)" -o bin/ ./cmd/...

clean: ## Supprime les binaires
	rm -rf bin

run-collector: build ## Lance le collector en local (Ctrl+C pour l'arreter)
	./bin/collector

run-processor: build ## Lance le processor en local (Ctrl+C pour l'arreter)
	./bin/processor

run-aggregator: build ## Lance l'aggregator en local (Ctrl+C pour l'arreter)
	./bin/aggregator

smoke: ## Test de fumee du collector (il doit deja tourner)
	bash scripts/smoke-collector.sh

poison: ## Publie 2 messages inexploitables dans raw-events (pour le dead-letter)
	bash scripts/poison.sh

sessions: ## Affiche quelques sessions actives dans Redis
	bash scripts/sessions.sh

demo-sessions: ## Envoie des evenements espaces dans le temps pour voir les sessions
	bash scripts/sessions-demo.sh

aggregates: ## Compteurs par minute dans MongoDB : make aggregates SITE=site-42
	bash scripts/aggregates.sh $(SITE)

demo-dedupe: ## Envoie 3 fois le meme evenement : il ne doit etre compte qu'une fois
	bash scripts/dedupe-demo.sh

metrics: ## Affiche les metriques pulse_* des 3 programmes : make metrics F=batch
	bash scripts/metrics.sh $(F)

traffic: ## Genere du trafic varie pendant N secondes : make traffic N=120
	bash scripts/traffic.sh $(or $(N),60)

load: build ## Test de charge a debit impose : make load RATES=200,500,1000 DURATION=20s WORKERS=128
	./bin/loadgen -rates $(RATES) -duration $(DURATION) -workers $(WORKERS)

dashboard: ## Ouvre le dashboard Grafana dans le navigateur (macOS)
	open http://localhost:3000/d/pulse-stream

topics: ## Cree les topics Kafka (infrastructure demarree)
	bash scripts/create-topics.sh

consume: ## Lit un topic : make consume TOPIC=enriched-events (Ctrl+C pour quitter)
	docker compose exec redpanda rpk topic consume $(TOPIC) -o start -f 'partition=%p offset=%o key=%k value=%v\n'

group: ## Membres, partitions assignees et retard du groupe pulse-processor
	docker compose exec redpanda rpk group describe pulse-processor

group-aggregator: ## Membres, partitions assignees et retard du groupe pulse-aggregator
	docker compose exec redpanda rpk group describe pulse-aggregator

up: ## Demarre l'infrastructure locale (Redpanda, Redis, MongoDB)
	docker compose up -d --wait

down: ## Arrete l'infrastructure locale
	docker compose down

ps: ## Etat des conteneurs
	docker compose ps

logs: ## Suit les journaux des conteneurs
	docker compose logs -f --tail=50

docker-build: ## Construit les 4 images Docker (pulse-stream/<programme>:<version> et :latest)
	@for c in $(IMAGES); do \
	  echo "==> $$c"; \
	  docker build --build-arg CMD=$$c --build-arg VERSION=$(VERSION) -t pulse-stream/$$c:$(VERSION) -t pulse-stream/$$c:latest . || exit 1; \
	done

docker-images: ## Liste les images pulse-stream et leur taille
	docker images "pulse-stream/*"

app-up: up topics ## Lance collector, processor et aggregator EN CONTENEURS (construit les images)
	PULSE_VERSION=$(VERSION) docker compose --profile app up -d --build

app-down: ## Arrete les 3 programmes conteneurises (l'infrastructure reste)
	docker compose --profile app rm -sf collector processor aggregator

app-logs: ## Suit les journaux des 3 programmes conteneurises (Ctrl+C pour quitter)
	docker compose --profile app logs -f --tail=50 collector processor aggregator

kind-up: ## Cree le cluster Kubernetes local (kind) nomme $(KIND_CLUSTER)
	kind create cluster --name $(KIND_CLUSTER) --config deploy/kind/kind-config.yaml
	kubectl cluster-info --context kind-$(KIND_CLUSTER)

kind-down: ## Supprime le cluster Kubernetes local
	kind delete cluster --name $(KIND_CLUSTER)

kind-load: ## Copie les images pulse-stream dans le cluster (il n'a pas acces aux images locales de Docker)
	@for img in $(KUBE_IMAGES); do \
		echo "-> pulse-stream/$$img:latest"; \
		kind load docker-image pulse-stream/$$img:latest --name $(KIND_CLUSTER) || exit 1; \
	done

kind-status: ## Etat du cluster : noeuds et pods de tous les espaces de noms
	kubectl get nodes
	kubectl get pods -A

helm-lint: ## Verifie la syntaxe et les bonnes pratiques du chart Helm
	helm lint $(CHART) $(HELM_FILES)

helm-template: ## Affiche les manifestes Kubernetes produits par le chart (rien n'est installe)
	helm template $(RELEASE) $(CHART) --namespace $(NAMESPACE) $(HELM_FILES) $(HELM_ARGS)

helm-install: ## Installe (ou met a jour) pulse-stream dans le cluster kind
	helm upgrade --install $(RELEASE) $(CHART) --namespace $(NAMESPACE) --create-namespace --wait --timeout 8m $(HELM_FILES) $(HELM_ARGS)

helm-uninstall: ## Desinstalle pulse-stream du cluster (supprime aussi ses donnees)
	helm uninstall $(RELEASE) --namespace $(NAMESPACE)

k8s-pods: ## Pods du namespace $(NAMESPACE), avec leur noeud et leur IP
	kubectl get pods --namespace $(NAMESPACE) -o wide

k8s-smoke: ## Test de fumee du collector qui tourne DANS le cluster (publie sur localhost:18080)
	bash scripts/smoke-collector.sh http://localhost:18080

k8s-mongo: ## Nombre de compteurs-minute dans le MongoDB du cluster
	kubectl exec --namespace $(NAMESPACE) mongo-0 -- mongosh --quiet pulse --eval 'db.minute_stats.countDocuments()'

k8s-dashboard: ## Ouvre Grafana (dans le cluster) dans le navigateur (macOS)
	open http://localhost:13000/d/pulse-stream

k8s-load: build ## Charge sur le collector DU CLUSTER (sans verification MongoDB) : make k8s-load RATES=2000 DURATION=30s
	./bin/loadgen -url http://localhost:18080/collect -verify=false -rates $(RATES) -duration $(DURATION) -workers $(WORKERS)

# Le MongoDB du cluster n'est pas joignable depuis le Mac : on ouvre un tunnel (port-forward) le temps du test.
# Le programme est lance en arriere-plan (&), son numero est garde dans .pf.pid pour le fermer a la fin, meme en cas d'echec.
k8s-load-verify: build ## Charge sur le cluster AVEC verification exacte dans MongoDB : make k8s-load-verify RATES=3000 DURATION=30s
	@kubectl port-forward --namespace $(NAMESPACE) mongo-0 27018:27017 >/dev/null 2>&1 & echo $$! > .pf.pid; \
	sleep 3; \
	./bin/loadgen -url http://localhost:18080/collect -mongo-uri mongodb://localhost:27018 -rates $(RATES) -duration $(DURATION) -workers $(WORKERS); \
	status=$$?; kill $$(cat .pf.pid) 2>/dev/null; rm -f .pf.pid; exit $$status

k8s-chaos: build ## Test de panne : make k8s-chaos APP=aggregator MODE=crash KILLS=6 INTERVAL=40 RATES=2000 DURATION=120s
	bash scripts/k8s-chaos.sh $(APP) $(MODE) $(KILLS) $(INTERVAL) -rates $(RATES) -duration $(DURATION) -workers $(WORKERS)

k8s-redeploy: ## Reconstruit les 3 images, les recharge dans kind et redemarre les 3 programmes (apres un changement de code)
	$(MAKE) docker-build IMAGES="$(KUBE_IMAGES)"
	$(MAKE) kind-load
	kubectl rollout restart --namespace $(NAMESPACE) deployment/collector deployment/processor deployment/aggregator
	kubectl rollout status --namespace $(NAMESPACE) deployment/collector deployment/processor deployment/aggregator --timeout=3m
