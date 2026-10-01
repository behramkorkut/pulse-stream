.DEFAULT_GOAL := help

MODULE  := github.com/behramkorkut/pulse-stream
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
TOPIC   ?= raw-events
RATES    ?= 200,500,1000
DURATION ?= 20s
WORKERS  ?= 128
LDFLAGS := -X $(MODULE)/internal/version.Version=$(VERSION)

.PHONY: help doctor fmt fmt-check vet test cover ci test-integration build clean run-collector run-processor run-aggregator smoke poison sessions demo-sessions aggregates demo-dedupe metrics traffic load dashboard topics consume group group-aggregator up down ps logs

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
