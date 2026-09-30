.DEFAULT_GOAL := help

MODULE  := github.com/behramkorkut/pulse-stream
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X $(MODULE)/internal/version.Version=$(VERSION)

.PHONY: help doctor fmt vet test test-integration build clean run-collector smoke topics consume up down ps logs

help: ## Affiche cette aide
	grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

doctor: ## Verifie les outils installes et leurs versions
	bash scripts/check-tools.sh

fmt: ## Formate le code Go
	go fmt ./...

vet: ## Analyse statique du code Go
	go vet ./...

test: ## Lance les tests avec le detecteur de data races
	go test -race -count=1 ./...

test-integration: ## Tests d'integration (necessite make up)
	go test -race -count=1 -tags=integration ./internal/collector/...

build: ## Compile les binaires dans ./bin
	@mkdir -p bin
	go build -ldflags "$(LDFLAGS)" -o bin/ ./cmd/...

clean: ## Supprime les binaires
	rm -rf bin

run-collector: ## Lance le collector en local (Ctrl+C pour l'arreter)
	go run ./cmd/collector

smoke: ## Test de fumee du collector (il doit deja tourner)
	bash scripts/smoke-collector.sh

topics: ## Cree les topics Kafka (infrastructure demarree)
	bash scripts/create-topics.sh

consume: ## Affiche les messages du topic raw-events (Ctrl+C pour quitter)
	docker compose exec redpanda rpk topic consume raw-events -o start -f 'partition=%p offset=%o key=%k value=%v\n'

up: ## Demarre l'infrastructure locale (Redpanda, Redis, MongoDB)
	docker compose up -d --wait

down: ## Arrete l'infrastructure locale
	docker compose down

ps: ## Etat des conteneurs
	docker compose ps

logs: ## Suit les journaux des conteneurs
	docker compose logs -f --tail=50
