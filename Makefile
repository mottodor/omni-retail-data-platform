SHELL := /bin/bash
UV := uv

.PHONY: help setup lint test unit up down logs reset smoke-core generate-oltp mutate-oltp seed-supplier-files ingest-files ingest-api

help: ## List available commands
	@grep -E '^[a-zA-Z _-]+: ## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ": ## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Create the environment and install git hooks
	$(UV) sync
	$(UV) run pre-commit install

lint: ## Run ruff (check + format) and mypy
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run mypy src tests

test: ## Run unit tests
	$(UV) run pytest

unit: test ## Alias for unit tests

COMPOSE := docker compose --profile core

up: ## Start core infrastructure and wait until healthy
	$(COMPOSE) up -d --wait

down: ## Stop services (named volumes are preserved)
	$(COMPOSE) down

logs: ## Follow service logs
	docker compose logs -f --tail=100

reset: ## WARNING: destroy containers AND named volumes (all local data)
	@echo "warning: reset destroys all local volumes (postgres, minio, polaris metadata)"
	@$(COMPOSE) down -v

smoke-core: ## End-to-end check: Trino -> Polaris -> Iceberg -> MinIO
	bash infrastructure/scripts/smoke_core.sh

EVENTS ?= 200

generate-oltp: ## Apply OLTP schema and load initial data (10k/5k/100k, seed 42). ARGS="--orders 1000" to override
	@bash -c 'set -a; source .env; set +a; $(UV) run python -m omni_retail.generators.oltp initial $(ARGS)'

mutate-oltp: ## Apply EVENTS random mutations (inserts/updates/deletes) to the OLTP source
	@bash -c 'set -a; source .env; set +a; $(UV) run python -m omni_retail.generators.oltp mutate --events $(EVENTS) $(ARGS)'

ROWS ?= 200
SEED ?= 7

seed-supplier-files: ## Generate deterministic vendor files and upload to landing. ARGS="--rows 500 --seed 11" to override
	@bash -c 'set -a; source .env; set +a; $(UV) run python -m omni_retail.generators.vendor_files generate --upload --rows $(ROWS) --seed $(SEED) $(ARGS)'

ingest-files: ## Process pending vendor files (landing -> processing -> archive | rejected). ARGS="--source supplier-prices --date 2026-09-11"
	@bash -c 'set -a; source .env; set +a; $(UV) run python -m omni_retail.ingestion.files process $(ARGS)'

ingest-api: ## Fetch raw API pages into the archive bucket. ARGS="run --source fx-rates --date 2026-09-11" or "backfill --source fx-rates --from 2026-09-01 --to 2026-09-10"
	@bash -c 'set -a; source .env; set +a; $(UV) run python -m omni_retail.ingestion.api $(ARGS)'
