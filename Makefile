SHELL := /bin/bash
UV := uv

.PHONY: help setup lint test unit dbt-parse dbt-build dbt-test up down logs reset smoke-core generate-oltp mutate-oltp seed-supplier-files ingest-files ingest-api bronze-load integration airflow-build airflow-up airflow-down airflow-test airflow-backfill airflow-dag-test

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

bronze-load: ## Load raw archive data into Iceberg Bronze. ARGS="run --source orders --date 2026-09-18" or "run-all --date 2026-09-18"
	@bash -c 'set -a; source .env; set +a; $(UV) run python -m omni_retail.lakehouse.bronze $(ARGS)'

dbt-parse: ## Parse the dbt project offline (no live stack needed)
	$(UV) run dbt parse --project-dir dbt --profiles-dir dbt

dbt-build: ## Run dbt models + tests against the live core stack. ARGS="--select staging"
	@bash -c 'set -a; source .env; set +a; $(UV) run dbt build --project-dir dbt --profiles-dir dbt $(ARGS)'

dbt-test: ## Run dbt tests. ARGS="--select staging"
	@bash -c 'set -a; source .env; set +a; $(UV) run dbt test --project-dir dbt --profiles-dir dbt $(ARGS)'

integration: ## Run integration tests against the live core stack (requires `make up`)
	@bash -c 'set -a; source .env; set +a; no_proxy="127.0.0.1,localhost,$${no_proxy:-}" NO_PROXY="127.0.0.1,localhost,$${NO_PROXY:-}" OMNI_INTEGRATION=1 $(UV) run pytest tests/integration -v'

AIRFLOW_COMPOSE := docker compose --profile orchestration

airflow-build: ## Build the custom Airflow image (omni-retail/airflow:0.1.0)
	$(AIRFLOW_COMPOSE) build

airflow-up: ## Start Airflow (profile orchestration; requires the core profile up)
	$(AIRFLOW_COMPOSE) up -d --wait

airflow-down: ## Stop Airflow services (metadata and logs volumes are preserved)
	$(AIRFLOW_COMPOSE) down

airflow-test: ## Run DAG import/structure tests inside the Airflow image
	bash infrastructure/scripts/airflow_tests.sh

airflow-backfill: ## Backfill a DAG. ARGS="ingest_fx_api -s 2026-09-01 -e 2026-09-10"
	$(AIRFLOW_COMPOSE) exec airflow-scheduler airflow dags backfill $(ARGS)

airflow-dag-test: ## Run one DAG for a logical date. ARGS="ingest_fx_api 2026-09-10"
	$(AIRFLOW_COMPOSE) exec airflow-scheduler airflow dags test $(ARGS)
