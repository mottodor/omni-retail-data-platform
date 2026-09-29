SHELL := /bin/bash
UV := uv

.PHONY: help setup lint test unit dbt-parse dbt-build dbt-test up down logs reset smoke-core generate-oltp mutate-oltp seed-supplier-files ingest-files ingest-api bronze-load bronze-rebuild integration bi-up bi-down serving-publish serving-rebuild serving-benchmark airflow-build airflow-up airflow-down airflow-test airflow-backfill airflow-dag-test

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

bronze-load: ## Load raw archive data into Iceberg Bronze. ARGS="run --source orders --date 2026-09-18", "run-all --date 2026-09-18" or "run-new"
	@bash -c 'set -a; source .env; set +a; $(UV) run python -m omni_retail.lakehouse.bronze $(ARGS)'

bronze-rebuild: ## DESTRUCTIVE: clear only the configured Bronze schema, then rebuild it from archive with bounded Polaris recovery
	bash infrastructure/scripts/bronze_rebuild.sh

dbt-parse: ## Parse the dbt project offline (no live stack needed)
	$(UV) run dbt parse --project-dir dbt --profiles-dir dbt

dbt-build: ## Run dbt models + tests against the live core stack. ARGS="--select staging"
	@bash -c 'set -a; source .env; set +a; $(UV) run dbt build --project-dir dbt --profiles-dir dbt $(ARGS)'

dbt-test: ## Run dbt tests. ARGS="--select staging"
	@bash -c 'set -a; source .env; set +a; $(UV) run dbt test --project-dir dbt --profiles-dir dbt $(ARGS)'

integration: ## Run integration tests against the live stacks (requires `make up`; ClickHouse tests also need `make bi-up`)
	@bash -c 'set -a; source .env; set +a; no_proxy="127.0.0.1,localhost,$${no_proxy:-}" NO_PROXY="127.0.0.1,localhost,$${NO_PROXY:-}" OMNI_INTEGRATION=1 $(UV) run pytest tests/integration -v'

BI_COMPOSE := docker compose --profile bi

bi-up: ## Start the BI profile (ClickHouse + Superset; builds the custom image; requires core up)
	# Two phases (docker compose v5 `--wait` treats exited-0 one-shot
	# containers in the wait set as a failure): health-gate the long-running
	# services, then run the idempotent one-shot initializers to completion.
	# Services are targeted by name (no --profile) so the wait set stays
	# free of one-shot containers.
	docker compose up -d --wait --build clickhouse superset-postgres superset
	docker compose run --rm clickhouse-init
	docker compose run --rm superset-init

bi-down: ## Stop the bi profile services (clickhouse-data and superset metadata volumes are preserved)
	$(BI_COMPOSE) down

serving-publish: ## Publish a Gold mart to ClickHouse. ARGS="--mart mart_daily_sales"
	@bash -c 'set -a; source .env; set +a; no_proxy="127.0.0.1,localhost,$${no_proxy:-}" NO_PROXY="127.0.0.1,localhost,$${NO_PROXY:-}" $(UV) run python -m omni_retail.serving.clickhouse publish $(ARGS)'

serving-rebuild: ## Rebuild serving marts from Iceberg Gold. No ARGS = every mart; ARGS="--mart mart_daily_sales" = one mart
	@bash -c 'set -a; source .env; set +a; no_proxy="127.0.0.1,localhost,$${no_proxy:-}" NO_PROXY="127.0.0.1,localhost,$${NO_PROXY:-}" $(UV) run python -m omni_retail.serving.clickhouse rebuild $(if $(ARGS),$(ARGS),--all)'

serving-benchmark: ## Compare a representative Gold query in Trino and ClickHouse. ARGS="--repetitions 5"
	@bash -c 'set -a; source .env; set +a; no_proxy="127.0.0.1,localhost,$${no_proxy:-}" NO_PROXY="127.0.0.1,localhost,$${NO_PROXY:-}" $(UV) run python -m omni_retail.serving.clickhouse benchmark $(ARGS)'

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
