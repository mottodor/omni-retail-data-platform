SHELL := /bin/bash
UV := uv

.PHONY: help setup lint test unit up down logs reset

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

up down logs reset: ## docker compose wrappers (docker-compose.yml arrives in Phase 1)
	@test -f docker-compose.yml || { echo "error: docker-compose.yml not found (it arrives in Phase 1)"; exit 1; }
	@if [ "$@" = "logs" ]; then docker compose logs -f --tail=100; \
	elif [ "$@" = "reset" ]; then echo "warning: reset destroys local volumes"; docker compose down -v; \
	else docker compose $@; fi
