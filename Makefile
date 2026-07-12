# cdc-read-model — single entrypoint for every workflow.
# Run from the repo root. `make help` lists targets.

# --env-file pins the repo-root .env for ${VAR} interpolation. Without it, compose
# would look for .env next to the compose file (deploy/) and the vars come out blank.
COMPOSE := docker compose --env-file .env -f deploy/docker-compose.yml

.DEFAULT_GOAL := help

.PHONY: help up down nuke ps logs sync lint format test smoke migrate seed

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-8s\033[0m %s\n", $$1, $$2}'

## ── Stack (epic 01) ──────────────────────────────────────────────────────────

up: ## Bring up the full stack and wait until every service is healthy
	$(COMPOSE) up -d --wait

down: ## Stop the stack, keep data (named volumes survive)
	$(COMPOSE) down

nuke: ## Stop the stack and DESTROY all data (named volumes)
	$(COMPOSE) down -v

ps: ## Show service status and health
	$(COMPOSE) ps

logs: ## Tail logs from all services
	$(COMPOSE) logs -f

## ── Source database (epic 02) ────────────────────────────────────────────────

migrate: ## Apply Alembic migrations (schema + CDC capture config) to head
	uv run alembic upgrade head

seed: migrate ## Migrate then load seed data (~30 runs, ~5k specimens); idempotent
	uv run python -m cdc_pipeline.db.seed

## ── Python ───────────────────────────────────────────────────────────────────

sync: ## Install/refresh the Python environment (.venv) from pyproject + uv.lock
	uv sync

lint: ## ruff lint + format check (no changes made — fails if either would)
	uv run ruff check .
	uv run ruff format --check .

format: ## Auto-fix lint findings and reformat in place
	uv run ruff check --fix .
	uv run ruff format .

test: ## Fast unit tests — no infrastructure required
	uv run pytest tests/unit

smoke: ## Prove the live stack works end-to-end (run `make up` first)
	uv run pytest -m smoke tests/smoke
