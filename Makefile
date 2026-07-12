# cdc-read-model — single entrypoint for every workflow.
# Run from the repo root. `make help` lists targets.

# --env-file pins the repo-root .env for ${VAR} interpolation. Without it, compose
# would look for .env next to the compose file (deploy/) and the vars come out blank.
COMPOSE := docker compose --env-file .env -f deploy/docker-compose.yml

.DEFAULT_GOAL := help

.PHONY: help up down nuke ps logs sync lint format test itest smoke migrate seed api load \
        connector connector-status connector-delete slot-status

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

## ── Tracking API (epic 02.2) ─────────────────────────────────────────────────

api: ## Run the tracking API locally with auto-reload (needs Postgres up + migrated)
	@set -a; [ -f .env ] && . ./.env; set +a; \
	  uv run uvicorn cdc_pipeline.api.app:app --reload --port $${API_PORT:-8000}

## ── Load generator (epic 02.3) ───────────────────────────────────────────────

# Tunables (override on the command line): SCENARIO RATE DURATION CONCURRENCY PROCESSES SEED.
# e.g. `make load SCENARIO=burst RATE=200 DURATION=120`
SCENARIO    ?= steady
RATE        ?= 1000
DURATION    ?= 60
CONCURRENCY ?= 48
PROCESSES   ?= 1
SEED        ?= 0

# Drives the API from the host. NOTE (macOS/Docker Desktop): the host→container port proxy
# both caps aggregate throughput to a few hundred req/s AND chokes on the connection storm
# from several client processes — so PROCESSES defaults to 1 here (reliable ~400–560/s). The
# tuned API itself serves each write in ~5 ms and is capable of the full 1k/s; that's best
# shown where there's no host-proxy hop — a Linux Docker host / CI, where PROCESSES>1 scales
# cleanly (see docs/pipeline/load-generator.md). Reproducible: same SEED + PROCESSES ⇒ same
# workload.
load: ## Drive write load through the API (vars: SCENARIO RATE DURATION CONCURRENCY PROCESSES SEED)
	uv run cdc-load $(SCENARIO) --rate $(RATE) --duration $(DURATION) \
	  --concurrency $(CONCURRENCY) --processes $(PROCESSES) --seed $(SEED)

## ── CDC connector (epic 03.1) ────────────────────────────────────────────────

# Registers the Debezium connector from deploy/connectors/genomics.json (${VAR}
# placeholders filled from .env). Idempotent PUT: create-if-absent, update-in-place.
# Needs `make up seed` first — the connector subscribes to the publication the
# migration owns (cdc_pub) and snapshots the seeded rows.
connector: ## Register/update the Debezium CDC connector (idempotent)
	uv run cdc-connector register

connector-status: ## Show connector + task status (RUNNING when healthy)
	uv run cdc-connector status

connector-delete: ## Delete the connector instance (keeps the replication slot)
	uv run cdc-connector delete

# The slot is Debezium's WAL cursor. `active` = a sender is attached; confirmed_flush_lsn
# advances as events are acknowledged; retained_wal is how much WAL the slot is pinning
# (the sharp edge — it grows if the connector stalls). Sourcing .env mirrors the api target.
slot-status: ## Show the Postgres replication slot (active + confirmed_flush_lsn + retained WAL)
	@set -a; [ -f .env ] && . ./.env; set +a; \
	  $(COMPOSE) exec -T postgres psql -U $${POSTGRES_USER} -d $${POSTGRES_DB} -c \
	  "SELECT slot_name, plugin, slot_type, active, active_pid, confirmed_flush_lsn, \
	   pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained_wal \
	   FROM pg_replication_slots;"

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

itest: ## API integration tests against the compose Postgres (run `make up migrate` first)
	uv run pytest -m integration tests/integration

smoke: ## Prove the live stack works end-to-end (run `make up` first)
	uv run pytest -m smoke tests/smoke
