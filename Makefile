# cdc-read-model — single entrypoint for every workflow.
# Run from the repo root. `make help` lists targets.

# --env-file pins the repo-root .env for ${VAR} interpolation. Without it, compose
# would look for .env next to the compose file (deploy/) and the vars come out blank.
COMPOSE := docker compose --env-file .env -f deploy/docker-compose.yml

.DEFAULT_GOAL := help

.PHONY: help up down nuke ps logs lint test smoke

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

## ── Python (owned by later stories — stubs fail loudly, never silently) ───────

lint: ## (story 01.2) ruff lint + format check
	@echo "make lint: not implemented yet — owned by story 01.2 (repo tooling)"; exit 1

test: ## (story 01.2) pytest unit tests
	@echo "make test: not implemented yet — owned by story 01.2 (repo tooling)"; exit 1

smoke: ## (story 01.3) prove the live stack actually works
	@echo "make smoke: not implemented yet — owned by story 01.3 (smoke tests)"; exit 1
