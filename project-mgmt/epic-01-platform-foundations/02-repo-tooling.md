# Story 01.2 — Repo tooling

- **Status:** todo
- **Effort:** M
- **Depends on:** —

## Context

One Python project houses the API, all consumers, and the tools. Consistent tooling now
means every later story starts at the interesting part.

## Tasks

- [ ] `uv`-managed project: `pyproject.toml`, one package `cdc_pipeline` with
      subpackages (`api`, `consumers`, `tools`), pinned deps
      (fastapi, sqlalchemy/psycopg, confluent-kafka, elasticsearch, redis, typer).
- [ ] `ruff` (lint + format) and `pytest` configured; `make lint`, `make test`.
- [ ] Makefile as the single entrypoint: `up`, `down`, `nuke`, `lint`, `test`, `smoke`
      (stubs allowed for targets owned by later stories, but they must fail loudly, not
      silently succeed).
- [ ] GitHub Actions: lint + unit tests on push; a second job that runs `make up &&
      make smoke` with compose (this also keeps the stack honest on CI's clean machine).
- [ ] `.env.example` with every connection string; nothing hardcoded in code.

## Acceptance criteria

- [ ] Fresh clone: `uv sync && make lint test` passes.
- [ ] CI is green on the initial commit, including the compose smoke job.

## What this teaches

Mostly leverage rather than new concepts — but CI running the full compose stack is a
genuinely useful pattern (integration tests against real infra, no mocks) that pays off
in every subsequent epic.
