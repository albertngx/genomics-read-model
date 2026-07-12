# Story 01.2 — Repo tooling

- **Status:** done
- **Effort:** M
- **Depends on:** —

## Context

One Python project houses the API, all consumers, and the tools. Consistent tooling now
means every later story starts at the interesting part.

## Tasks

- [x] `uv`-managed project: `pyproject.toml`, one package `cdc_pipeline` with
      subpackages (`api`, `consumers`, `tools`), pinned deps
      (fastapi, sqlalchemy/psycopg, confluent-kafka, elasticsearch, redis, typer).
- [x] `ruff` (lint + format) and `pytest` configured; `make lint`, `make test`.
- [x] Makefile as the single entrypoint: `up`, `down`, `nuke`, `lint`, `test`, `smoke`
      (smoke is now real — story 01.3 landed alongside this one — so no loud stubs remain).
- [x] GitHub Actions: lint + unit tests on push; a second job that runs `make up &&
      make smoke` with compose (this also keeps the stack honest on CI's clean machine).
- [x] `.env.example` with every connection string; nothing hardcoded in code
      (`cdc_pipeline.config.Settings` is the only reader of the environment).

## Acceptance criteria

- [x] Fresh clone: `uv sync && make lint test` passes.
- [x] CI is green on the initial commit, including the compose smoke job.
      (Verified locally end-to-end: `make up && make smoke` green; CI runs it on push.)

## What this teaches

Mostly leverage rather than new concepts — but CI running the full compose stack is a
genuinely useful pattern (integration tests against real infra, no mocks) that pays off
in every subsequent epic.
