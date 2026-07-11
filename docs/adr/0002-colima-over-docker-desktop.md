# 0002 — Container runtime: Docker Desktop, with Colima as fallback

- **Status:** Accepted
- **Date:** 2026-07-12
- **Context story:** 01.1 (compose stack)

## Context

The stack runs on `docker compose`, which needs a container engine underneath. On this
macOS/Apple Silicon machine the runtime choice was not smooth, and the journey is worth
recording because it explains the fallback we keep documented.

1. **Docker Desktop, first attempt — hung.** Installed, and its `docker` CLI parsed
   compose files fine, but the **engine never became usable**: `docker info` blocked
   indefinitely (2-minute timeout, no response), so `make up` could not run.
2. **Colima — worked.** Installed Colima + the standalone `docker`/`docker-compose`
   CLIs via Homebrew. `colima start` provisioned a lightweight Linux VM (Apple `vz`
   framework) and `make up` brought all six services healthy on the first try.
3. **Docker Desktop recovered.** Shortly after, Docker Desktop's engine started working.
   Running **both** engines at once caused a real problem: they contend for the same
   published host ports (5432/9092/9200/…), and Docker Desktop sets itself as the active
   `docker` context on startup — which made containers appear to "vanish" (we were
   querying Desktop while the stack was still in Colima).

## Decision

Use **Docker Desktop** as the primary runtime. Keep **Colima documented as a fallback**
for when Docker Desktop misbehaves (it already did once here).

Because we depend only on the standard Docker API and `docker compose`, the runtime is a
swappable detail — the compose files, Makefile, and consumers are identical on either.

**Do not run both at once.** Pick one; if switching, stop the other first
(`colima stop`, or quit Docker Desktop) so they don't fight over host ports and the
active context.

## Rationale

- **Docker Desktop is the mainstream, GUI-managed default** the developer prefers now
  that it works; resource limits live in Settings → Resources.
- **Colima earned its place as the fallback:** free, CLI-only, no always-on GUI daemon,
  and it was the thing that actually worked when Desktop was wedged. One command to fall
  back: `colima start --cpu 4 --memory 8`.
- **Zero blast radius either way** — nothing in the project references a specific runtime.

## Consequences

- The active context must be **`desktop-linux`** when using Docker Desktop
  (`docker context use desktop-linux`); Colima's context is `colima`. Confirm with
  `docker context show` if `docker ps` ever looks unexpectedly empty — that symptom is
  almost always "wrong active context / two engines running."
- Give Docker Desktop ≥6 GB RAM (8 GB comfortable) so Elasticsearch starts.
- [PREREQUISITES.md](../../PREREQUISITES.md) documents Docker Desktop as the supported
  path and Colima as the fallback.

## Alternatives considered

- **Colima as primary:** viable and proven here, but demoted to fallback per developer
  preference once Desktop recovered.
- **Rancher Desktop / Podman:** not needed — Desktop (primary) and Colima (fallback)
  cover the requirement.
