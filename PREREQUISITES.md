# Prerequisites

What to install on a fresh machine before running the stack. Run
`./scripts/bootstrap.sh` at any time — it checks all of this and tells you exactly
what's missing (it never installs anything for you).

Target platform: **macOS on Apple Silicon (arm64)**. Every image in the stack is
multi-arch, so Intel Macs and Linux work too; only the install commands differ.

## Required

The container runtime is **Docker Desktop**, with **Colima as a documented fallback** —
see [`docs/adr/0002-colima-over-docker-desktop.md`](docs/adr/0002-colima-over-docker-desktop.md)
for the (bumpy) history. Never run both at once — they fight over host ports and the
active `docker` context.

| Tool | Why | Install |
|---|---|---|
| **Docker Desktop** | Container engine + `docker`/`docker compose` CLIs for the whole stack. | `brew install --cask docker`, then launch **Docker.app** once so the engine starts. Or the `.dmg` from docker.com. |
| **uv** | Python project & dependency manager (`cdc_pipeline` package, all consumers, tools, tests). | Already installed ✅ — else `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| **make** | Single entrypoint for every workflow (`make up`, `make smoke`, …). | Ships with macOS ✅ |
| **git** | Version control. | Ships with macOS ✅ |

> **Docker Desktop resources:** in Settings → Resources give it at least **6 GB RAM**
> (8 GB comfortable) and 4 CPUs, or Elasticsearch will exit on startup.

> **Wrong-context symptom:** if `docker ps` looks unexpectedly empty, check
> `docker context show` — it should be `desktop-linux`. An empty list almost always means
> the wrong context is active or two engines are running.

### Fallback: Colima (if Docker Desktop misbehaves)

Docker Desktop hung once on this machine; Colima is the escape hatch. It's a lightweight
CLI-only Linux VM the same CLIs talk to unchanged.

```bash
brew install colima docker docker-compose      # Homebrew required (installer prints PATH lines)
# Homebrew flags that the compose plugin lives in /opt/homebrew/lib/docker/cli-plugins;
# link it so `docker compose` finds it:
mkdir -p ~/.docker/cli-plugins && \
  ln -sf /opt/homebrew/lib/docker/cli-plugins/docker-compose ~/.docker/cli-plugins/docker-compose
colima start --cpu 4 --memory 8 --disk 60      # 8 GB VM so Elasticsearch is happy
docker context show                            # → colima
```

Switch back with `docker context use desktop-linux` (and `colima stop` to free the RAM).

## Recommended (not blocking)

| Tool | Why | Install |
|---|---|---|
| **kcat** | Command-line Kafka producer/consumer — invaluable for inspecting CDC events by hand. | `brew install kcat` |
| **jq** | Pretty-print the JSON from Kafka Connect / Elasticsearch REST calls. | Ships with macOS ✅ (else `brew install jq`) |

## First-run sequence

```bash
./scripts/bootstrap.sh   # verify prerequisites, copy .env, sync Python deps
# ...install anything it flags as missing, then re-run until it's all green...
open -a Docker           # start Docker Desktop's engine (wait for it to say "running")
make up                  # bring the stack up, health-checked (story 01.1)
make smoke               # prove every service works (added in story 01.3)
```

Ports each service binds on your machine are listed in [`.env.example`](.env.example)
and printed by `./scripts/bootstrap.sh --ports`.
