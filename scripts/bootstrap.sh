#!/usr/bin/env bash
#
# bootstrap.sh — initialize the cdc-read-model project on a fresh machine.
#
# Idempotent. Never installs anything for you — it checks prerequisites, tells you
# exactly what's missing and how to get it, then prepares the local project (.env +
# Python deps). Safe to re-run any time.
#
#   ./scripts/bootstrap.sh          check prereqs, set up .env, sync Python deps
#   ./scripts/bootstrap.sh --check  only check prerequisites (no changes)
#   ./scripts/bootstrap.sh --ports  print the locked port map and exit
#   ./scripts/bootstrap.sh --layout print the repo layout and exit
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."   # repo root, regardless of caller's cwd

# ── pretty output ────────────────────────────────────────────────────────────
if [[ -t 1 ]]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; RED=$'\033[31m'; GRN=$'\033[32m'
  YEL=$'\033[33m'; BLU=$'\033[34m'; RST=$'\033[0m'
else
  BOLD=; DIM=; RED=; GRN=; YEL=; BLU=; RST=
fi
ok()   { printf "  ${GRN}✓${RST} %s\n" "$1"; }
warn() { printf "  ${YEL}!${RST} %s\n" "$1"; }
bad()  { printf "  ${RED}✗${RST} %s\n" "$1"; }
head() { printf "\n${BOLD}%s${RST}\n" "$1"; }

MISSING_REQUIRED=0

# ── content descriptions ─────────────────────────────────────────────────────
print_layout() {
  head "Repository layout"
  cat <<EOF
  ${BOLD}ARCHITECTURE.md${RST}   system design: components, topics, delivery semantics, decisions
  ${BOLD}README.md${RST}         project overview and quickstart
  ${BOLD}PREREQUISITES.md${RST}  what to install before running the stack
  ${BOLD}.env.example${RST}      single source of truth for ports & connection strings
  ${BOLD}project-mgmt/${RST}     epics (folders) and stories (files) — the build plan
  ${BOLD}scripts/${RST}          bootstrap + operational helper scripts
  ${BOLD}deploy/${RST}           docker-compose stack           ${DIM}(built in epic 01)${RST}
  ${BOLD}src/cdc_pipeline/${RST} api, consumers, tools packages ${DIM}(built in epics 02+)${RST}
  ${BOLD}tests/${RST}            smoke + unit + integration tests
  ${BOLD}tools/${RST}            verifier, replay & demo scripts ${DIM}(built in epic 06)${RST}
EOF
}

print_ports() {
  head "Locked service ports (host → container)"
  cat <<EOF
  ${BOLD}Service${RST}               ${BOLD}Port${RST}   ${BOLD}Reach it at${RST}
  Postgres              5432   postgresql://cdc:cdc@localhost:5432/genomics
  Kafka broker          9092   localhost:9092 (client bootstrap)
  Kafka Connect (REST)  8083   http://localhost:8083   ${DIM}Debezium mgmt API${RST}
  kafka-ui              8080   http://localhost:8080   ${DIM}web console${RST}
  Elasticsearch         9200   http://localhost:9200
  Redis                 6379   redis://localhost:6379
  Tracking API          8000   http://localhost:8000/docs  ${DIM}(epic 02)${RST}
${DIM}  Values live in .env.example — change a port there, once.${RST}
EOF
}

# ── one prerequisite check ───────────────────────────────────────────────────
# usage: check_tool <cmd> <required|optional> <install hint>
check_tool() {
  local cmd="$1" level="$2" hint="$3" version=""
  if command -v "$cmd" >/dev/null 2>&1; then
    case "$cmd" in
      docker)  version="$(docker --version 2>/dev/null | sed 's/,.*//')" ;;
      uv)      version="$(uv --version 2>/dev/null)" ;;
      make)    version="$(make --version 2>/dev/null | head -1)" ;;
      git)     version="$(git --version 2>/dev/null)" ;;
      *)       version="$($cmd --version 2>/dev/null | head -1)" ;;
    esac
    ok "${cmd}  ${DIM}${version}${RST}"
    return 0
  fi
  if [[ "$level" == required ]]; then
    bad "${cmd} — MISSING (required). ${hint}"
    MISSING_REQUIRED=1
  else
    warn "${cmd} — not found (optional). ${hint}"
  fi
  return 1
}

check_prereqs() {
  head "Required"
  check_tool uv     required "Install: curl -LsSf https://astral.sh/uv/install.sh | sh"
  check_tool make   required "Ships with macOS / Xcode command line tools."
  check_tool git    required "Ships with macOS / Xcode command line tools."
  if check_tool docker required "Install Docker Desktop: brew install --cask docker (then launch Docker.app)"; then
    # docker present — is the engine actually running?
    if docker info >/dev/null 2>&1; then
      ok "docker engine reachable"
    else
      warn "docker installed but engine not running — launch Docker Desktop (Docker.app)"
    fi
  fi

  head "Recommended (not blocking)"
  check_tool brew optional "Install: /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\"" || true
  check_tool kcat optional "Handy for inspecting Kafka events: brew install kcat" || true
  check_tool jq   optional "Pretty-prints REST JSON: brew install jq" || true
}

# ── local project setup ──────────────────────────────────────────────────────
setup_env() {
  head "Local project setup"
  if [[ -f .env ]]; then
    ok ".env already exists (left untouched)"
  else
    cp .env.example .env
    ok ".env created from .env.example"
  fi

  if command -v uv >/dev/null 2>&1 && [[ -f pyproject.toml ]]; then
    printf "  ${DIM}running uv sync…${RST}\n"
    uv sync
    ok "Python dependencies synced (.venv)"
  else
    warn "skipping 'uv sync' — pyproject.toml not present yet (added in story 01.2)"
  fi
}

# ── entrypoint ───────────────────────────────────────────────────────────────
case "${1:-}" in
  --ports)  print_ports; exit 0 ;;
  --layout) print_layout; exit 0 ;;
  --check)  check_prereqs
            if [[ $MISSING_REQUIRED -eq 0 ]]; then
              printf "\n${GRN}${BOLD}All required prerequisites present.${RST}\n"; exit 0
            else
              printf "\n${RED}${BOLD}Missing required prerequisites — see above.${RST}\n"; exit 1
            fi ;;
  ""|--all)
    printf "${BOLD}${BLU}cdc-read-model — bootstrap${RST}\n"
    print_layout
    check_prereqs
    if [[ $MISSING_REQUIRED -ne 0 ]]; then
      printf "\n${RED}${BOLD}Install the required tools above, then re-run this script.${RST}\n"
      printf "${DIM}Details: PREREQUISITES.md${RST}\n"
      exit 1
    fi
    setup_env
    print_ports
    head "Next"
    printf "  make up      ${DIM}bring up the stack (story 01.1)${RST}\n"
    printf "  make smoke   ${DIM}prove it works (story 01.3)${RST}\n"
    printf "\n${GRN}${BOLD}Bootstrap complete.${RST}\n"
    ;;
  -h|--help)
    grep '^#' "$0" | sed 's/^# \{0,1\}//' | sed -n '2,20p' ;;
  *) bad "unknown option: $1"; exit 2 ;;
esac
