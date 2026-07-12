# Container image for the tracking API (story 02.2).
#
# Built by the `api` compose service (context is the repo root). Uses the uv base image
# so the build matches local dev exactly — same resolver, same uv.lock.
#
#   docker compose --env-file .env -f deploy/docker-compose.yml up -d --build api

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# curl is used by the compose healthcheck to hit /health.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency manifests first so the (slow) dependency layer caches across code edits.
COPY pyproject.toml uv.lock ./
# Install runtime deps only (no dev group), into a project venv, from the frozen lock.
RUN uv sync --frozen --no-dev --no-install-project

# Now the source, then install the package itself.
COPY src ./src
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

# The API listens on all interfaces inside the container; the host port is mapped in compose.
CMD ["uvicorn", "cdc_pipeline.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
