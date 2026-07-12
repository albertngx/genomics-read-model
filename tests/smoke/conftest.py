"""Shared fixtures for the smoke suite.

The smoke tests run against the *live* stack (``make up`` first). They read connection
strings from the environment; to make ``make smoke`` work whether or not the caller
exported ``.env``, we load the repo-root ``.env`` here without clobbering variables that
are already set (so CI or an explicit ``export`` still wins).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from cdc_pipeline.config import Settings

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            continue
        key = key.strip()
        if key in os.environ:  # already set — don't override the caller/CI
            continue
        # drop an inline "  # comment" and any surrounding quotes
        value = value.split(" #", 1)[0].strip().strip('"').strip("'")
        os.environ[key] = value


@pytest.fixture(scope="session")
def settings() -> Settings:
    _load_dotenv(_REPO_ROOT / ".env")
    return Settings.from_env()
