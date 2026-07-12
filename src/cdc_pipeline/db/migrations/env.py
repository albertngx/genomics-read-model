"""Alembic runtime environment.

The database URL is not read from alembic.ini — it comes from the repo-root ``.env``
(the single source of truth for connection strings) via ``cdc_pipeline.db.engine``.
``target_metadata`` is wired to the shared models so ``alembic revision --autogenerate``
can diff future changes against them, even though the initial migrations are hand-written.
"""

from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool

from cdc_pipeline.db.engine import database_url_from_env, sqlalchemy_url
from cdc_pipeline.db.models import Base

config = context.config
target_metadata = Base.metadata

# Inject the URL from .env so the ini stays credential-free.
config.set_main_option("sqlalchemy.url", sqlalchemy_url(database_url_from_env()))


def run_migrations_offline() -> None:
    """Emit SQL to stdout without a live connection (``alembic upgrade head --sql``)."""
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live connection."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
