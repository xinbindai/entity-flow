"""
Alembic environment.

The database URL is deliberately not in alembic.ini -- it comes from
POSTGRES_URL in the environment, or from .env at the repo root, so credentials
stay out of version control and match what the test suite and demo already use.

    alembic upgrade head                       # apply everything
    alembic revision --autogenerate -m "..."   # draft the next one
    alembic downgrade -1                       # step back

Autogenerate sees tables, columns, indexes and constraints. It does NOT see
the PL/pgSQL status-validation trigger, so any revision that changes it has to
say so by hand -- see the initial revision for the pattern.
"""

from __future__ import annotations

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from entitymodel.models import Base  # noqa: E402

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _database_url() -> str:
    """POSTGRES_URL from the environment, falling back to .env at the root."""
    url = os.environ.get("POSTGRES_URL")
    if url:
        return url

    env_file = REPO_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            if key.strip() == "POSTGRES_URL":
                return value.strip().strip('"').strip("'")

    raise SystemExit(
        "POSTGRES_URL is not set. Export it, or copy "
        f"{REPO_ROOT / '.env.example'} to .env and fill it in."
    )


# compare_type matters here: timestamp -> timestamptz is exactly the kind of
# change that made these migrations necessary, and autogenerate ignores type
# changes without it.
_COMPARE = {"compare_type": True, "compare_server_default": True}


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        **_COMPARE,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = _database_url()
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata, **_COMPARE)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
