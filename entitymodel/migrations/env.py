"""
Alembic environment.

These migrations ship inside the installed package, so a deployment that only
has the wheel can still build and evolve its schema -- see entitymodel.migrate,
which is the supported entry point:

    python -m entitymodel.migrate upgrade head --db-url postgresql+psycopg2://...

From a checkout, the alembic CLI works as before and reads .env:

    alembic upgrade head                       # apply everything
    alembic revision --autogenerate -m "..."   # draft the next one
    alembic downgrade -1                       # step back

The database URL is deliberately not in alembic.ini. It is taken, in order,
from the caller (migrate.py passes it through config.attributes), then
POSTGRES_URL in the environment, then a .env in the working directory -- so
credentials stay out of version control, and an installed copy never depends
on a repository layout that is not there.

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

# Only needed when running from a checkout, where entitymodel may not be on
# the path yet. Installed, it already is, and this resolves to site-packages
# where the insert is harmless.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from entitymodel.models import Base  # noqa: E402

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _database_url() -> str:
    """
    The caller's url, then POSTGRES_URL, then a .env in the working directory.

    The caller comes first so entitymodel.migrate can pass --db-url straight
    through: an installed copy has no repository around it, and a deployment
    should not have to set an environment variable to name the database it is
    already holding a connection string for.
    """
    url = config.attributes.get("db_url")
    if url:
        return url

    url = os.environ.get("POSTGRES_URL")
    if url:
        return url

    env_file = Path.cwd() / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            if key.strip() == "POSTGRES_URL":
                return value.strip().strip('"').strip("'")

    raise SystemExit(
        "No database URL. Pass --db-url, export POSTGRES_URL, or put it in a "
        ".env in the working directory."
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
