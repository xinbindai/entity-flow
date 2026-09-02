"""
Apply this package's database migrations, from wherever it is installed.

The schema entitymodel needs -- the entity table and its validation trigger,
the event log, the per-handler checkpoint and failure tables -- is created and
evolved by Alembic revisions that ship inside the package. A deployment that
only has the wheel can therefore build its database without a checkout:

    python -m entitymodel.migrate upgrade head --db-url postgresql+psycopg2://host/db
    python -m entitymodel.migrate current      --db-url ...
    python -m entitymodel.migrate history
    python -m entitymodel.migrate downgrade -1 --db-url ...

or, installed as a console script, `entity-flow-migrate upgrade head`.

--sql prints the statements instead of running them, for a deployment that
applies schema changes through review rather than from the application:

    python -m entitymodel.migrate upgrade head --sql --db-url ...

The URL may also come from POSTGRES_URL, or from a .env in the working
directory; --db-url wins over both.

Alembic is an extra rather than a hard dependency -- `pip install
entity-flow[migrations]`. Most consumers run migrations from a deploy job or a
one-off container rather than from the application process, and a library that
owns the schema still should not put a migration tool into every environment
that merely reads from it. The import error below says so rather than leaving
a bare ModuleNotFoundError to be interpreted.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

__all__ = ["make_config", "migrations_path"]

_MISSING_ALEMBIC = (
    "Alembic is not installed. It ships as an extra, because a process that "
    "only reads from the database has no use for it:\n\n"
    "    pip install 'entity-flow[migrations]'\n"
)


def migrations_path() -> Path:
    """
    The packaged migrations directory.

    Resolved from this module's location rather than from the working
    directory, which is the whole point: installed, there is no repository to
    be relative to.
    """
    path = Path(__file__).resolve().parent / "migrations"
    if not (path / "env.py").exists():
        raise SystemExit(
            f"packaged migrations are missing from {path}. This is a packaging "
            f"fault rather than a configuration one -- the wheel should contain "
            f"entitymodel/migrations/."
        )
    return path


def make_config(db_url: str | None = None):
    """
    An Alembic Config pointing at the packaged migrations.

    Use this to drive migrations from your own code -- a deploy script, a test
    fixture, an application's startup check -- rather than through the CLI:

        from alembic import command
        from entitymodel.migrate import make_config
        command.upgrade(make_config(url), "head")

    db_url is passed to env.py through config.attributes rather than as
    sqlalchemy.url, so a URL containing a percent sign survives: main options
    go through ConfigParser interpolation, and attributes do not.
    """
    try:
        from alembic.config import Config
    except ImportError as exc:  # pragma: no cover - depends on the install
        raise SystemExit(_MISSING_ALEMBIC) from exc

    config = Config()
    config.set_main_option("script_location", str(migrations_path()))
    if db_url:
        config.attributes["db_url"] = db_url
    return config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m entitymodel.migrate",
        description="Apply entity-flow's database migrations.",
    )
    parser.add_argument(
        "action",
        choices=["upgrade", "downgrade", "current", "history", "heads"],
        help="what to do",
    )
    parser.add_argument(
        "revision",
        nargs="?",
        default=None,
        help="target revision: 'head', a revision id, or a relative step like -1",
    )
    parser.add_argument(
        "--db-url",
        default=None,
        help="database URL; also read from POSTGRES_URL or a .env in the cwd",
    )
    parser.add_argument(
        "--sql",
        action="store_true",
        help="print the SQL instead of running it (offline mode)",
    )
    args = parser.parse_args(argv)

    try:
        from alembic import command
    except ImportError as exc:  # pragma: no cover - depends on the install
        raise SystemExit(_MISSING_ALEMBIC) from exc

    config = make_config(args.db_url)

    if args.action in ("upgrade", "downgrade"):
        if args.revision is None:
            # 'upgrade' with no target is ambiguous and 'downgrade' with none
            # is dangerous; neither should be guessed at.
            raise SystemExit(
                f"{args.action} needs a revision, e.g. "
                f"'{args.action} {'head' if args.action == 'upgrade' else '-1'}'"
            )
        getattr(command, args.action)(config, args.revision, sql=args.sql)
    elif args.action == "current":
        command.current(config, verbose=True)
    elif args.action == "history":
        command.history(config, verbose=True)
    else:
        command.heads(config, verbose=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
