"""
This lab's entity taxonomy: which (category, subcategory) pairs exist and what
statuses each may hold.

The data lives in entity_types.csv and entity_statuses.csv beside this file,
so it can be edited without touching Python, and the reconciliation logic
lives in entitymodel.taxonomy_sync, so it is reusable by any deployment. What
remains here is only the pointer from one to the other. Those two CSVs are the
single copy of the taxonomy -- entity_schema_unified.sql defines the tables
but deliberately does not list their rows.

This is domain configuration, not schema, which is why it sits at the repo
root rather than inside the entitymodel package: models.py defines the shape
of any entity-event-task system, these files say what *this* lab's entities
are. A different deployment keeps the package and replaces these.

It is also not test or demo data -- a real deployment must seed these rows
before it can insert a single entity, because the validate_entity_status
trigger rejects any status with no matching entity_status row.

    python taxonomy.py postgresql+psycopg2://localhost/lab            # create + seed
    python taxonomy.py postgresql+psycopg2://localhost/lab --sync     # reconcile only
    python taxonomy.py postgresql+psycopg2://localhost/lab --dry-run  # show the diff
"""

from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy.orm import Session

from entitymodel.taxonomy_sync import TaxonomyDiff, sync_taxonomy_from_csv

HERE = Path(__file__).resolve().parent
ENTITY_TYPES_CSV = HERE / "entity_types.csv"
ENTITY_STATUSES_CSV = HERE / "entity_statuses.csv"

__all__ = ["ENTITY_STATUSES_CSV", "ENTITY_TYPES_CSV", "seed_taxonomy", "sync_taxonomy"]


def sync_taxonomy(
    session: Session, *, delete_missing: bool = False, dry_run: bool = False
) -> TaxonomyDiff:
    """
    Reconcile the database with the CSVs: insert what is new, update what
    changed, and report what the files no longer mention. Safe to re-run --
    an unchanged pair of files produces no writes.

    The caller commits.
    """
    return sync_taxonomy_from_csv(
        session,
        ENTITY_TYPES_CSV,
        ENTITY_STATUSES_CSV,
        delete_missing=delete_missing,
        dry_run=dry_run,
    )


def seed_taxonomy(session: Session) -> None:
    """Seed a fresh database. Kept as the name callers already use; it is now
    sync_taxonomy plus the commit, and works on a populated database too."""
    sync_taxonomy(session)
    session.commit()


if __name__ == "__main__":
    from sqlalchemy import create_engine

    from entitymodel.models import Base

    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    url = args[0] if args else "postgresql+psycopg2://localhost/lab_platform"
    engine = create_engine(url)

    if "--dry-run" in sys.argv:
        with Session(engine) as session:
            print(sync_taxonomy(session, dry_run=True).summary())
    elif "--sync" in sys.argv:
        with Session(engine) as session:
            diff = sync_taxonomy(session, delete_missing="--delete-missing" in sys.argv)
            session.commit()
        print(diff.summary() if diff else "taxonomy already matches the CSVs")
    else:
        # Bootstrap a dev/test database in one call. Use Alembic rather than
        # create_all() for anything longer-lived than this.
        Base.metadata.create_all(engine)
        with Session(engine) as session:
            seed_taxonomy(session)
        print(f"created schema and seeded taxonomy in {url}")
