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

from entitymodel.models import Entity
from entitymodel.taxonomy_sync import TaxonomyDiff, sync_taxonomy_from_csv

HERE = Path(__file__).resolve().parent
ENTITY_TYPES_CSV = HERE / "entity_types.csv"
ENTITY_STATUSES_CSV = HERE / "entity_statuses.csv"

__all__ = [
    "ENTITY_STATUSES_CSV",
    "ENTITY_TYPES_CSV",
    "Batch",
    "Client",
    "Order",
    "Patient",
    "Result",
    "Sample",
    "seed_taxonomy",
    "sync_taxonomy",
]


# --------------------------------------------------------------------------
# Typed accessors for this lab's categories.
#
# These are optional conveniences, not schema: `category` is the polymorphic
# discriminator, so a category with no subclass loads as a plain Entity. They
# live here rather than in entitymodel.models because Patient and Sample are
# lab vocabulary, not part of the entity-event-task model -- only Entity and
# Task are. A deployment with different entities declares its own here and
# keeps the package untouched.
#
# Each polymorphic_identity must equal the category in entity_types.csv
# exactly, or rows load as bare Entity instead. Importing this module is what
# registers them: SQLAlchemy can only map a discriminator value to a class it
# has seen.
# --------------------------------------------------------------------------
class Client(Entity):
    """The ordering institution or practice that submits lab orders -- the
    lab's customer, distinct from the Patient a specimen came from.

    A root entity like Patient: no provenance parent, and its link to an Order
    is an entity_relationship edge (Client --places--> Order), not a column.
    The institution's name lives in Entity.name, which is unique per
    (category, subcategory), so two clients can't share a name.
    """

    __mapper_args__ = {"polymorphic_identity": "Client"}

    @property
    def account_number(self) -> str | None:
        return self.attributes.get("account_number")

    @property
    def billing_contact_email(self) -> str | None:
        return self.attributes.get("billing_contact_email")


class Patient(Entity):
    __mapper_args__ = {"polymorphic_identity": "Patient"}

    @property
    def mrn(self) -> str | None:
        return self.attributes.get("mrn")


class Order(Entity):
    __mapper_args__ = {"polymorphic_identity": "Order"}

    @property
    def test_panel(self) -> str | None:
        return self.attributes.get("test_panel")


class Sample(Entity):
    """Covers both raw_specimen and library_sample subcategories."""

    __mapper_args__ = {"polymorphic_identity": "Sample"}

    @property
    def specimen_type(self) -> str | None:
        return self.attributes.get("specimen_type")


class Batch(Entity):
    __mapper_args__ = {"polymorphic_identity": "Batch"}

    @property
    def flow_cell_id(self) -> str | None:
        return self.attributes.get("flow_cell_id")


class Result(Entity):
    __mapper_args__ = {"polymorphic_identity": "Result"}

    @property
    def pipeline_version(self) -> str | None:
        return self.attributes.get("pipeline_version")


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
