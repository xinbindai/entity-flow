"""
SQLAlchemy 2.0 ORM models for the unified entity-event-task schema
(companion to entity_schema_unified.sql and events_schema.sql).

The unified `entity` table is the chosen schema shape (see §2.4 of
entity-event-task-architecture.md for the rejected per-entity-table
alternative and why). Two things it makes possible that the alternative
could not:

1. events.entity_id can now be a real, enforced foreign key to entity.id --
   since every entity type lives in one table, "which entity is this event
   about" is no longer a loose (entity_type, entity_id) pair with nothing
   backing it; it's a genuine FK with referential integrity.

2. Task is just another polymorphic subclass of Entity (see the
   Patient/Order/Sample/Batch/Result/Task classes below), the same way it's
   just another `category` row in entity_type -- confirming in code what
   was established in discussion: a task is an entity.

For ongoing schema changes, pair this with Alembic rather than relying on
create_all() in production -- create_all()/the DDL hooks below are meant
for bootstrapping a dev/test database in one call.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    event,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship
from sqlalchemy.schema import DDL


class Base(DeclarativeBase):
    pass


# --------------------------------------------------------------------------
# 1. Taxonomy: valid (category, subcategory) pairs.
# --------------------------------------------------------------------------
class EntityType(Base):
    __tablename__ = "entity_type"

    category: Mapped[str] = mapped_column(String, primary_key=True)
    subcategory: Mapped[str] = mapped_column(String, primary_key=True)
    description: Mapped[str | None] = mapped_column(Text)


# --------------------------------------------------------------------------
# 2. Valid statuses per subcategory -- each entity subcategory's own state
#    machine, enforced in the database by the trigger wired up below.
# --------------------------------------------------------------------------
class EntityStatus(Base):
    __tablename__ = "entity_status"
    __table_args__ = (
        ForeignKeyConstraint(
            ["category", "subcategory"],
            ["entity_type.category", "entity_type.subcategory"],
        ),
    )

    category: Mapped[str] = mapped_column(String, primary_key=True)
    subcategory: Mapped[str] = mapped_column(String, primary_key=True)
    status: Mapped[str] = mapped_column(String, primary_key=True)
    is_terminal: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


# --------------------------------------------------------------------------
# 3. The unified entity table.
# --------------------------------------------------------------------------
class Entity(Base):
    __tablename__ = "entity"
    __table_args__ = (
        ForeignKeyConstraint(
            ["category", "subcategory"],
            ["entity_type.category", "entity_type.subcategory"],
        ),
        UniqueConstraint("category", "subcategory", "name", name="uq_entity_name_per_subcategory"),
        Index("idx_entity_attributes_gin", "attributes", postgresql_using="gin"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    category: Mapped[str] = mapped_column(String, nullable=False)
    subcategory: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    correlation_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), index=True)
    attributes: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now())

    __mapper_args__ = {
        "polymorphic_on": category,
        "polymorphic_identity": "Entity",
    }

    def __repr__(self) -> str:
        return f"<{self.category}/{self.subcategory} {self.name!r} status={self.status!r}>"


# The status-validation trigger is Postgres-specific PL/pgSQL, not something
# the ORM's declarative model syntax can express -- wire it up as raw DDL
# that fires right after the `entity` table is created, so create_all()
# still produces a complete schema in one call.
_validate_status_function = DDL(
    """
    CREATE OR REPLACE FUNCTION validate_entity_status() RETURNS TRIGGER AS $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM entity_status
            WHERE category = NEW.category AND subcategory = NEW.subcategory AND status = NEW.status
        ) THEN
            -- Doubled percent signs below are escapes, not typos: SQLAlchemy's
            -- DDL construct runs printf-style interpolation over this string,
            -- so a literal percent sign must be doubled or create_all() raises
            -- TypeError before the statement ever reaches the server.
            RAISE EXCEPTION 'invalid status %% for %%/%%', NEW.status, NEW.category, NEW.subcategory;
        END IF;
        NEW.updated_at := now();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """
)
_validate_status_trigger = DDL(
    """
    CREATE TRIGGER trg_validate_entity_status
    BEFORE INSERT OR UPDATE ON entity
    FOR EACH ROW EXECUTE FUNCTION validate_entity_status();
    """
)
event.listen(Entity.__table__, "after_create", _validate_status_function)
event.listen(Entity.__table__, "after_create", _validate_status_trigger)


# --------------------------------------------------------------------------
# Polymorphic convenience subclasses -- same table, same rows, just typed
# Python-side accessors into `attributes` for each category. Optional: you
# can ignore these entirely and work with plain Entity rows instead.
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


class Task(Entity):
    """A task is just another entity -- see the class docstring at the top."""

    __mapper_args__ = {"polymorphic_identity": "Task"}

    @property
    def triggering_event_id(self) -> str | None:
        # Stored in attributes, not a real column -- not FK-enforced by the
        # database (see the trade-offs note in entity_schema_unified.sql).
        return self.attributes.get("triggering_event_id")

    @property
    def retry_count(self) -> int:
        return self.attributes.get("retry_count", 0)


# --------------------------------------------------------------------------
# 4. The relationship graph -- all lineage, in both directions, including
#    what used to be plain FKs (sample.order_id) and Task's target/produced
#    edges.
# --------------------------------------------------------------------------
class EntityRelationship(Base):
    __tablename__ = "entity_relationship"
    __table_args__ = (
        UniqueConstraint("parent_entity_id", "child_entity_id", "relationship_type"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    parent_entity_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("entity.id"), index=True)
    child_entity_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("entity.id"), index=True)
    relationship_type: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    parent: Mapped[Entity] = relationship(foreign_keys=[parent_entity_id])
    child: Mapped[Entity] = relationship(foreign_keys=[child_entity_id])


# --------------------------------------------------------------------------
# Event log / outbox (see events_schema.sql for full design rationale).
# entity_id is now a real FK, made possible by the unified entity table.
# causation_id stays a plain UUID (no FK): it may point at either another
# event or a task, so it's paired with causation_type instead of one FK.
# --------------------------------------------------------------------------
class EventRecord(Base):
    __tablename__ = "events"
    __table_args__ = (
        Index("idx_events_correlation_id", "correlation_id"),
        Index("idx_events_entity", "entity_type", "entity_id"),
        Index("idx_events_causation", "causation_type", "causation_id"),
        Index("idx_events_type_occurred", "event_type", "occurred_at"),
        # Partial index, present in events_schema.sql but previously missing
        # here: it covers only unpublished rows, so the relay's hot path stays
        # small no matter how large the events table grows.
        Index(
            "idx_events_unpublished",
            "published_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    entity_type: Mapped[str] = mapped_column(String, nullable=False)
    entity_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("entity.id"), nullable=False)

    correlation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    causation_type: Mapped[str | None] = mapped_column(String)  # 'event' | 'task' | null (root event)
    causation_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    trace_id: Mapped[str | None] = mapped_column(String)

    source: Mapped[str] = mapped_column(String, nullable=False)
    producer_version: Mapped[str | None] = mapped_column(String)
    actor_type: Mapped[str] = mapped_column(String, nullable=False)  # 'system' | 'user' | 'worker'
    actor_id: Mapped[str | None] = mapped_column(String)

    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # Timing. All three are timestamptz (matching events_schema.sql): Postgres
    # stores a UTC instant and converts on read, so the value survives DST and
    # multi-region deploys. A plain `timestamp` discards the offset with no way
    # to recover what it meant. Pass aware datetimes from Python --
    # datetime.now(timezone.utc), never the naive datetime.utcnow().
    #
    # occurred_at is business time: when the fact actually happened. It has no
    # server default on purpose -- only the producer knows it, and now() would
    # be wrong twice over (it's transaction *start* time, so it both lags the
    # real event and collapses every event in one transaction to one value).
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    # recorded_at is system time: when this row was written. Left to the
    # database so it never depends on a producer's clock. clock_timestamp()
    # rather than now() so events written in one transaction get distinct
    # values -- see the caveat in fire_event's docstring about commit time.
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )
    # Set by the outbox relay once the broker acks -- never at insert time.
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # server_default as well as default: `default=0` is client-side only, so a
    # relay inserting via raw SQL would hit a not-null violation without it.
    publish_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )

    entity: Mapped[Entity] = relationship()


class EventHandlerCheckpoint(Base):
    """Per-handler idempotency ledger -- only needed for handlers whose
    side effect isn't naturally idempotent (see the replay/idempotency
    discussion). Composite PK is what lets N handlers each track their own
    processing of the same event independently."""

    __tablename__ = "event_handler_checkpoints"

    handler_name: Mapped[str] = mapped_column(String, primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("events.event_id"), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(server_default=func.now())


# --------------------------------------------------------------------------
# Taxonomy seed data -- mirrors the INSERT statements in
# entity_schema_unified.sql. Call once against a fresh database.
# --------------------------------------------------------------------------
def seed_taxonomy(session: Session) -> None:
    types = [
        EntityType(category="Client", subcategory="ordering_institution", description="Ordering institution or practice that submits lab orders"),
        EntityType(category="Patient", subcategory="patient", description="Root clinical identity"),
        EntityType(category="Order", subcategory="lab_order", description="Test requisition"),
        EntityType(category="Sample", subcategory="raw_specimen", description="Physical specimen as collected"),
        EntityType(category="Sample", subcategory="library_sample", description="Prepped library/aliquot ready for sequencing"),
        EntityType(category="Batch", subcategory="illumina_run", description="A sequencing run pooling many library samples"),
        EntityType(category="Result", subcategory="sample_result", description="Pipeline output for one or more library samples"),
        EntityType(category="Task", subcategory="bioinformatics_pipeline_analysis", description="Async pipeline execution task"),
        EntityType(category="Task", subcategory="data_archiving", description="Async data archival task"),
    ]

    # Every subcategory in `types` needs at least one row here: the
    # validate_entity_status trigger rejects any status without a matching
    # (category, subcategory, status) row, so a subcategory with none is
    # impossible to insert at all.
    statuses = [
        ("Client", "ordering_institution", "Onboarding", False),
        ("Client", "ordering_institution", "Active", False),
        ("Client", "ordering_institution", "Suspended", False),
        ("Client", "ordering_institution", "Offboarded", True),
        ("Patient", "patient", "Active", False), ("Patient", "patient", "Inactive", False),
        ("Patient", "patient", "Deceased", True), ("Patient", "patient", "Merged", True),
        ("Order", "lab_order", "Placed", False), ("Order", "lab_order", "InProgress", False),
        ("Order", "lab_order", "Completed", True), ("Order", "lab_order", "Cancelled", True),
        ("Sample", "raw_specimen", "Received", False), ("Sample", "raw_specimen", "Accessioned", False),
        ("Sample", "raw_specimen", "QC_Passed", False), ("Sample", "raw_specimen", "Rejected", True),
        ("Sample", "raw_specimen", "Consumed", True),
        ("Sample", "library_sample", "Prepped", False), ("Sample", "library_sample", "Loaded", False),
        ("Sample", "library_sample", "Sequencing", False), ("Sample", "library_sample", "Sequenced", True),
        ("Sample", "library_sample", "Failed", True),
        ("Batch", "illumina_run", "Planned", False), ("Batch", "illumina_run", "Loading", False),
        ("Batch", "illumina_run", "Running", False), ("Batch", "illumina_run", "Complete", True),
        ("Batch", "illumina_run", "Failed", True),
        ("Result", "sample_result", "Pending", False), ("Result", "sample_result", "Processing", False),
        ("Result", "sample_result", "Complete", False), ("Result", "sample_result", "Reviewed", False),
        ("Result", "sample_result", "Released", False), ("Result", "sample_result", "Archived", True),
        ("Result", "sample_result", "Failed", True),
        ("Task", "bioinformatics_pipeline_analysis", "Queued", False),
        ("Task", "bioinformatics_pipeline_analysis", "Running", False),
        ("Task", "bioinformatics_pipeline_analysis", "Succeeded", True),
        ("Task", "bioinformatics_pipeline_analysis", "Failed", True),
        ("Task", "bioinformatics_pipeline_analysis", "Retrying", False),
        ("Task", "bioinformatics_pipeline_analysis", "Cancelled", True),
        ("Task", "data_archiving", "Queued", False), ("Task", "data_archiving", "Running", False),
        ("Task", "data_archiving", "Succeeded", True), ("Task", "data_archiving", "Failed", True),
        ("Task", "data_archiving", "Retrying", False), ("Task", "data_archiving", "Cancelled", True),
    ]

    session.add_all(types)
    # Flush the parent rows before adding the children: the entity_type ->
    # entity_status dependency is a table-level ForeignKeyConstraint with no
    # ORM relationship() behind it, so the unit of work has nothing to sort
    # these two INSERT batches by and may emit entity_status first.
    session.flush()
    session.add_all(
        EntityStatus(category=c, subcategory=s, status=st, is_terminal=t) for c, s, st, t in statuses
    )
    session.commit()


if __name__ == "__main__":
    # Minimal bootstrap example for a dev/test database:
    #   pip install "sqlalchemy>=2.0" psycopg2-binary
    from sqlalchemy import create_engine

    engine = create_engine("postgresql+psycopg2://localhost/lab_platform")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        seed_taxonomy(session)
