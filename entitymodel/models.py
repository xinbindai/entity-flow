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

2. Task is just another polymorphic subclass of Entity, the same way it's
   just another `category` row in entity_type -- confirming in code what
   was established in discussion: a task is an entity.

This module is domain-free: the schema, the DDL hook for the status trigger,
and Task, which is part of the entity-event-task model itself. A deployment's
own categories and the taxonomy naming them live in taxonomy.py, so a
different deployment keeps this file verbatim and replaces that one.

For ongoing schema changes, pair this with Alembic rather than relying on
create_all() in production -- create_all()/the DDL hooks below are meant
for bootstrapping a dev/test database in one call (see taxonomy.py's
__main__ for that).
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
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
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
    # timestamptz, matching entity_schema_unified.sql, and clock_timestamp()
    # rather than now(): now() is transaction start time, so a batch of
    # entities created in one transaction would all share a created_at, and
    # an entity updated twice in one transaction would show no change at all.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )
    # Written by the validate_entity_status trigger on every INSERT and
    # UPDATE, so the application can't set it or forget to. The server_default
    # only ever applies if that trigger is missing.
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )

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
        -- clock_timestamp(), not now(): now() is transaction start time, so
        -- two updates to the same row in one transaction would leave
        -- updated_at unchanged, and anything polling "changed since X" would
        -- miss the second one.
        NEW.updated_at := clock_timestamp();
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
# Task: the third of the model's three concepts, and a polymorphic subclass of
# Entity like any other -- which is the point. A task's execution status is a
# state machine like any entity's, it is created in reaction to an event like
# any entity mutation, and it emits its own events on transition. The system
# has two primitives, not three.
#
# Task lives here because it belongs to the model. The lab's own categories --
# Patient, Sample, Batch and so on -- are domain vocabulary and live beside
# their taxonomy in taxonomy.py, so a deployment with different entities keeps
# this package unchanged and declares its own.
#
# Subclasses are optional: `category` is the discriminator, so a category with
# no subclass simply loads as a plain Entity. Declare one when you want typed
# accessors into `attributes`, and make its polymorphic_identity match the
# category exactly.
# --------------------------------------------------------------------------
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
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )

    parent: Mapped[Entity] = relationship(foreign_keys=[parent_entity_id])
    child: Mapped[Entity] = relationship(foreign_keys=[child_entity_id])


# --------------------------------------------------------------------------
# Event log / outbox (see events_schema.sql for full design rationale).
# entity_id is now a real FK, made possible by the unified entity table.
# causation_id stays a plain UUID (no FK): it may point at either another
# event or a task, so it's paired with causation_type instead of one FK.
# --------------------------------------------------------------------------
# The channel an event lands in when its producer names none, and the one a
# subscription consumes when it names none. Both defaults are this value on
# purpose: it is what makes adding the column inert until somebody uses it.
DEFAULT_CHANNEL = "default"


class EventRecord(Base):
    __tablename__ = "events"
    __table_args__ = (
        Index("idx_events_correlation_id", "correlation_id"),
        Index("idx_events_entity", "entity_type", "entity_id"),
        Index("idx_events_causation", "causation_type", "causation_id"),
        Index("idx_events_type_occurred", "event_type", "occurred_at"),
        # The channel-scoped consumer's selection (section 3.7). Kept
        # alongside idx_events_type_occurred rather than replacing it: a
        # handler subscribed to every channel issues the pre-channel query
        # with no channel predicate at all, and that index is what keeps it
        # off a sequential scan.
        Index("idx_events_channel_type_occurred", "channel", "event_type", "occurred_at"),
        # Partial: most events have no inbound request behind them -- cron
        # jobs, and every event a handler emits without forwarding one. This
        # covers "everything that request caused", which is the only question
        # trace_id is for.
        Index(
            "idx_events_trace",
            "trace_id",
            postgresql_where=text("trace_id IS NOT NULL"),
        ),
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

    # Where this event was published (section 3.7). Orthogonal to event_type:
    # one type can be fired into several channels, one channel carries many
    # types.
    #
    # NOT NULL with a default, never nullable: a nullable routing key puts
    # three-valued logic into the consumer's selection, and `channel = NULL`
    # is never true, so the failure would be silent under-delivery rather than
    # an error. server_default as well as default, for the same reason
    # publish_attempts has one -- a relay or fixture inserting via raw SQL
    # would otherwise hit a not-null violation.
    channel: Mapped[str] = mapped_column(
        String,
        nullable=False,
        default=DEFAULT_CHANNEL,
        # Quoted: text() emits its argument raw, so the bare word would render
        # as `DEFAULT default` -- a reserved word where a literal belongs.
        server_default=text(f"'{DEFAULT_CHANNEL}'"),
    )

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
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )
    # NULL when the handler did the work; set when it declined the event by
    # raising HandlerCancelled. Both outcomes settle the event and stop it
    # being offered again, so without this the two are indistinguishable
    # afterwards -- and "why did nothing happen for this order" is exactly the
    # question someone asks months later.
    cancelled_reason: Mapped[str | None] = mapped_column(Text)


class EventHandlerFailure(Base):
    """Per-handler retry bookkeeping -- the counterpart to
    EventHandlerCheckpoint. A checkpoint row means "this handler processed
    this event successfully"; a failure row means "it tried and raised".

    Deliberately a separate table rather than a status column on the
    checkpoint ledger: "a checkpoint exists" has to keep meaning exactly one
    thing, since both poll_and_dispatch's anti-join and replay() rest on it.

    The row is deleted once the handler eventually succeeds, so `attempts`
    always counts *consecutive* failures. An event whose attempts reach the
    caller's max_attempts is dead-lettered: poll_and_dispatch stops selecting
    it, and it takes an explicit replay() to try again.
    """

    __tablename__ = "event_handler_failures"

    handler_name: Mapped[str] = mapped_column(String, primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("events.event_id"), primary_key=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    last_error: Mapped[str | None] = mapped_column(Text)
    first_failed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )
    last_failed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )
    # Earliest time this handler should be offered the event again. Without
    # it a failing event is retried on every poll, so a 1s poll loop would
    # burn the whole attempt budget in seconds and record five near-identical
    # errors. Computed server-side at failure time as an exponential backoff
    # from `attempts`, so it never depends on a worker's clock.
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp(), index=True
    )
