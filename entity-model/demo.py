"""
Demo: firing an event atomically with an entity transition (the
transactional outbox pattern / "Transaction 1"), and a poll-based consumer
that dispatches unprocessed events to a handler with per-handler
idempotency via event_handler_checkpoints ("Transaction 2").

Verified logic (fire_event / poll_and_dispatch / checkpoint idempotency /
causation_id chaining) against an in-memory database before shipping this
file; run it for real against Postgres, since models.py uses JSONB,
gen_random_uuid(), and a PL/pgSQL trigger that only exist there:

    pip install "sqlalchemy>=2.0" psycopg2-binary
    python demo.py postgresql+psycopg2://localhost/lab_platform_demo
"""

from __future__ import annotations

import sys
import uuid
from collections.abc import Callable
from datetime import datetime, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models import Base, EventHandlerCheckpoint, EventRecord, Result, Task, seed_taxonomy


# --------------------------------------------------------------------------
# Producer side: entity transition + its own event, one transaction --
# "Transaction 1" from the design discussion. Doesn't commit itself, so it
# composes with whatever else the caller needs in the same transaction.
# --------------------------------------------------------------------------
def fire_event(
    session: Session,
    entity,
    *,
    event_type: str,
    new_status: str,
    payload: dict,
    source: str,
    actor_type: str,
    actor_id: str | None = None,
    causation_type: str | None = None,
    causation_id: uuid.UUID | None = None,
    occurred_at: datetime | None = None,
) -> EventRecord:
    """
    occurred_at is business time -- when the fact actually happened. Pass it
    whenever the producer knows it (a pipeline that finished at 10:00:00 but
    commits at 10:00:04 must record 10:00:00). The fallback is the moment this
    event was constructed, which is still far closer than a server-side now(),
    since now() is transaction start time.

    recorded_at is left to the database (clock_timestamp()). Note that even
    that is insert time, not commit time: a transaction can insert at T and
    commit at T+30s, and rows only become visible at commit. So don't use
    either timestamp as a consumption cursor -- drain on published_at IS NULL
    with FOR UPDATE SKIP LOCKED instead.
    """
    entity.status = new_status
    ev = EventRecord(
        event_type=event_type,
        entity_type=entity.category,
        entity_id=entity.id,
        correlation_id=entity.correlation_id or uuid.uuid4(),
        causation_type=causation_type,
        causation_id=causation_id,
        source=source,
        actor_type=actor_type,
        actor_id=actor_id,
        payload=payload,
        occurred_at=occurred_at or datetime.now(timezone.utc),
    )
    session.add(ev)
    return ev


# --------------------------------------------------------------------------
# Consumer side: poll for events this handler hasn't processed yet (no
# checkpoint row for this handler), dispatch each one, commit the handler's
# writes together with its own checkpoint row.
#
# NOT EXISTS rather than NOT IN: Postgres won't rewrite `NOT IN (subquery)`
# as an anti-join -- NULL semantics block it even though event_id is NOT
# NULL -- so it hashes every checkpoint row for this handler on each poll,
# and degrades to a per-row rescan once that exceeds work_mem. Correlated
# NOT EXISTS plans as a real anti-join against the (handler_name, event_id)
# primary key instead.
#
# The batch_size limit caps how much one poll pulls into memory. Note that
# this still scans all history for these event types to find the new rows;
# bounding that needs a watermark or a claim-based drain (see the
# published_at/publish_attempts columns on EventRecord).
# --------------------------------------------------------------------------
def poll_and_dispatch(
    session: Session,
    *,
    handler_name: str,
    event_types: list[str],
    handle: Callable[[Session, EventRecord], None],
    batch_size: int = 100,
) -> int:
    already_processed = (
        select(EventHandlerCheckpoint.event_id)
        .where(
            EventHandlerCheckpoint.handler_name == handler_name,
            EventHandlerCheckpoint.event_id == EventRecord.event_id,
        )
        .exists()
    )
    pending = session.scalars(
        select(EventRecord)
        .where(EventRecord.event_type.in_(event_types))
        .where(~already_processed)
        .order_by(EventRecord.occurred_at)
        .limit(batch_size)
    ).all()

    for ev in pending:
        handle(session, ev)
        session.add(EventHandlerCheckpoint(handler_name=handler_name, event_id=ev.event_id))
        session.commit()  # "Transaction 2": handler's entity writes + its checkpoint, atomically

    return len(pending)


# --------------------------------------------------------------------------
# The actual handler: on AnalysisTaskSucceeded, create the Sample Result
# and emit the event that describes it -- same "create-sample-result"
# handler discussed throughout the design conversation.
# --------------------------------------------------------------------------
def create_sample_result_on_analysis_succeeded(session: Session, ev: EventRecord) -> None:
    result = Result(
        category="Result",
        subcategory="sample_result",
        name=f"result-{ev.payload['sequencing_sample_id']}",
        status="Complete",
        correlation_id=ev.correlation_id,
        attributes={
            "pipeline_version": ev.payload["pipeline_version"],
            "reference_genome": ev.payload.get("reference_genome", "GRCh38"),
        },
    )
    session.add(result)
    session.flush()  # need result.id (server-generated) before referencing it below

    fire_event(
        session,
        result,
        event_type="SampleResultCreated",
        new_status="Complete",  # no-op transition -- already Complete, kept for symmetry with fire_event's contract
        payload={"sequencing_sample_id": ev.payload["sequencing_sample_id"]},
        source="sample-result-service",
        actor_type="system",
        causation_type="event",
        causation_id=ev.event_id,
    )
    print(f"  -> created Result {result.name!r} (id={result.id}), emitted SampleResultCreated")


def main(db_url: str) -> None:
    engine = create_engine(db_url, echo=False)
    Base.metadata.drop_all(engine)  # demo convenience -- never do this against real data
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        seed_taxonomy(session)

        # Minimal setup: one task, already Running, standing in for a
        # pipeline job that's about to finish.
        task = Task(
            category="Task",
            subcategory="bioinformatics_pipeline_analysis",
            name="pipeline-run-demo-1",
            status="Running",
            correlation_id=uuid.uuid4(),
            attributes={"retry_count": 0},
        )
        session.add(task)
        session.commit()

        print("Producer: task succeeds, firing AnalysisTaskSucceeded")
        fire_event(
            session,
            task,
            event_type="AnalysisTaskSucceeded",
            new_status="Succeeded",
            payload={
                "sequencing_sample_id": "SEQ-001",
                "pipeline_version": "v2.3.1",
                "reference_genome": "GRCh38",
            },
            source="pipeline-worker",
            actor_type="worker",
        )
        session.commit()  # Transaction 1: task.status + AnalysisTaskSucceeded event, together

        print("\nConsumer: first poll")
        n = poll_and_dispatch(
            session,
            handler_name="create-sample-result-on-analysis-succeeded",
            event_types=["AnalysisTaskSucceeded"],
            handle=create_sample_result_on_analysis_succeeded,
        )
        print(f"  processed {n} event(s)")

        print("\nConsumer: second poll (idempotency check)")
        n = poll_and_dispatch(
            session,
            handler_name="create-sample-result-on-analysis-succeeded",
            event_types=["AnalysisTaskSucceeded"],
            handle=create_sample_result_on_analysis_succeeded,
        )
        print(f"  processed {n} event(s) -- already-handled event correctly skipped")

        print("\nEvent log:")
        for e in session.scalars(select(EventRecord).order_by(EventRecord.occurred_at)):
            print(f"  {e.event_type:22s} entity={e.entity_type:8s} causation_id={e.causation_id}")


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "postgresql+psycopg2://localhost/lab_platform_demo"
    main(url)
