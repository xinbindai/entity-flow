"""
Worked example of the entity-event-task model: firing an event atomically
with an entity transition (the transactional outbox pattern / "Transaction
1"), and a poll-based consumer that dispatches unprocessed events to a
handler with per-handler idempotency via event_handler_checkpoints
("Transaction 2").

The reusable pieces -- fire_event, the handler registry and the dispatch
loop -- live in outbox.py. What's left here is domain-specific: one handler,
its subscription, and a scripted walkthrough.

Run it against Postgres; models.py uses JSONB, gen_random_uuid() and a
PL/pgSQL trigger that only exist there:

    pip install "sqlalchemy>=2.0" psycopg2-binary
    python demo.py postgresql+psycopg2://localhost/lab_platform_demo
"""

from __future__ import annotations

import sys
import uuid

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models import Base, EventRecord, Result, Task
from taxonomy import seed_taxonomy
from outbox import HandlerRegistry, dispatch_once, fire_event


# --------------------------------------------------------------------------
# The subscriptions this process runs. A second handler on the same event
# type would be another @registry.on below and nothing else -- no change to
# the producer, no change to this handler.
# --------------------------------------------------------------------------
registry = HandlerRegistry()


# The actual handler: on AnalysisTaskSucceeded, create the Sample Result and
# emit the event that describes it -- same "create-sample-result" handler
# discussed throughout the design conversation.
@registry.on("AnalysisTaskSucceeded", name="create-sample-result-on-analysis-succeeded")
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

        # dispatch_once is one pass over every registered handler. A real
        # worker calls listen(session_factory, registry) instead, which is
        # this in a loop; the demo steps it manually to show idempotency.
        print("\nConsumer: first poll")
        n = dispatch_once(session, registry)
        print(f"  processed {n} event(s)")

        print("\nConsumer: second poll (idempotency check)")
        n = dispatch_once(session, registry)
        print(f"  processed {n} event(s) -- already-handled event correctly skipped")

        print("\nEvent log:")
        for e in session.scalars(select(EventRecord).order_by(EventRecord.occurred_at)):
            print(f"  {e.event_type:22s} entity={e.entity_type:8s} causation_id={e.causation_id}")


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "postgresql+psycopg2://localhost/lab_platform_demo"
    main(url)
