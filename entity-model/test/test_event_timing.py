"""
Tests for EventRecord's timing columns: occurred_at (business time, supplied
by the producer), recorded_at (system time, supplied by the database) and
published_at (set by the outbox relay).

    python test/test_event_timing.py
"""

from __future__ import annotations

import sys
import traceback
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from testdata import (  # noqa: E402
    TEST_SCHEMA,
    drop_schema,
    fresh_session,
    make_engine,
    make_event,
    make_task,
)

from demo import fire_event  # noqa: E402
from models import EventRecord  # noqa: E402


def test_occurred_at_is_required(session: Session) -> None:
    """No server default: omitting business time must fail loudly, not
    silently record transaction start time."""
    task = make_task(session)
    session.add(
        EventRecord(
            event_type="AnalysisTaskSucceeded",
            entity_type=task.category,
            entity_id=task.id,
            correlation_id=uuid.uuid4(),
            source="pipeline-worker",
            actor_type="worker",
            payload={},
        )
    )
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        assert "occurred_at" in str(exc), f"expected a not-null violation on occurred_at: {exc}"
    else:
        raise AssertionError("expected an IntegrityError for a missing occurred_at")


def test_timestamps_are_timezone_aware(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)
    session.refresh(ev)

    assert ev.occurred_at.tzinfo is not None, "occurred_at came back naive"
    assert ev.recorded_at.tzinfo is not None, "recorded_at came back naive"
    assert ev.occurred_at.utcoffset() == timedelta(0), ev.occurred_at


def test_producer_occurred_at_is_preserved(session: Session) -> None:
    """Business time survives the round trip untouched, and is independent of
    when the row was written."""
    task = make_task(session)
    happened = datetime(2026, 3, 14, 15, 9, 26, tzinfo=timezone.utc)

    ev = fire_event(
        session, task,
        event_type="AnalysisTaskSucceeded",
        new_status="Succeeded",
        payload={"sequencing_sample_id": "SEQ-001", "pipeline_version": "v2.3.1"},
        source="pipeline-worker",
        actor_type="worker",
        occurred_at=happened,
    )
    session.commit()
    session.refresh(ev)

    assert ev.occurred_at == happened, f"expected {happened}, got {ev.occurred_at}"
    assert ev.recorded_at > ev.occurred_at, "recorded_at should be the later, system-side time"


def test_fire_event_defaults_occurred_at_to_now(session: Session) -> None:
    task = make_task(session)
    before = datetime.now(timezone.utc)
    ev = fire_event(
        session, task,
        event_type="AnalysisTaskSucceeded",
        new_status="Succeeded",
        payload={},
        source="pipeline-worker",
        actor_type="worker",
    )
    session.commit()
    session.refresh(ev)
    after = datetime.now(timezone.utc)

    assert before <= ev.occurred_at <= after, f"{ev.occurred_at} not within [{before}, {after}]"


def test_recorded_at_differs_within_one_transaction(session: Session) -> None:
    """
    The point of clock_timestamp() over now(): three events written in a
    single transaction must get three distinct recorded_at values, while
    transaction_timestamp() stays frozen.
    """
    task = make_task(session)

    events = []
    for i in range(3):
        events.append(make_event(session, task, seq=i, commit=False))
        session.execute(text("SELECT pg_sleep(0.01)"))

    frozen = {session.scalar(select(func.now())) for _ in range(3)}
    session.commit()

    for ev in events:
        session.refresh(ev)
    recorded = [ev.recorded_at for ev in events]

    assert len(set(recorded)) == 3, f"recorded_at collapsed within a transaction: {recorded}"
    assert recorded == sorted(recorded), f"recorded_at should increase with insert order: {recorded}"
    assert len(frozen) == 1, f"now() should be frozen for the transaction, saw {frozen}"


def test_published_at_starts_null_and_relay_sets_it(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)
    session.refresh(ev)

    assert ev.published_at is None, "published_at must be null until relayed"
    assert ev.publish_attempts == 0

    # What the outbox relay does after the broker acks. clock_timestamp() and
    # the in-place increment both stay server-side so concurrent relays can't
    # lose a count via read-modify-write.
    session.execute(
        update(EventRecord)
        .where(EventRecord.event_id == ev.event_id)
        .values(
            published_at=func.clock_timestamp(),
            publish_attempts=EventRecord.publish_attempts + 1,
        )
    )
    session.commit()
    session.refresh(ev)

    assert ev.published_at is not None, "relay should have stamped published_at"
    assert ev.published_at.tzinfo is not None
    assert ev.publish_attempts == 1
    assert ev.published_at >= ev.recorded_at


def test_publish_attempts_has_a_server_default(session: Session) -> None:
    """A relay inserting via raw SQL must not hit a not-null violation."""
    task = make_task(session)
    session.execute(
        text(
            f"""
            INSERT INTO {TEST_SCHEMA}.events
                (event_type, schema_version, entity_type, entity_id,
                 correlation_id, source, actor_type, payload, occurred_at)
            VALUES ('RawInsert', 1, :etype, :eid, gen_random_uuid(),
                    'sql', 'system', '{{}}'::jsonb, now())
            """
        ),
        {"etype": task.category, "eid": task.id},
    )
    session.commit()

    ev = session.scalars(select(EventRecord).where(EventRecord.event_type == "RawInsert")).one()
    assert ev.publish_attempts == 0, f"expected server-side default of 0, got {ev.publish_attempts}"


def test_unpublished_partial_index_exists(session: Session) -> None:
    """The relay's hot path depends on this staying a *partial* index."""
    definition = session.scalar(
        text(
            "SELECT indexdef FROM pg_indexes "
            "WHERE schemaname = :s AND indexname = 'idx_events_unpublished'"
        ),
        {"s": TEST_SCHEMA},
    )
    assert definition is not None, "idx_events_unpublished is missing"
    assert "WHERE (published_at IS NULL)" in definition, definition


# --------------------------------------------------------------------------
TESTS = [
    test_occurred_at_is_required,
    test_timestamps_are_timezone_aware,
    test_producer_occurred_at_is_preserved,
    test_fire_event_defaults_occurred_at_to_now,
    test_recorded_at_differs_within_one_transaction,
    test_published_at_starts_null_and_relay_sets_it,
    test_publish_attempts_has_a_server_default,
    test_unpublished_partial_index_exists,
]


def main(keep: bool = False) -> int:
    engine = make_engine()
    passed, failed = 0, []

    for test in TESTS:
        session = fresh_session(engine)
        try:
            test(session)
            print(f"PASS  {test.__name__}")
            passed += 1
        except Exception:
            print(f"FAIL  {test.__name__}")
            traceback.print_exc()
            failed.append(test.__name__)
        finally:
            session.close()

    if not keep:
        drop_schema(engine)
    engine.dispose()

    print(f"\n{passed} passed, {len(failed)} failed")
    for name in failed:
        print(f"  failed: {name}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(keep="--keep" in sys.argv))
