"""
Tests for trace_id -- the handle for the inbound call that set a chain off.

It is the one field here that is not a domain fact: an API request id or a W3C
traceparent, useful only for lining events up against the request's logs, and
worthless once those logs rotate. The tests are about it reaching every event
one request causes, including across the process boundary into a worker.

    python test/test_trace_id.py
"""

from __future__ import annotations

import sys
import traceback
import uuid

from celery import Celery
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from testdata import (  # noqa: E402
    drop_schema,
    fresh_session,
    make_engine,
    make_event,
    make_task,
)

from entitymodel.models import EventRecord, Task  # noqa: E402
from entitymodel.outbox import HandlerRegistry, dispatch_once, fire_event  # noqa: E402
from entitymodel.celery_tasks import entity_task, submit_task  # noqa: E402

REQUEST_ID = "req-7f3a91c4"


class FakeCelery:
    def send_task(self, name, kwargs=None, queue=None, **_):
        class Result:
            id = "celery-1"
        return Result()


def traces(session: Session) -> dict[str, str | None]:
    return {
        e.event_type: e.trace_id
        for e in session.scalars(select(EventRecord).order_by(EventRecord.recorded_at))
    }


# --------------------------------------------------------------------------
def test_fire_event_records_it(session: Session) -> None:
    task = make_task(session)
    ev = fire_event(session, task, event_type="OrderPlaced", new_status="Succeeded",
                    payload={}, source="order-api", actor_type="user", actor_id="dr.smith",
                    trace_id=REQUEST_ID)
    session.commit()

    assert ev.trace_id == REQUEST_ID


def test_it_is_optional(session: Session) -> None:
    """Not every event comes from a request -- a cron job has no trace."""
    task = make_task(session)
    ev = fire_event(session, task, event_type="NightlySweep", new_status="Succeeded",
                    payload={}, source="cron", actor_type="system")
    session.commit()

    assert ev.trace_id is None


def test_it_is_separate_from_actor_and_causation(session: Session) -> None:
    """
    The whole point of the answer: a request id is neither who acted nor what
    caused this. Those columns must be untouched by it.
    """
    task = make_task(session)
    ev = fire_event(session, task, event_type="OrderPlaced", new_status="Succeeded",
                    payload={}, source="order-api", actor_type="user", actor_id="dr.smith",
                    trace_id=REQUEST_ID)
    session.commit()

    assert ev.actor_id == "dr.smith", "the person, not the invocation"
    assert ev.causation_type is None and ev.causation_id is None, "an API call is a root"
    assert ev.trace_id == REQUEST_ID


def test_a_handler_forwards_it(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, event_type="SequencingReady", seq=0, trace_id=REQUEST_ID)

    registry = HandlerRegistry()

    @registry.on("SequencingReady", name="downstream")
    def downstream(sess: Session, ev: EventRecord) -> None:
        fire_event(sess, task, event_type="Downstream", new_status="Succeeded",
                   payload={}, source="svc", actor_type="worker",
                   causation_type="event", causation_id=ev.event_id,
                   trace_id=ev.trace_id)

    assert dispatch_once(session, registry) == 1
    assert traces(session) == {"SequencingReady": REQUEST_ID, "Downstream": REQUEST_ID}


def test_it_is_not_inherited_without_being_passed(session: Session) -> None:
    """Deliberate: fire_event holds only the causing event's id, and will not
    query for it behind the caller's back to copy a debugging field."""
    task = make_task(session)
    first = fire_event(session, task, event_type="First", new_status="Succeeded",
                       payload={}, source="api", actor_type="user", trace_id=REQUEST_ID)
    session.commit()

    second = fire_event(session, task, event_type="Second", new_status="Succeeded",
                        payload={}, source="svc", actor_type="worker",
                        causation_type="event", causation_id=first.event_id)
    session.commit()

    assert second.trace_id is None


def test_submit_task_records_it_on_the_event_and_the_task(session: Session) -> None:
    task = submit_task(session, FakeCelery(), celery_task_name="t.run",
                       subcategory="bioinformatics_pipeline_analysis",
                       name="run-1", payload={"sample": "S1"}, trace_id=REQUEST_ID)

    assert traces(session)["TaskQueued"] == REQUEST_ID
    assert task.attributes["trace_id"] == REQUEST_ID, \
        "the worker needs it without a lookup"


def test_it_survives_into_the_worker(session: Session) -> None:
    """
    The case that matters: the worker is a different process, running minutes
    later, and its events must still point at the original request.
    """
    app = Celery("t")
    app.conf.update(task_always_eager=True, task_eager_propagates=False, broker_url="memory://")
    factory = sessionmaker(session.get_bind())

    @entity_task(app, factory, name="t.run")
    def run(sess, task, payload):
        return {"done": True}

    task = submit_task(session, FakeCelery(), celery_task_name="t.run",
                       subcategory="bioinformatics_pipeline_analysis",
                       name="run-1", payload={}, trace_id=REQUEST_ID)
    run.apply(kwargs={"entity_task_id": str(task.id)})

    session.expire_all()
    seen = traces(session)
    assert seen == {"TaskQueued": REQUEST_ID, "TaskStarted": REQUEST_ID,
                    "TaskSucceeded": REQUEST_ID}, seen


def test_a_failing_task_still_carries_it(session: Session) -> None:
    app = Celery("t")
    app.conf.update(task_always_eager=True, task_eager_propagates=False, broker_url="memory://")
    factory = sessionmaker(session.get_bind())

    @entity_task(app, factory, name="t.boom")
    def boom(sess, task, payload):
        raise RuntimeError("disk on fire")

    task = submit_task(session, FakeCelery(), celery_task_name="t.boom",
                       subcategory="bioinformatics_pipeline_analysis",
                       name="boom-1", payload={}, trace_id=REQUEST_ID)
    boom.apply(kwargs={"entity_task_id": str(task.id)}, throw=False)

    session.expire_all()
    seen = traces(session)
    assert seen.get("TaskFailed") == REQUEST_ID, seen


def test_a_task_submitted_without_one_stays_null(session: Session) -> None:
    app = Celery("t")
    app.conf.update(task_always_eager=True, task_eager_propagates=False, broker_url="memory://")
    factory = sessionmaker(session.get_bind())

    @entity_task(app, factory, name="t.quiet")
    def quiet(sess, task, payload):
        return {}

    task = submit_task(session, FakeCelery(), celery_task_name="t.quiet",
                       subcategory="bioinformatics_pipeline_analysis",
                       name="quiet-1", payload={})
    quiet.apply(kwargs={"entity_task_id": str(task.id)})

    session.expire_all()
    assert set(traces(session).values()) == {None}


def test_everything_one_request_caused_is_one_query(session: Session) -> None:
    task = make_task(session)
    for i in range(3):
        fire_event(session, task, event_type=f"E{i}", new_status="Succeeded", payload={},
                   source="api", actor_type="user", trace_id=REQUEST_ID)
    fire_event(session, task, event_type="Unrelated", new_status="Succeeded", payload={},
               source="api", actor_type="user", trace_id="req-other")
    session.commit()

    mine = session.scalars(
        select(EventRecord).where(EventRecord.trace_id == REQUEST_ID)).all()
    assert sorted(e.event_type for e in mine) == ["E0", "E1", "E2"], [e.event_type for e in mine]


def test_the_partial_index_exists_and_is_scoped(session: Session) -> None:
    """
    Partial on purpose: most events have no inbound request behind them, so
    indexing every row would be mostly NULLs. The WHERE clause is load-bearing
    and a future migration must not quietly drop it.
    """
    from sqlalchemy import text

    from testdata import TEST_SCHEMA

    definition = session.scalar(
        text("SELECT indexdef FROM pg_indexes "
             "WHERE schemaname = :s AND indexname = 'idx_events_trace'"),
        {"s": TEST_SCHEMA},
    )
    assert definition is not None, "idx_events_trace is missing"
    assert "WHERE (trace_id IS NOT NULL)" in definition, definition


# --------------------------------------------------------------------------
TESTS = [
    test_fire_event_records_it,
    test_it_is_optional,
    test_it_is_separate_from_actor_and_causation,
    test_a_handler_forwards_it,
    test_it_is_not_inherited_without_being_passed,
    test_submit_task_records_it_on_the_event_and_the_task,
    test_it_survives_into_the_worker,
    test_a_failing_task_still_carries_it,
    test_a_task_submitted_without_one_stays_null,
    test_everything_one_request_caused_is_one_query,
    test_the_partial_index_exists_and_is_scoped,
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
