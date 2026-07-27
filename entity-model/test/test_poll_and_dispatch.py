"""
Tests for demo.poll_and_dispatch against a real Postgres (JSONB, the
PL/pgSQL status trigger and gen_random_uuid() rule out SQLite).

Plain asserts and a tiny runner, so this needs nothing beyond the project's
existing sqlalchemy + psycopg2 -- but the test functions are pytest-shaped,
so `pytest test/` works unchanged if pytest is added later.

    python test/test_poll_and_dispatch.py
    python test/test_poll_and_dispatch.py --keep    # leave the schema for inspection
"""

from __future__ import annotations

import sys
import traceback

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from testdata import (  # noqa: E402
    drop_schema,
    fresh_session,
    make_engine,
    make_event,
    make_event_series,
    make_task,
)

from demo import create_sample_result_on_analysis_succeeded, poll_and_dispatch  # noqa: E402
from models import EventHandlerCheckpoint, EventRecord, Result  # noqa: E402

HANDLER = "create-sample-result-on-analysis-succeeded"


def recording_handler(seen: list[int]):
    """A handler with no side effects beyond recording dispatch order."""

    def handle(session: Session, ev: EventRecord) -> None:
        seen.append(ev.payload["seq"])

    return handle


# --------------------------------------------------------------------------
def test_dispatches_pending_event(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, sample_id="SEQ-001")

    n = poll_and_dispatch(
        session,
        handler_name=HANDLER,
        event_types=["AnalysisTaskSucceeded"],
        handle=create_sample_result_on_analysis_succeeded,
    )

    assert n == 1, f"expected 1 event dispatched, got {n}"

    result = session.scalars(select(Result)).one()
    assert result.name == "result-SEQ-001", result.name
    assert result.pipeline_version == "v2.3.1", result.attributes

    emitted = session.scalars(
        select(EventRecord).where(EventRecord.event_type == "SampleResultCreated")
    ).one()
    assert emitted.causation_id == ev.event_id, "causation_id should chain to the source event"
    assert emitted.causation_type == "event"
    assert emitted.correlation_id == ev.correlation_id, "correlation_id should propagate"

    checkpoint = session.scalars(select(EventHandlerCheckpoint)).one()
    assert checkpoint.handler_name == HANDLER
    assert checkpoint.event_id == ev.event_id


def test_second_poll_is_idempotent(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, sample_id="SEQ-001")

    first = poll_and_dispatch(
        session, handler_name=HANDLER, event_types=["AnalysisTaskSucceeded"],
        handle=create_sample_result_on_analysis_succeeded,
    )
    second = poll_and_dispatch(
        session, handler_name=HANDLER, event_types=["AnalysisTaskSucceeded"],
        handle=create_sample_result_on_analysis_succeeded,
    )

    assert (first, second) == (1, 0), f"expected (1, 0), got ({first}, {second})"
    assert session.scalar(select(func.count()).select_from(Result)) == 1, "handler ran twice"


def test_each_handler_tracks_its_own_progress(session: Session) -> None:
    """The composite checkpoint PK is what lets N handlers consume the same event."""
    task = make_task(session)
    make_event(session, task, seq=0)

    seen_a: list[int] = []
    seen_b: list[int] = []
    n_a = poll_and_dispatch(session, handler_name="handler-a",
                            event_types=["AnalysisTaskSucceeded"], handle=recording_handler(seen_a))
    n_b = poll_and_dispatch(session, handler_name="handler-b",
                            event_types=["AnalysisTaskSucceeded"], handle=recording_handler(seen_b))

    assert (n_a, n_b) == (1, 1), f"both handlers should see the event, got ({n_a}, {n_b})"
    assert seen_a == seen_b == [0]


def test_another_handlers_checkpoint_does_not_mask_event(session: Session) -> None:
    """
    Regression guard: the handler_name predicate must live *inside* the
    NOT EXISTS subquery. Without it, one handler's checkpoint would hide the
    event from every other handler.
    """
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    session.add(EventHandlerCheckpoint(handler_name="some-other-handler", event_id=ev.event_id))
    session.commit()

    seen: list[int] = []
    n = poll_and_dispatch(session, handler_name="mine",
                          event_types=["AnalysisTaskSucceeded"], handle=recording_handler(seen))

    assert n == 1, "another handler's checkpoint must not hide this event"
    assert seen == [0]


def test_filters_by_event_type(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, event_type="AnalysisTaskSucceeded", seq=0)
    make_event(session, task, event_type="AnalysisTaskFailed", seq=1)
    make_event(session, task, event_type="SomethingUnrelated", seq=2)

    seen: list[int] = []
    n = poll_and_dispatch(session, handler_name=HANDLER,
                          event_types=["AnalysisTaskSucceeded"], handle=recording_handler(seen))

    assert n == 1, f"only the subscribed event type should dispatch, got {n}"
    assert seen == [0], seen


def test_batch_size_limits_and_preserves_order(session: Session) -> None:
    task = make_task(session)
    make_event_series(session, task, count=5)

    seen: list[int] = []
    handle = recording_handler(seen)
    counts = [
        poll_and_dispatch(session, handler_name=HANDLER, event_types=["AnalysisTaskSucceeded"],
                          handle=handle, batch_size=2)
        for _ in range(3)
    ]

    assert counts == [2, 2, 1], f"expected batches of [2, 2, 1], got {counts}"
    assert seen == [0, 1, 2, 3, 4], f"events should dispatch oldest-first, got {seen}"

    drained = poll_and_dispatch(session, handler_name=HANDLER,
                                event_types=["AnalysisTaskSucceeded"], handle=handle, batch_size=2)
    assert drained == 0, "queue should be drained"


def test_batch_size_default_does_not_truncate_small_queue(session: Session) -> None:
    task = make_task(session)
    make_event_series(session, task, count=3)

    seen: list[int] = []
    n = poll_and_dispatch(session, handler_name=HANDLER,
                          event_types=["AnalysisTaskSucceeded"], handle=recording_handler(seen))

    assert n == 3, f"default batch_size should cover a 3-event queue, got {n}"
    assert seen == [0, 1, 2]


class _CapturingSession:
    """Delegates to a real Session, recording the statements passed to scalars()."""

    def __init__(self, inner: Session, sink: list) -> None:
        self._inner, self._sink = inner, sink

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    def scalars(self, statement, *args, **kwargs):
        self._sink.append(statement)
        return self._inner.scalars(statement, *args, **kwargs)


def test_query_plans_as_an_anti_join(session: Session) -> None:
    """
    Guard against reverting to NOT IN: Postgres will not rewrite
    `NOT IN (subquery)` as an anti-join, and degrades to a per-row rescan
    once the checkpoint set exceeds work_mem.

    This EXPLAINs the statement poll_and_dispatch actually builds rather than
    a copy of it, so the test fails if the implementation changes shape.
    """
    task = make_task(session)
    make_event_series(session, task, count=3)

    captured: list = []
    poll_and_dispatch(
        _CapturingSession(session, captured),
        handler_name=HANDLER,
        event_types=["AnalysisTaskSucceeded"],
        handle=recording_handler([]),
    )
    assert len(captured) == 1, f"expected one polling query, captured {len(captured)}"

    sql = str(captured[0].compile(session.bind, compile_kwargs={"literal_binds": True}))
    plan = "\n".join(row[0] for row in session.execute(text("EXPLAIN " + sql)))

    assert "Anti Join" in plan, f"expected an anti-join, plan was:\n{plan}"
    assert "SubPlan" not in plan, f"unexpected SubPlan (NOT IN behaviour):\n{plan}"


# --------------------------------------------------------------------------
TESTS = [
    test_dispatches_pending_event,
    test_second_poll_is_idempotent,
    test_each_handler_tracks_its_own_progress,
    test_another_handlers_checkpoint_does_not_mask_event,
    test_filters_by_event_type,
    test_batch_size_limits_and_preserves_order,
    test_batch_size_default_does_not_truncate_small_queue,
    test_query_plans_as_an_anti_join,
]


def main(keep: bool = False) -> int:
    engine = make_engine()
    passed, failed = 0, []

    for test in TESTS:
        # Each test gets an empty schema, so they're order-independent.
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
