"""
Tests for HandlerCancelled -- a handler declining an event for good.

The distinction under test is between "this failed, try again" and "this is
not my business, stop asking". Getting it wrong is expensive in both
directions: retrying a decision wastes the whole backoff schedule and ends in
a dead-letter warning nobody should act on, and treating a real failure as a
decision loses the event silently.

    python test/test_cancelled.py
"""

from __future__ import annotations

import logging
import sys
import traceback
import uuid

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from testdata import (  # noqa: E402
    drop_schema,
    fresh_session,
    make_engine,
    make_event,
    make_event_series,
    make_task,
)

from entitymodel.models import EventHandlerCheckpoint, EventHandlerFailure  # noqa: E402
from entitymodel.outbox import (  # noqa: E402
    HandlerCancelled,
    HandlerRegistry,
    dead_lettered,
    dispatch_once,
    poll_and_dispatch,
    replay,
)

TYPES = ["AnalysisTaskSucceeded"]


def cancels(reason="not my business"):
    def handle(session: Session, ev) -> None:
        raise HandlerCancelled(reason)

    return handle


def boom(session: Session, ev) -> None:
    raise RuntimeError("disk on fire")


def counts(session: Session) -> tuple[int, int]:
    return (
        session.scalar(select(func.count()).select_from(EventHandlerCheckpoint)),
        session.scalar(select(func.count()).select_from(EventHandlerFailure)),
    )


# --------------------------------------------------------------------------
def test_a_cancelled_event_is_never_offered_again(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)

    seen: list = []

    def handle(sess, ev):
        seen.append(ev.event_id)
        raise HandlerCancelled("nope")

    poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=handle,
                      backoff_seconds=0)
    poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=handle,
                      backoff_seconds=0)

    assert len(seen) == 1, f"the handler saw the event {len(seen)} times; it should be once"


def test_it_is_checkpointed_not_failed(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)

    poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=cancels())

    checkpoints, failures = counts(session)
    assert checkpoints == 1, "a cancelled event should be settled by a checkpoint"
    assert failures == 0, "cancelling is not a failure and must not start a retry clock"


def test_it_is_not_dead_lettered(session: Session) -> None:
    """Dead letters are things needing attention. A decision is not one."""
    task = make_task(session)
    make_event(session, task, seq=0)

    poll_and_dispatch(session, handler_name="h", event_types=TYPES,
                      handle=cancels(), max_attempts=3, backoff_seconds=0)

    assert dead_lettered(session, handler_name="h", max_attempts=3) == []


def test_it_counts_towards_the_return_value(session: Session) -> None:
    """The number drives the drain loop, which needs to know a pass made
    progress -- settling events it will never see again is progress."""
    task = make_task(session)
    make_event_series(session, task, count=3)

    n = poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=cancels())
    assert n == 3, n
    assert poll_and_dispatch(session, handler_name="h", event_types=TYPES,
                             handle=cancels()) == 0, "second pass should find nothing"


def test_the_handlers_partial_writes_are_rolled_back(session: Session) -> None:
    """Do the work or decline it, not both."""
    from entitymodel.models import Task

    task = make_task(session)
    make_event(session, task, seq=0)

    def writes_then_cancels(sess: Session, ev) -> None:
        sess.add(Task(subcategory="bioinformatics_pipeline_analysis", name="side-effect",
                      status="Queued", correlation_id=uuid.uuid4(), attributes={}))
        sess.flush()
        raise HandlerCancelled("changed my mind")

    poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=writes_then_cancels)

    leaked = session.scalars(select(Task).where(Task.name == "side-effect")).all()
    assert leaked == [], "the cancelled handler's write survived"
    assert counts(session)[0] == 1, "but the event should still be settled"


def test_it_clears_earlier_failures(session: Session) -> None:
    """A handler that failed twice and then decides the event is not its
    business should not leave a retry clock ticking."""
    task = make_task(session)
    make_event(session, task, seq=0)

    poll_and_dispatch(session, handler_name="h", event_types=TYPES,
                      handle=boom, max_attempts=5, backoff_seconds=0)
    assert counts(session)[1] == 1, "expected a failure row first"

    poll_and_dispatch(session, handler_name="h", event_types=TYPES,
                      handle=cancels(), max_attempts=5, backoff_seconds=0)

    checkpoints, failures = counts(session)
    assert failures == 0, "the failure history should be cleared"
    assert checkpoints == 1


def test_only_the_cancelling_handler_is_affected(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)

    registry = HandlerRegistry()
    seen: list = []
    registry.register(name="declines", event_types=TYPES, handle=cancels())
    registry.register(name="works", event_types=TYPES,
                      handle=lambda s, e: seen.append(e.event_id))

    assert dispatch_once(session, registry) == 2
    assert len(seen) == 1, "the other handler must still get the event"


def test_a_real_failure_still_retries(session: Session) -> None:
    """The control: only HandlerCancelled short-circuits the retry."""
    task = make_task(session)
    make_event(session, task, seq=0)

    calls: list = []

    def fails(sess, ev):
        calls.append(1)
        raise RuntimeError("transient")

    for _ in range(2):
        poll_and_dispatch(session, handler_name="h", event_types=TYPES,
                          handle=fails, max_attempts=5, backoff_seconds=0)

    assert len(calls) == 2, "an ordinary failure should still be retried"
    assert counts(session) == (0, 1), counts(session)


def test_a_subclass_cancels_too(session: Session) -> None:
    class NotApplicable(HandlerCancelled):
        pass

    task = make_task(session)
    make_event(session, task, seq=0)

    def handle(sess, ev):
        raise NotApplicable("wrong assay")

    poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=handle)
    assert counts(session) == (1, 0)


def test_replay_settles_rather_than_raising(session: Session) -> None:
    """A decision is an answer, not an error -- even for an operator."""
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    n = replay(session, handler_name="h", event_ids=[ev.event_id], handle=cancels())

    assert n == 1, "replay should report the event as settled"
    assert counts(session) == (1, 0)


def test_replay_can_undo_a_cancellation(session: Session) -> None:
    """The way back if the decision was wrong."""
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=cancels())

    seen: list = []
    assert replay(session, handler_name="h", event_ids=[ev.event_id],
                  handle=lambda s, e: seen.append(e.event_id)) == 1
    assert seen == [ev.event_id], "replay should run a previously cancelled event"


def test_the_reason_is_logged(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)

    records: list = []
    logger = logging.getLogger("entitymodel.outbox")
    handler = logging.Handler()
    handler.emit = records.append
    previous = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        poll_and_dispatch(session, handler_name="h", event_types=TYPES,
                          handle=cancels("order was superseded"))
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)

    text = "\n".join(r.getMessage() for r in records)
    assert "cancelled by the handler" in text, text
    assert "order was superseded" in text, "the reason must survive into the log"
    assert "will not be retried" in text, text


def test_the_reason_is_stored_on_the_checkpoint(session: Session) -> None:
    """A checkpoint alone cannot say whether the handler worked or declined."""
    task = make_task(session)
    make_event(session, task, seq=0)

    poll_and_dispatch(session, handler_name="h", event_types=TYPES,
                      handle=cancels("order was superseded"))

    cp = session.scalars(select(EventHandlerCheckpoint)).one()
    assert cp.cancelled_reason == "order was superseded", cp.cancelled_reason


def test_a_worked_event_has_no_reason(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)

    poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=lambda s, e: None)

    cp = session.scalars(select(EventHandlerCheckpoint)).one()
    assert cp.cancelled_reason is None, "a handler that did the work leaves no reason"


def test_cancelled_and_worked_are_distinguishable(session: Session) -> None:
    """The whole point of the column: one query separates them."""
    task = make_task(session)
    make_event_series(session, task, count=2)

    def cancel_odd(sess, ev):
        if ev.payload["seq"] % 2:
            raise HandlerCancelled("odd sequence")

    poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=cancel_odd)

    worked = session.scalars(select(EventHandlerCheckpoint)
                             .where(EventHandlerCheckpoint.cancelled_reason.is_(None))).all()
    declined = session.scalars(select(EventHandlerCheckpoint)
                               .where(EventHandlerCheckpoint.cancelled_reason.is_not(None))).all()
    assert len(worked) == 1 and len(declined) == 1, (len(worked), len(declined))
    assert declined[0].cancelled_reason == "odd sequence"


def test_a_replay_that_succeeds_clears_the_reason(session: Session) -> None:
    """
    Otherwise the row would claim to be both declined and processed, and the
    stale reason would outlive the decision it recorded.
    """
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    poll_and_dispatch(session, handler_name="h", event_types=TYPES,
                      handle=cancels("wrong assay"))
    assert session.scalars(select(EventHandlerCheckpoint)).one().cancelled_reason == "wrong assay"

    replay(session, handler_name="h", event_ids=[ev.event_id], handle=lambda s, e: None)

    session.expire_all()
    cp = session.scalars(select(EventHandlerCheckpoint)).one()
    assert cp.cancelled_reason is None, f"stale reason survived the replay: {cp.cancelled_reason!r}"


def test_a_replay_that_cancels_again_records_the_new_reason(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=cancels("first"))
    replay(session, handler_name="h", event_ids=[ev.event_id], handle=cancels("second"))

    session.expire_all()
    assert session.scalars(select(EventHandlerCheckpoint)).one().cancelled_reason == "second"


def test_an_empty_reason_still_records_something(session: Session) -> None:
    """HandlerCancelled() with no message must not look like a success."""
    task = make_task(session)
    make_event(session, task, seq=0)

    def handle(sess, ev):
        raise HandlerCancelled()

    poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=handle)

    cp = session.scalars(select(EventHandlerCheckpoint)).one()
    assert cp.cancelled_reason == "HandlerCancelled", cp.cancelled_reason


# --------------------------------------------------------------------------
TESTS = [
    test_a_cancelled_event_is_never_offered_again,
    test_it_is_checkpointed_not_failed,
    test_it_is_not_dead_lettered,
    test_it_counts_towards_the_return_value,
    test_the_handlers_partial_writes_are_rolled_back,
    test_it_clears_earlier_failures,
    test_only_the_cancelling_handler_is_affected,
    test_a_real_failure_still_retries,
    test_a_subclass_cancels_too,
    test_replay_settles_rather_than_raising,
    test_replay_can_undo_a_cancellation,
    test_the_reason_is_logged,
    test_the_reason_is_stored_on_the_checkpoint,
    test_a_worked_event_has_no_reason,
    test_cancelled_and_worked_are_distinguishable,
    test_a_replay_that_succeeds_clears_the_reason,
    test_a_replay_that_cancels_again_records_the_new_reason,
    test_an_empty_reason_still_records_something,
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
