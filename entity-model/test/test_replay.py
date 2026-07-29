"""
Tests for outbox.replay -- targeted re-dispatch of specific events to one
handler, overriding the "a checkpoint means done" rule that poll_and_dispatch
enforces.

The property worth protecting: a replay that fails must not leave the event
looking unprocessed, or every subsequent poll would retry and fail on it.

    python test/test_replay.py
"""

from __future__ import annotations

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

from demo import create_sample_result_on_analysis_succeeded  # noqa: E402
from models import EventHandlerCheckpoint, Result  # noqa: E402
from outbox import poll_and_dispatch, replay  # noqa: E402

HANDLER = "create-sample-result"
TYPES = ["AnalysisTaskSucceeded"]


def recording_handler(seen: list[int]):
    def handle(session: Session, ev) -> None:
        seen.append(ev.payload["seq"])

    return handle


def checkpoint_for(session: Session, handler_name: str, event_id: uuid.UUID):
    return session.scalars(
        select(EventHandlerCheckpoint).where(
            EventHandlerCheckpoint.handler_name == handler_name,
            EventHandlerCheckpoint.event_id == event_id,
        )
    ).one_or_none()


# --------------------------------------------------------------------------
def test_replays_an_already_processed_event(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    seen: list[int] = []
    handle = recording_handler(seen)

    assert poll_and_dispatch(session, handler_name=HANDLER, event_types=TYPES, handle=handle) == 1
    assert poll_and_dispatch(session, handler_name=HANDLER, event_types=TYPES, handle=handle) == 0

    n = replay(session, handler_name=HANDLER, event_ids=[ev.event_id], handle=handle)

    assert n == 1, f"replay should have dispatched 1, got {n}"
    assert seen == [0, 0], f"handler should have run twice, saw {seen}"


def test_replay_does_not_duplicate_the_checkpoint(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)
    handle = recording_handler([])

    poll_and_dispatch(session, handler_name=HANDLER, event_types=TYPES, handle=handle)
    before = checkpoint_for(session, HANDLER, ev.event_id).processed_at

    replay(session, handler_name=HANDLER, event_ids=[ev.event_id], handle=handle)
    session.expire_all()

    rows = session.scalars(select(EventHandlerCheckpoint)).all()
    assert len(rows) == 1, f"expected the checkpoint to be upserted, found {len(rows)} rows"
    assert rows[0].processed_at >= before, "processed_at should move forward on replay"


def test_replay_leaves_other_handlers_alone(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    seen_a: list[int] = []
    seen_b: list[int] = []
    poll_and_dispatch(session, handler_name="handler-a", event_types=TYPES,
                      handle=recording_handler(seen_a))
    poll_and_dispatch(session, handler_name="handler-b", event_types=TYPES,
                      handle=recording_handler(seen_b))

    replay(session, handler_name="handler-a", event_ids=[ev.event_id],
           handle=recording_handler(seen_a))

    assert seen_a == [0, 0], seen_a
    assert seen_b == [0], f"handler-b should not have been replayed, saw {seen_b}"
    assert checkpoint_for(session, "handler-b", ev.event_id) is not None


def test_replay_of_a_never_processed_event_dispatches_it(session: Session) -> None:
    """Replay means "run the handler on these now", not "only if done before"."""
    task = make_task(session)
    ev = make_event(session, task, seq=7)

    seen: list[int] = []
    n = replay(session, handler_name=HANDLER, event_ids=[ev.event_id],
               handle=recording_handler(seen))

    assert n == 1
    assert seen == [7]
    assert checkpoint_for(session, HANDLER, ev.event_id) is not None


def test_replay_dispatches_oldest_first(session: Session) -> None:
    task = make_task(session)
    events = make_event_series(session, task, count=4)

    seen: list[int] = []
    ids = [events[3].event_id, events[0].event_id, events[2].event_id]
    n = replay(session, handler_name=HANDLER, event_ids=ids, handle=recording_handler(seen))

    assert n == 3
    assert seen == [0, 2, 3], f"expected occurred_at order regardless of input order, got {seen}"


def test_unknown_event_id_raises(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)
    bogus = uuid.uuid4()

    seen: list[int] = []
    try:
        replay(session, handler_name=HANDLER, event_ids=[ev.event_id, bogus],
               handle=recording_handler(seen))
    except ValueError as exc:
        assert str(bogus) in str(exc), exc
        assert seen == [], "nothing should dispatch when an id is unknown"
    else:
        raise AssertionError("expected ValueError for an unknown event id")


def test_empty_event_ids_is_a_noop(session: Session) -> None:
    seen: list[int] = []
    assert replay(session, handler_name=HANDLER, event_ids=[],
                  handle=recording_handler(seen)) == 0
    assert seen == []


def test_failed_replay_does_not_poison_polling(session: Session) -> None:
    """
    The demo handler isn't idempotent -- replaying it collides with
    uq_entity_name_per_subcategory. The checkpoint must survive that, or the
    event would look unprocessed and every later poll would retry and fail.
    """
    task = make_task(session)
    ev = make_event(session, task, seq=0, sample_id="SEQ-001")

    poll_and_dispatch(session, handler_name=HANDLER, event_types=TYPES,
                      handle=create_sample_result_on_analysis_succeeded)
    assert session.scalar(select(func.count()).select_from(Result)) == 1

    try:
        replay(session, handler_name=HANDLER, event_ids=[ev.event_id],
               handle=create_sample_result_on_analysis_succeeded)
    except Exception as exc:
        assert "uq_entity_name_per_subcategory" in str(exc), exc
        # replay() already rolled back; this makes the assertions below report
        # the real problem even against an implementation that didn't.
        session.rollback()
    else:
        raise AssertionError("expected the unique constraint to reject the duplicate Result")

    assert checkpoint_for(session, HANDLER, ev.event_id) is not None, \
        "checkpoint was lost, so polling will now retry this event forever"
    assert session.scalar(select(func.count()).select_from(Result)) == 1, "duplicate Result created"

    # The decisive check: normal polling is unaffected.
    n = poll_and_dispatch(session, handler_name=HANDLER, event_types=TYPES,
                          handle=create_sample_result_on_analysis_succeeded)
    assert n == 0, f"poll re-dispatched a failed replay's event ({n})"


def test_partial_replay_failure_keeps_earlier_events_committed(session: Session) -> None:
    task = make_task(session)
    events = make_event_series(session, task, count=3)

    seen: list[int] = []

    def fails_on_third(sess: Session, ev) -> None:
        if ev.payload["seq"] == 2:
            raise RuntimeError("handler blew up")
        seen.append(ev.payload["seq"])

    try:
        replay(session, handler_name=HANDLER,
               event_ids=[e.event_id for e in events], handle=fails_on_third)
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected the handler error to propagate")

    assert seen == [0, 1], seen
    assert checkpoint_for(session, HANDLER, events[0].event_id) is not None
    assert checkpoint_for(session, HANDLER, events[1].event_id) is not None
    assert checkpoint_for(session, HANDLER, events[2].event_id) is None
    # Session is still usable after the rollback.
    assert session.scalar(select(func.count()).select_from(EventHandlerCheckpoint)) == 2


# --------------------------------------------------------------------------
TESTS = [
    test_replays_an_already_processed_event,
    test_replay_does_not_duplicate_the_checkpoint,
    test_replay_leaves_other_handlers_alone,
    test_replay_of_a_never_processed_event_dispatches_it,
    test_replay_dispatches_oldest_first,
    test_unknown_event_id_raises,
    test_empty_event_ids_is_a_noop,
    test_failed_replay_does_not_poison_polling,
    test_partial_replay_failure_keeps_earlier_events_committed,
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
