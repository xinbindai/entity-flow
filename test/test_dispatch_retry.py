"""
Tests for handler-dispatch retry limiting and dead-lettering.

Two properties matter here. A failing handler must not block the queue for
every other event, and it must not be retried forever: after max_attempts
consecutive failures the event is dead-lettered and only an explicit replay
brings it back.

    python test/test_dispatch_retry.py
"""

from __future__ import annotations

import sys
import time
import traceback

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
from entitymodel.outbox import dead_lettered, poll_and_dispatch, replay  # noqa: E402

HANDLER = "flaky-handler"
TYPES = ["AnalysisTaskSucceeded"]


def poll(session: Session, handle, *, handler_name: str = HANDLER, **kwargs) -> int:
    """poll_and_dispatch with backoff off by default -- most tests here are
    about the attempt counter, not about timing. The backoff tests pass their
    own backoff_seconds."""
    kwargs.setdefault("backoff_seconds", 0.0)
    return poll_and_dispatch(
        session, handler_name=handler_name, event_types=TYPES, handle=handle, **kwargs
    )


def always_fails(message: str = "handler blew up"):
    def handle(session: Session, ev) -> None:
        raise RuntimeError(message)

    return handle


def fails_on(seqs: set[int], seen: list[int]):
    """Fails for the given payload seqs, records the rest."""

    def handle(session: Session, ev) -> None:
        seq = ev.payload["seq"]
        if seq in seqs:
            raise RuntimeError(f"cannot handle {seq}")
        seen.append(seq)

    return handle


def failure_for(session: Session, handler_name: str, event_id):
    return session.scalars(
        select(EventHandlerFailure).where(
            EventHandlerFailure.handler_name == handler_name,
            EventHandlerFailure.event_id == event_id,
        )
    ).one_or_none()


# --------------------------------------------------------------------------
def test_failing_handler_does_not_abort_the_batch(session: Session) -> None:
    """One poison event must not stop the others in the same poll."""
    task = make_task(session)
    make_event_series(session, task, count=4)

    seen: list[int] = []
    n = poll(session, fails_on({1}, seen))

    assert seen == [0, 2, 3], f"expected the other three to dispatch, saw {seen}"
    assert n == 3, f"return value should count successes only, got {n}"


def test_failure_is_recorded_with_error_text(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    poll(session, always_fails("disk on fire"))

    row = failure_for(session, HANDLER, ev.event_id)
    assert row is not None, "no failure row recorded"
    assert row.attempts == 1, row.attempts
    assert "RuntimeError" in row.last_error and "disk on fire" in row.last_error, row.last_error
    assert row.first_failed_at is not None and row.last_failed_at is not None
    assert failure_for(session, HANDLER, ev.event_id).attempts == 1


def test_attempts_accumulate_across_polls(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)
    handle = always_fails()

    for expected in (1, 2, 3):
        poll(session, handle, max_attempts=5)
        session.expire_all()
        assert failure_for(session, HANDLER, ev.event_id).attempts == expected


def test_event_is_dead_lettered_at_the_limit(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)
    handle = always_fails()

    for _ in range(3):
        poll(session, handle, max_attempts=3)

    session.expire_all()
    assert failure_for(session, HANDLER, ev.event_id).attempts == 3

    # Fourth poll must not offer it again.
    calls: list[int] = []

    def counting(sess: Session, e) -> None:
        calls.append(1)

    n = poll(session, counting, max_attempts=3)
    assert n == 0 and calls == [], "dead-lettered event was dispatched again"
    session.expire_all()
    assert failure_for(session, HANDLER, ev.event_id).attempts == 3, "attempts kept climbing"


def test_dead_lettered_lists_the_event(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)
    handle = always_fails()

    for _ in range(2):
        poll(session, handle, max_attempts=2)

    rows = dead_lettered(session, handler_name=HANDLER, max_attempts=2)
    assert [r.event_id for r in rows] == [ev.event_id], rows
    assert dead_lettered(session, handler_name="someone-else", max_attempts=2) == []


def test_dead_lettering_is_per_handler(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)

    for _ in range(2):
        poll(session, always_fails(), handler_name="handler-a", max_attempts=2)

    seen: list[int] = []
    n = poll(session, fails_on(set(), seen), handler_name="handler-b", max_attempts=2)

    assert n == 1 and seen == [0], "handler-a's dead letter blocked handler-b"


def test_success_clears_failure_history(session: Session) -> None:
    """attempts counts consecutive failures, so a success resets it."""
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    poll(session, always_fails(), max_attempts=5)
    session.expire_all()
    assert failure_for(session, HANDLER, ev.event_id).attempts == 1

    seen: list[int] = []
    n = poll(session, fails_on(set(), seen), max_attempts=5)

    assert n == 1 and seen == [0]
    session.expire_all()
    assert failure_for(session, HANDLER, ev.event_id) is None, "failure row survived a success"
    assert session.scalar(select(func.count()).select_from(EventHandlerCheckpoint)) == 1


def test_replay_recovers_a_dead_lettered_event(session: Session) -> None:
    """The documented way out of the dead-letter state."""
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    for _ in range(2):
        poll(session, always_fails(), max_attempts=2)
    assert len(dead_lettered(session, handler_name=HANDLER, max_attempts=2)) == 1

    seen: list[int] = []
    n = replay(session, handler_name=HANDLER, event_ids=[ev.event_id],
               handle=fails_on(set(), seen))

    assert n == 1 and seen == [0], "replay should dispatch a dead-lettered event"
    session.expire_all()
    assert failure_for(session, HANDLER, ev.event_id) is None
    assert dead_lettered(session, handler_name=HANDLER, max_attempts=2) == []


def test_replay_still_raises_rather_than_counting_attempts(session: Session) -> None:
    """poll swallows and records; replay is operator-invoked and must surface."""
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    try:
        replay(session, handler_name=HANDLER, event_ids=[ev.event_id],
               handle=always_fails("boom"))
    except RuntimeError as exc:
        assert "boom" in str(exc)
    else:
        raise AssertionError("expected the handler error to propagate from replay")

    assert failure_for(session, HANDLER, ev.event_id) is None, \
        "replay should not write retry bookkeeping"


def test_max_attempts_is_per_call(session: Session) -> None:
    """The limit is applied at selection time, so a caller can raise it to
    give up-to-date attempts another chance without touching stored rows."""
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    for _ in range(2):
        poll(session, always_fails(), max_attempts=2)
    assert poll(session, always_fails(), max_attempts=2) == 0

    seen: list[int] = []
    n = poll(session, fails_on(set(), seen), max_attempts=10)

    assert n == 1 and seen == [0], "raising max_attempts should offer the event again"
    assert ev.event_id is not None


# --------------------------------------------------------------------------
# Backoff: a failed event must not be retried on the very next poll.
# --------------------------------------------------------------------------
def test_backoff_holds_the_event_until_its_deadline(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    poll(session, always_fails(), backoff_seconds=60)

    row = failure_for(session, HANDLER, ev.event_id)
    assert row.next_attempt_at > row.last_failed_at, "no backoff applied"
    delay = (row.next_attempt_at - row.last_failed_at).total_seconds()
    assert 59 <= delay <= 61, f"expected ~60s backoff, got {delay}"

    calls: list[int] = []
    n = poll(session, lambda s, e: calls.append(1), backoff_seconds=60)
    assert n == 0 and calls == [], "event was retried before its backoff elapsed"


def test_event_becomes_eligible_once_backoff_elapses(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)

    poll(session, always_fails(), backoff_seconds=0.2)

    seen: list[int] = []
    assert poll(session, fails_on(set(), seen), backoff_seconds=0.2) == 0, "retried too early"

    time.sleep(0.35)
    n = poll(session, fails_on(set(), seen), backoff_seconds=0.2)
    assert n == 1 and seen == [0], "event never became eligible again"


def test_backoff_grows_exponentially(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)
    base = 0.05

    delays = []
    for _ in range(3):
        poll(session, always_fails(), backoff_seconds=base, max_attempts=9)
        session.expire_all()
        row = failure_for(session, HANDLER, ev.event_id)
        delays.append((row.next_attempt_at - row.last_failed_at).total_seconds())
        time.sleep(delays[-1] + 0.05)

    expected = [base, base * 2, base * 4]
    for got, want in zip(delays, expected):
        assert abs(got - want) < 0.02, f"expected {expected}, got {delays}"


def test_backoff_is_capped(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    poll(session, always_fails(), backoff_seconds=1000, max_backoff_seconds=0.2)

    row = failure_for(session, HANDLER, ev.event_id)
    delay = (row.next_attempt_at - row.last_failed_at).total_seconds()
    assert abs(delay - 0.2) < 0.02, f"expected the cap to apply, got {delay}s"


def test_changing_backoff_does_not_release_a_held_event(session: Session) -> None:
    """next_attempt_at is a stored deadline, unlike max_attempts which is
    compared at selection time. Lowering the backoff only affects later
    failures."""
    task = make_task(session)
    make_event(session, task, seq=0)

    poll(session, always_fails(), backoff_seconds=60)

    calls: list[int] = []
    n = poll(session, lambda s, e: calls.append(1), backoff_seconds=0)
    assert n == 0 and calls == [], "a stored deadline should not be undone by new config"


def test_backoff_is_per_handler(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)

    poll(session, always_fails(), handler_name="handler-a", backoff_seconds=60)

    seen: list[int] = []
    n = poll(session, fails_on(set(), seen), handler_name="handler-b", backoff_seconds=60)
    assert n == 1 and seen == [0], "handler-a's backoff held back handler-b"


# --------------------------------------------------------------------------
TESTS = [
    test_failing_handler_does_not_abort_the_batch,
    test_failure_is_recorded_with_error_text,
    test_attempts_accumulate_across_polls,
    test_event_is_dead_lettered_at_the_limit,
    test_dead_lettered_lists_the_event,
    test_dead_lettering_is_per_handler,
    test_success_clears_failure_history,
    test_replay_recovers_a_dead_lettered_event,
    test_replay_still_raises_rather_than_counting_attempts,
    test_backoff_holds_the_event_until_its_deadline,
    test_event_becomes_eligible_once_backoff_elapses,
    test_backoff_grows_exponentially,
    test_backoff_is_capped,
    test_changing_backoff_does_not_release_a_held_event,
    test_backoff_is_per_handler,
    test_max_attempts_is_per_call,
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
