"""
Tests that several worker processes can run the same handler at once.

The hazard: poll_and_dispatch's SELECT is only a candidate list, so two
workers polling the same handler_name will return overlapping rows. Without a
claim they would both run the side effect, and the checkpoint primary key
would only notice afterwards -- too late, the writes already happened.

Each event is therefore claimed with a transaction-scoped advisory lock on
(handler_name, event_id) before its handler runs. These tests use real
threads on separate connections, with handlers slow enough to keep the window
wide open.

    python test/test_concurrency.py
"""

from __future__ import annotations

import sys
import threading
import time
import traceback
from collections import Counter

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

from models import EventHandlerCheckpoint, EventHandlerFailure  # noqa: E402
from outbox import poll_and_dispatch, replay  # noqa: E402

TYPES = ["AnalysisTaskSucceeded"]


def slow_recorder(seen: list, lock: threading.Lock, delay: float = 0.05):
    """Records every invocation, slowly, so concurrent workers overlap."""

    def handle(session: Session, ev) -> None:
        with lock:
            seen.append(ev.payload["seq"])
        time.sleep(delay)

    return handle


def run_workers(session: Session, count: int, target) -> list:
    """`count` threads, each with its own Session on the same engine."""
    bind = session.get_bind()
    results: list = []
    results_lock = threading.Lock()

    def worker() -> None:
        with Session(bind) as own:
            value = target(own)
        with results_lock:
            results.append(value)

    threads = [threading.Thread(target=worker) for _ in range(count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not any(t.is_alive() for t in threads), "a worker thread hung"
    return results


# --------------------------------------------------------------------------
def test_concurrent_workers_each_event_handled_once(session: Session) -> None:
    task = make_task(session)
    make_event_series(session, task, count=6)
    session.commit()

    seen: list = []
    lock = threading.Lock()
    handle = slow_recorder(seen, lock)

    totals = run_workers(
        session, 4,
        lambda s: poll_and_dispatch(s, handler_name="racy", event_types=TYPES, handle=handle),
    )

    counts = Counter(seen)
    duplicated = {seq: n for seq, n in counts.items() if n > 1}
    assert not duplicated, f"events handled more than once: {duplicated}"
    assert sorted(counts) == [0, 1, 2, 3, 4, 5], f"not every event ran: {sorted(counts)}"
    assert sum(totals) == 6, f"returned counts should total 6 across workers, got {totals}"

    session.expire_all()
    assert session.scalar(select(func.count()).select_from(EventHandlerCheckpoint)) == 6


def test_concurrent_workers_do_not_raise(session: Session) -> None:
    """Before claiming, the loser of a race hit a checkpoint primary-key
    violation. Now it skips cleanly."""
    task = make_task(session)
    make_event_series(session, task, count=4)
    session.commit()

    seen: list = []
    lock = threading.Lock()
    errors: list = []

    def worker(s: Session) -> int:
        try:
            return poll_and_dispatch(s, handler_name="racy", event_types=TYPES,
                                     handle=slow_recorder(seen, lock))
        except Exception as exc:  # noqa: BLE001 - recording it is the point
            errors.append(exc)
            return 0

    run_workers(session, 4, worker)

    assert not errors, f"a worker raised instead of skipping: {errors!r}"
    assert session.scalar(select(func.count()).select_from(EventHandlerFailure)) == 0


def test_claims_do_not_block_other_handlers(session: Session) -> None:
    """The lock is per (handler, event), so fan-out is unaffected: a slow
    handler must not stop a different handler consuming the same events."""
    task = make_task(session)
    make_event_series(session, task, count=3)
    session.commit()

    seen_a: list = []
    seen_b: list = []
    lock = threading.Lock()

    bind = session.get_bind()
    done = threading.Event()

    def slow_a() -> None:
        with Session(bind) as s:
            poll_and_dispatch(s, handler_name="handler-a", event_types=TYPES,
                              handle=slow_recorder(seen_a, lock, delay=0.15))
        done.set()

    thread = threading.Thread(target=slow_a)
    thread.start()
    time.sleep(0.05)  # let handler-a claim its first event

    with Session(bind) as s:
        n_b = poll_and_dispatch(s, handler_name="handler-b", event_types=TYPES,
                                handle=slow_recorder(seen_b, lock, delay=0))

    thread.join(timeout=30)
    assert done.is_set(), "handler-a never finished"
    assert n_b == 3, f"handler-b was blocked by handler-a's claims, dispatched {n_b}"
    assert sorted(seen_b) == [0, 1, 2]
    assert sorted(seen_a) == [0, 1, 2]


def test_claim_is_released_on_handler_failure(session: Session) -> None:
    """A failed dispatch rolls back, which drops the advisory lock with it --
    otherwise the event would stay claimed until the process died."""
    task = make_task(session)
    ev = make_event(session, task, seq=0)
    session.commit()

    def explodes(s: Session, e) -> None:
        raise RuntimeError("nope")

    n = poll_and_dispatch(session, handler_name="h", event_types=TYPES,
                          handle=explodes, backoff_seconds=0)
    assert n == 0

    # A second worker on a fresh connection must be able to claim it again.
    seen: list = []
    lock = threading.Lock()
    totals = run_workers(
        session, 1,
        lambda s: poll_and_dispatch(s, handler_name="h", event_types=TYPES,
                                    handle=slow_recorder(seen, lock, delay=0),
                                    backoff_seconds=0),
    )
    assert totals == [1], f"event stayed claimed after a failure: {totals}"
    assert seen == [0]
    assert ev.event_id is not None


def test_replay_waits_for_an_in_flight_claim(session: Session) -> None:
    """replay() blocks on the claim rather than skipping, so an operator's
    explicit request is never silently dropped."""
    task = make_task(session)
    ev = make_event(session, task, seq=0)
    session.commit()

    order: list = []
    lock = threading.Lock()
    bind = session.get_bind()

    def worker() -> None:
        with Session(bind) as s:
            poll_and_dispatch(s, handler_name="h", event_types=TYPES,
                              handle=slow_recorder(order, lock, delay=0.3))

    thread = threading.Thread(target=worker)
    thread.start()
    time.sleep(0.08)  # worker is now mid-handler, holding the claim

    started = time.monotonic()
    with Session(bind) as s:
        n = replay(s, handler_name="h", event_ids=[ev.event_id],
                   handle=slow_recorder(order, lock, delay=0))
    waited = time.monotonic() - started

    thread.join(timeout=30)

    assert n == 1, "replay skipped instead of waiting"
    assert len(order) == 2, f"expected the worker then the replay, got {order}"
    assert waited >= 0.1, (
        f"replay returned in {waited:.3f}s -- it should have waited for the "
        f"in-flight claim rather than running alongside it"
    )


# --------------------------------------------------------------------------
TESTS = [
    test_concurrent_workers_each_event_handled_once,
    test_concurrent_workers_do_not_raise,
    test_claims_do_not_block_other_handlers,
    test_claim_is_released_on_handler_failure,
    test_replay_waits_for_an_in_flight_claim,
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
