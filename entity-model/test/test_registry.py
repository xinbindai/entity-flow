"""
Tests for HandlerRegistry, dispatch_once and listen -- declaring handlers and
running them in a loop.

    python test/test_registry.py
"""

from __future__ import annotations

import sys
import threading
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

from models import EventHandlerCheckpoint, EventHandlerFailure  # noqa: E402
from outbox import (  # noqa: E402
    DEFAULT_MAX_ATTEMPTS,
    HandlerRegistry,
    dispatch_once,
    listen,
)


def recorder(seen: list):
    def handle(session: Session, ev) -> None:
        seen.append((ev.event_type, ev.payload.get("seq")))

    return handle


def session_factory_for(session: Session):
    """A factory bound to the same engine, so listen() gets its own session
    per cycle exactly as it would in a worker process."""
    bind = session.get_bind()
    return lambda: Session(bind)


# --------------------------------------------------------------------------
# Registration
# --------------------------------------------------------------------------
def test_register_and_iterate(session: Session) -> None:
    registry = HandlerRegistry()
    seen: list = []
    reg = registry.register(name="h1", event_types=["A", "B"], handle=recorder(seen))

    assert reg.name == "h1"
    assert reg.event_types == ("A", "B"), "event_types should be stored as a tuple"
    assert reg.max_attempts == DEFAULT_MAX_ATTEMPTS
    assert len(registry) == 1
    assert "h1" in registry
    assert registry.names() == ["h1"]
    assert [r.name for r in registry] == ["h1"]
    assert registry.get("h1") is reg


def test_decorator_form_returns_the_function(session: Session) -> None:
    registry = HandlerRegistry()
    seen: list = []

    @registry.on("AnalysisTaskSucceeded", name="create-sample-result")
    def handler(sess: Session, ev) -> None:
        seen.append(ev.event_type)

    assert "create-sample-result" in registry
    assert registry.get("create-sample-result").event_types == ("AnalysisTaskSucceeded",)
    # The decorator must not replace the function -- it stays directly callable.
    assert callable(handler)
    handler(session, type("E", (), {"event_type": "X"})())
    assert seen == ["X"]


def test_duplicate_name_is_rejected(session: Session) -> None:
    """Two handlers sharing a name would share a checkpoint ledger, and each
    would mark the other's events processed."""
    registry = HandlerRegistry()
    registry.register(name="dup", event_types=["A"], handle=recorder([]))

    try:
        registry.register(name="dup", event_types=["B"], handle=recorder([]))
    except ValueError as exc:
        assert "already registered" in str(exc), exc
    else:
        raise AssertionError("expected a duplicate handler name to be rejected")

    assert len(registry) == 1, "the failed registration should not have been stored"


def test_empty_event_types_is_rejected(session: Session) -> None:
    registry = HandlerRegistry()
    for bad in ([], ()):
        try:
            registry.register(name="none", event_types=bad, handle=recorder([]))
        except ValueError as exc:
            assert "no event types" in str(exc), exc
        else:
            raise AssertionError("expected a handler with no event types to be rejected")


def test_empty_name_is_rejected(session: Session) -> None:
    registry = HandlerRegistry()
    try:
        registry.register(name="", event_types=["A"], handle=recorder([]))
    except ValueError as exc:
        assert "name is required" in str(exc), exc
    else:
        raise AssertionError("expected an empty handler name to be rejected")


# --------------------------------------------------------------------------
# dispatch_once
# --------------------------------------------------------------------------
def test_dispatch_once_runs_every_handler(session: Session) -> None:
    task = make_task(session)
    make_event_series(session, task, count=3)

    registry = HandlerRegistry()
    seen_a: list = []
    seen_b: list = []
    registry.register(name="a", event_types=["AnalysisTaskSucceeded"], handle=recorder(seen_a))
    registry.register(name="b", event_types=["AnalysisTaskSucceeded"], handle=recorder(seen_b))

    n = dispatch_once(session, registry)

    assert n == 6, f"expected 3 events x 2 handlers, got {n}"
    assert [s[1] for s in seen_a] == [0, 1, 2]
    assert [s[1] for s in seen_b] == [0, 1, 2]
    assert session.scalar(select(func.count()).select_from(EventHandlerCheckpoint)) == 6


def test_dispatch_once_routes_by_event_type(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, event_type="AnalysisTaskSucceeded", seq=0)
    make_event(session, task, event_type="AnalysisTaskFailed", seq=1)

    registry = HandlerRegistry()
    on_success: list = []
    on_failure: list = []
    registry.register(name="s", event_types=["AnalysisTaskSucceeded"], handle=recorder(on_success))
    registry.register(name="f", event_types=["AnalysisTaskFailed"], handle=recorder(on_failure))

    dispatch_once(session, registry)

    assert [s[0] for s in on_success] == ["AnalysisTaskSucceeded"], on_success
    assert [s[0] for s in on_failure] == ["AnalysisTaskFailed"], on_failure


def test_dispatch_once_is_idempotent(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)

    registry = HandlerRegistry()
    seen: list = []
    registry.register(name="a", event_types=["AnalysisTaskSucceeded"], handle=recorder(seen))

    assert dispatch_once(session, registry) == 1
    assert dispatch_once(session, registry) == 0
    assert len(seen) == 1


def test_one_failing_handler_does_not_stop_the_others(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)

    registry = HandlerRegistry()
    seen: list = []

    def explodes(sess: Session, ev) -> None:
        raise RuntimeError("nope")

    registry.register(name="broken", event_types=["AnalysisTaskSucceeded"],
                      handle=explodes, backoff_seconds=0)
    registry.register(name="fine", event_types=["AnalysisTaskSucceeded"], handle=recorder(seen))

    n = dispatch_once(session, registry)

    assert n == 1, f"only the working handler should count as dispatched, got {n}"
    assert len(seen) == 1, "the working handler was skipped"
    assert session.scalar(select(func.count()).select_from(EventHandlerFailure)) == 1


def test_per_registration_settings_are_applied(session: Session) -> None:
    """max_attempts is per registration, not global."""
    task = make_task(session)
    make_event(session, task, seq=0)

    calls: list = []

    def fails_loudly(sess: Session, ev) -> None:
        calls.append(ev.event_id)
        raise RuntimeError("x")

    registry = HandlerRegistry()
    registry.register(name="strict", event_types=["AnalysisTaskSucceeded"],
                      handle=fails_loudly, max_attempts=1, backoff_seconds=0)

    dispatch_once(session, registry)
    session.expire_all()
    assert session.scalars(select(EventHandlerFailure)).one().attempts == 1
    assert len(calls) == 1, "handler should have been called once"

    dispatch_once(session, registry)
    assert len(calls) == 1, "max_attempts=1 should have dead-lettered after one failure"
    session.expire_all()
    assert session.scalars(select(EventHandlerFailure)).one().attempts == 1


def test_empty_registry_dispatches_nothing(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)
    assert dispatch_once(session, HandlerRegistry()) == 0


# --------------------------------------------------------------------------
# listen
# --------------------------------------------------------------------------
def test_listen_drains_then_stops_at_max_cycles(session: Session) -> None:
    task = make_task(session)
    make_event_series(session, task, count=3)
    session.commit()

    registry = HandlerRegistry()
    seen: list = []
    registry.register(name="a", event_types=["AnalysisTaskSucceeded"], handle=recorder(seen))

    total = listen(session_factory_for(session), registry, poll_interval=0.01, max_cycles=3)

    assert total == 3, f"expected all 3 events dispatched, got {total}"
    assert [s[1] for s in seen] == [0, 1, 2]


def test_listen_returns_zero_when_there_is_nothing_to_do(session: Session) -> None:
    session.commit()
    registry = HandlerRegistry()
    registry.register(name="a", event_types=["AnalysisTaskSucceeded"], handle=recorder([]))

    started = time.monotonic()
    total = listen(session_factory_for(session), registry, poll_interval=0.05, max_cycles=3)
    elapsed = time.monotonic() - started

    assert total == 0
    assert elapsed >= 0.05, f"idle cycles should sleep poll_interval, took {elapsed:.3f}s"


def test_listen_picks_up_events_committed_between_cycles(session: Session) -> None:
    """Each cycle opens a fresh session, so a long-running worker sees work
    committed by other connections rather than sitting on a stale snapshot."""
    task = make_task(session)
    session.commit()

    registry = HandlerRegistry()
    seen: list = []
    registry.register(name="a", event_types=["AnalysisTaskSucceeded"], handle=recorder(seen))

    factory = session_factory_for(session)
    stop = threading.Event()
    result: list = []

    worker = threading.Thread(
        target=lambda: result.append(listen(factory, registry, poll_interval=0.02, stop=stop))
    )
    worker.start()
    try:
        time.sleep(0.1)  # let it idle first
        with factory() as producer:
            producer.add(make_event(producer, task, seq=0, commit=False))
            producer.commit()
        time.sleep(0.3)
    finally:
        stop.set()
        worker.join(timeout=5)

    assert not worker.is_alive(), "listen did not stop when the event was set"
    assert [s[1] for s in seen] == [0], f"worker never saw the new event, saw {seen}"
    assert result == [1]


def test_listen_stops_promptly_on_the_stop_event(session: Session) -> None:
    session.commit()
    registry = HandlerRegistry()
    registry.register(name="a", event_types=["AnalysisTaskSucceeded"], handle=recorder([]))

    stop = threading.Event()
    factory = session_factory_for(session)
    started = time.monotonic()
    worker = threading.Thread(target=lambda: listen(factory, registry, poll_interval=30, stop=stop))
    worker.start()
    time.sleep(0.1)
    stop.set()
    worker.join(timeout=5)
    elapsed = time.monotonic() - started

    assert not worker.is_alive(), "listen ignored the stop event"
    assert elapsed < 5, (
        f"shutdown took {elapsed:.2f}s -- the idle sleep should wait on the event, "
        f"not block for the full poll_interval"
    )


# --------------------------------------------------------------------------
TESTS = [
    test_register_and_iterate,
    test_decorator_form_returns_the_function,
    test_duplicate_name_is_rejected,
    test_empty_event_types_is_rejected,
    test_empty_name_is_rejected,
    test_dispatch_once_runs_every_handler,
    test_dispatch_once_routes_by_event_type,
    test_dispatch_once_is_idempotent,
    test_one_failing_handler_does_not_stop_the_others,
    test_per_registration_settings_are_applied,
    test_empty_registry_dispatches_nothing,
    test_listen_drains_then_stops_at_max_cycles,
    test_listen_returns_zero_when_there_is_nothing_to_do,
    test_listen_picks_up_events_committed_between_cycles,
    test_listen_stops_promptly_on_the_stop_event,
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
