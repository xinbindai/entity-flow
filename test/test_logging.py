"""
Tests for the debug logging in entitymodel.outbox.

Log lines are usually not worth testing. These are, because they exist to
answer one question -- "why did my handler not run" -- and a diagnostic that
quietly stops saying anything is worse than none at all: it makes the reader
conclude the wrong thing.

    python test/test_logging.py
"""

from __future__ import annotations

import logging
import re
import sys
import time
import traceback
import uuid
from pathlib import Path

from sqlalchemy.orm import Session

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from testdata import (  # noqa: E402
    drop_schema,
    fresh_session,
    make_engine,
    make_event,
    make_event_series,
    make_task,
)

from entitymodel.outbox import (  # noqa: E402
    HandlerRegistry,
    dispatch_once,
    poll_and_dispatch,
    replay,
)

TYPES = ["AnalysisTaskSucceeded"]


class Captured:
    """Collects records from entitymodel.outbox for the duration of a block."""

    def __init__(self, level=logging.DEBUG):
        self.records: list[logging.LogRecord] = []
        self.level = level
        self._logger = logging.getLogger("entitymodel.outbox")

    def __enter__(self):
        self._previous = self._logger.level
        self._logger.setLevel(self.level)
        self._handler = logging.Handler()
        self._handler.emit = self.records.append
        self._logger.addHandler(self._handler)
        return self

    def __exit__(self, *exc):
        self._logger.removeHandler(self._handler)
        self._logger.setLevel(self._previous)
        return False

    def messages(self, level=None) -> list[str]:
        return [
            r.getMessage() for r in self.records
            if level is None or r.levelno == level
        ]

    def text(self, level=None) -> str:
        return "\n".join(self.messages(level))


def boom(session: Session, ev) -> None:
    raise RuntimeError("disk on fire")


def noop(session: Session, ev) -> None:
    pass


# --------------------------------------------------------------------------
def test_a_successful_dispatch_is_traceable_by_event_id(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    with Captured() as log:
        poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=noop)

    text = log.text()
    assert str(ev.event_id) in text, "the event id must appear, or a single event can't be followed"
    assert "h:" in text, "the handler name must appear"
    assert "dispatching" in text and "handled and checkpointed" in text, text


def test_an_empty_poll_says_no_such_events(session: Session) -> None:
    make_task(session)
    with Captured() as log:
        poll_and_dispatch(session, handler_name="h", event_types=["Nope"], handle=noop)
    assert "no events of type ['Nope'] exist yet" in log.text(), log.text()


def test_an_empty_poll_says_already_processed(session: Session) -> None:
    task = make_task(session)
    make_event_series(session, task, count=2)
    poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=noop)

    with Captured() as log:
        poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=noop)
    assert "2 already processed" in log.text(), log.text()


def test_an_empty_poll_says_backing_off(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)
    poll_and_dispatch(session, handler_name="f", event_types=TYPES,
                      handle=boom, backoff_seconds=600)

    with Captured() as log:
        poll_and_dispatch(session, handler_name="f", event_types=TYPES,
                          handle=boom, backoff_seconds=600)
    assert "1 backing off" in log.text(), log.text()


def test_an_empty_poll_says_dead_lettered(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)
    poll_and_dispatch(session, handler_name="d", event_types=TYPES,
                      handle=boom, max_attempts=1, backoff_seconds=0)

    with Captured() as log:
        poll_and_dispatch(session, handler_name="d", event_types=TYPES,
                          handle=boom, max_attempts=1, backoff_seconds=0)
    assert "1 dead-lettered" in log.text(), log.text()


def test_a_failure_logs_the_error_and_the_next_attempt(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)

    with Captured() as log:
        poll_and_dispatch(session, handler_name="f", event_types=TYPES,
                          handle=boom, max_attempts=5, backoff_seconds=30)

    text = log.text()
    assert "RuntimeError: disk on fire" in text, text
    assert "attempt 1 of 5" in text, text
    assert "next attempt at" in text, text


def test_dead_lettering_warns_rather_than_only_debugs(session: Session) -> None:
    """
    An event nobody will retry, dropped with no signal, is the failure mode
    that costs most to find late -- so it is the one message that must survive
    a production log level.
    """
    task = make_task(session)
    make_event(session, task, seq=0)

    with Captured(level=logging.WARNING) as log:
        poll_and_dispatch(session, handler_name="d", event_types=TYPES,
                          handle=boom, max_attempts=1, backoff_seconds=0)

    warnings = log.messages(logging.WARNING)
    assert len(warnings) == 1, warnings
    assert "dead-lettered after 1 attempt(s)" in warnings[0]
    assert "replay()" in warnings[0], "the message should say how to recover"


def test_nothing_is_logged_when_debug_is_off(session: Session) -> None:
    """The diagnostics must not cost anything when nobody is watching --
    including the extra query behind the empty-poll explanation."""
    task = make_task(session)
    make_event(session, task, seq=0)

    with Captured(level=logging.WARNING) as log:
        poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=noop)
        poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=noop)

    assert log.messages() == [], f"debug output leaked at WARNING: {log.messages()}"


def test_the_empty_poll_query_is_skipped_when_debug_is_off(session: Session) -> None:
    """The explanation costs a second query, so it must be behind the level
    check, not merely behind the log call."""
    from sqlalchemy import event as sa_event

    engine = session.get_bind()
    counts = {"n": 0}

    @sa_event.listens_for(engine, "before_cursor_execute")
    def count(conn, cursor, statement, params, ctx, many):
        if "count(" in statement.lower():
            counts["n"] += 1

    try:
        make_task(session)
        with Captured(level=logging.WARNING):
            poll_and_dispatch(session, handler_name="h", event_types=["Nope"], handle=noop)
        quiet = counts["n"]

        with Captured(level=logging.DEBUG):
            poll_and_dispatch(session, handler_name="h", event_types=["Nope"], handle=noop)
        noisy = counts["n"]
    finally:
        sa_event.remove(engine, "before_cursor_execute", count)

    assert quiet == 0, f"the diagnostic query ran with debug off ({quiet} counts)"
    assert noisy > 0, "the diagnostic query should run with debug on"


def test_registration_logs_what_a_path_resolved_to(session: Session) -> None:
    with Captured() as log:
        registry = HandlerRegistry()
        registry.register(name="by-path", event_types=["A"],
                          handle="handlers_fixture:record")
    assert "handlers_fixture:record" in log.text(), log.text()
    assert "by-path" in log.text()


def test_replay_logs_the_ids_it_will_run(session: Session) -> None:
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    with Captured() as log:
        replay(session, handler_name="h", event_ids=[ev.event_id], handle=noop)

    assert "replaying 1 event(s)" in log.text(), log.text()
    assert str(ev.event_id) in log.text()


def test_dispatch_once_reports_the_pass_total(session: Session) -> None:
    task = make_task(session)
    make_event_series(session, task, count=3)

    registry = HandlerRegistry()
    registry.register(name="a", event_types=TYPES, handle=noop)
    registry.register(name="b", event_types=TYPES, handle=noop)

    with Captured() as log:
        dispatch_once(session, registry)

    assert "over 2 handler(s) dispatched 6 event(s)" in log.text(), log.text()


def test_a_skipped_event_says_why(session: Session) -> None:
    """The post-claim recheck path: another worker finished it in between."""
    from entitymodel.models import EventHandlerCheckpoint

    task = make_task(session)
    ev = make_event(session, task, seq=0)

    # Stand in for the racing worker: the row exists, but our poll's snapshot
    # was taken before it did.
    pending = [ev]
    session.add(EventHandlerCheckpoint(handler_name="h", event_id=ev.event_id))
    session.commit()

    from entitymodel.outbox import _dispatch

    with Captured() as log:
        _dispatch(session, "h", pending, noop, overwrite_checkpoint=False,
                  record_failures=True, claim="try", max_attempts=5)

    assert "already processed by this handler" in log.text(), log.text()
    assert str(ev.event_id) in log.text()


# --------------------------------------------------------------------------
# Bracketing the handler call. The point of the pair is to separate time and
# failures inside the handler from this module's own work around it, and to
# name only things that exist as columns, so a log line leads back to a row.
# --------------------------------------------------------------------------
def test_the_handler_call_is_bracketed(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)

    with Captured() as log:
        poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=noop)

    text = log.text()
    assert "entering handler" in text, text
    assert "left handler" in text, text


def test_both_ends_name_the_handler_and_the_event_type(session: Session) -> None:
    """The two fields a log search actually starts from."""
    task = make_task(session)
    ev = make_event(session, task, seq=0)

    with Captured() as log:
        poll_and_dispatch(session, handler_name="create-result", event_types=TYPES, handle=noop)

    for phrase in ("entering handler", "left handler"):
        line = [m for m in log.messages() if phrase in m]
        assert len(line) == 1, f"{phrase}: {log.text()}"
        assert "create-result" in line[0], line[0]
        assert TYPES[0] in line[0], f"the event type must appear on the {phrase} line: {line[0]}"
        assert str(ev.event_id) in line[0], line[0]


def test_the_bracket_is_balanced_when_the_handler_raises(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)

    with Captured() as log:
        poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=boom)

    text = log.text()
    assert "entering handler" in text and "left handler" in text, \
        "an entering with no left should mean the process died, not that it raised"


def test_the_exit_line_reports_how_long_the_handler_took(session: Session) -> None:
    """Timed around the call alone, so the commit that follows is not counted."""
    task = make_task(session)
    make_event(session, task, seq=0)

    def slow(session: Session, ev) -> None:
        time.sleep(0.05)

    with Captured() as log:
        poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=slow)

    line = [m for m in log.messages() if "left handler" in m][0]
    ms = float(re.search(r"after ([0-9.]+) ms", line).group(1))
    assert 50 <= ms < 5000, f"expected roughly 50ms, got {ms}: {line}"


def test_a_failing_event_is_still_identified_by_type(session: Session) -> None:
    """So one event type can be filtered out of a noisy log without losing it."""
    task = make_task(session)
    make_event(session, task, seq=0)

    with Captured() as log:
        poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=boom)

    raised = [m for m in log.messages() if "raised" in m]
    assert raised and TYPES[0] in raised[0], raised



# --------------------------------------------------------------------------
# The bracket's level. It is INFO rather than DEBUG because log_search needs
# it to attribute the handler's own output, which is itself INFO -- at DEBUG
# the brackets vanish from an ordinary production log and take that ability
# with them. Pinned, because a level is a one-word change away from silently
# undoing that.
# --------------------------------------------------------------------------
def test_the_bracket_is_logged_at_info(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0)

    with Captured() as log:
        poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=noop)

    info = log.text(logging.INFO)
    assert "entering handler" in info, f"expected at INFO, got: {log.text()}"
    assert "left handler" in info, f"expected at INFO, got: {log.text()}"


def test_the_bracket_survives_when_debug_is_off(session: Session) -> None:
    """The guarantee log_search rests on: a production log still delimits runs."""
    task = make_task(session)
    make_event(session, task, seq=0)

    with Captured(level=logging.INFO) as log:
        poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=noop)

    text = log.text()
    assert "entering handler" in text and "left handler" in text, text


def test_the_rest_stays_at_debug(session: Session) -> None:
    """Only the pair is promoted. INFO must not turn into a firehose."""
    task = make_task(session)
    make_event(session, task, seq=0)

    with Captured(level=logging.INFO) as log:
        poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=noop)

    text = log.text()
    assert "candidate(s)" not in text, f"polling detail leaked to INFO: {text}"
    assert "handled and checkpointed" not in text, f"checkpoint detail leaked to INFO: {text}"
    assert len(log.messages()) == 2, f"exactly the pair, got: {log.messages()}"



# --------------------------------------------------------------------------
TESTS = [
    test_a_successful_dispatch_is_traceable_by_event_id,
    test_an_empty_poll_says_no_such_events,
    test_an_empty_poll_says_already_processed,
    test_an_empty_poll_says_backing_off,
    test_an_empty_poll_says_dead_lettered,
    test_a_failure_logs_the_error_and_the_next_attempt,
    test_dead_lettering_warns_rather_than_only_debugs,
    test_nothing_is_logged_when_debug_is_off,
    test_the_empty_poll_query_is_skipped_when_debug_is_off,
    test_registration_logs_what_a_path_resolved_to,
    test_replay_logs_the_ids_it_will_run,
    test_dispatch_once_reports_the_pass_total,
    test_a_skipped_event_says_why,
    test_the_handler_call_is_bracketed,
    test_both_ends_name_the_handler_and_the_event_type,
    test_the_bracket_is_balanced_when_the_handler_raises,
    test_the_exit_line_reports_how_long_the_handler_took,
    test_a_failing_event_is_still_identified_by_type,
    test_the_bracket_is_logged_at_info,
    test_the_bracket_survives_when_debug_is_off,
    test_the_rest_stays_at_debug,
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
