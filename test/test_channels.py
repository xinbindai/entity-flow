"""
Tests for channels -- scoping which handlers see which events (section 3.7).

The requirement these are written against: for one event type, some handlers
must consume it from every channel while others must see only their own. Both
subscribe to the same type, and neither may disturb the other.

Most of the weight is on the two ways this can fail quietly. A subscription
that widens by accident leaks another channel's events, and a channel typo
produces a system that looks idle rather than broken -- so the default is
fail-closed and the empty-poll explanation names the channel case outright.

    python test/test_channels.py
"""

from __future__ import annotations

import logging
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sqlalchemy import select
from sqlalchemy.orm import Session

from testdata import (  # noqa: E402
    drop_schema,
    fresh_session,
    make_engine,
    make_event,
    make_task,
)

from entitymodel.models import DEFAULT_CHANNEL, EventRecord  # noqa: E402
from entitymodel.outbox import (  # noqa: E402
    ALL_CHANNELS,
    HandlerRegistry,
    dispatch_once,
    fire_event,
    poll_and_dispatch,
    replay,
)

TYPES = ["AnalysisTaskSucceeded"]


def collector(into: list):
    def handle(session: Session, ev) -> None:
        into.append(ev.event_id)

    return handle


def noop(session: Session, ev) -> None:
    pass


# --------------------------------------------------------------------------
# The column
# --------------------------------------------------------------------------
def test_an_event_defaults_to_the_default_channel(session: Session) -> None:
    task = make_task(session)
    ev = fire_event(
        session, task, event_type="AnalysisTaskSucceeded", new_status="Succeeded",
        payload={}, source="t", actor_type="worker",
    )
    session.commit()

    assert ev.channel == DEFAULT_CHANNEL, ev.channel


def test_fire_event_puts_an_event_in_a_named_channel(session: Session) -> None:
    task = make_task(session)
    ev = fire_event(
        session, task, event_type="AnalysisTaskSucceeded", new_status="Succeeded",
        payload={}, source="t", actor_type="worker", channel="lab-a",
    )
    session.commit()

    stored = session.scalar(select(EventRecord).where(EventRecord.event_id == ev.event_id))
    assert stored.channel == "lab-a", stored.channel


# --------------------------------------------------------------------------
# Selection
# --------------------------------------------------------------------------
def test_a_scoped_handler_sees_only_its_channel(session: Session) -> None:
    task = make_task(session)
    mine = make_event(session, task, seq=0, channel="lab-a")
    make_event(session, task, seq=1, channel="lab-b")

    seen: list = []
    poll_and_dispatch(session, handler_name="h", event_types=TYPES,
                      channels=["lab-a"], handle=collector(seen))

    assert seen == [mine.event_id], seen


def test_a_handler_can_name_several_channels(session: Session) -> None:
    task = make_task(session)
    a = make_event(session, task, seq=0, channel="lab-a")
    b = make_event(session, task, seq=1, channel="lab-b")
    make_event(session, task, seq=2, channel="lab-c")

    seen: list = []
    poll_and_dispatch(session, handler_name="h", event_types=TYPES,
                      channels=["lab-a", "lab-b"], handle=collector(seen))

    assert set(seen) == {a.event_id, b.event_id}, seen


def test_naming_no_channel_means_the_default_not_everything(session: Session) -> None:
    """The load-bearing decision: a subscription must not widen by omission."""
    task = make_task(session)
    plain = make_event(session, task, seq=0)                       # 'default'
    make_event(session, task, seq=1, channel="lab-a")

    seen: list = []
    poll_and_dispatch(session, handler_name="h", event_types=TYPES, handle=collector(seen))

    assert seen == [plain.event_id], \
        "a handler that named no channel must not receive another channel's events"


def test_all_channels_consumes_every_channel(session: Session) -> None:
    task = make_task(session)
    a = make_event(session, task, seq=0, channel="lab-a")
    b = make_event(session, task, seq=1, channel="lab-b")
    d = make_event(session, task, seq=2)

    seen: list = []
    poll_and_dispatch(session, handler_name="audit", event_types=TYPES,
                      channels=ALL_CHANNELS, handle=collector(seen))

    assert set(seen) == {a.event_id, b.event_id, d.event_id}, seen


def test_all_channels_picks_up_a_channel_invented_later(session: Session) -> None:
    """ALL_CHANNELS includes channels that did not exist when it was written."""
    task = make_task(session)
    make_event(session, task, seq=0, channel="lab-a")

    seen: list = []
    poll_and_dispatch(session, handler_name="audit", event_types=TYPES,
                      channels=ALL_CHANNELS, handle=collector(seen))
    assert len(seen) == 1

    later = make_event(session, task, seq=1, channel="lab-invented-today")
    poll_and_dispatch(session, handler_name="audit", event_types=TYPES,
                      channels=ALL_CHANNELS, handle=collector(seen))

    assert later.event_id in seen, seen


# --------------------------------------------------------------------------
# The requirement: scoped and cross-channel handlers on one event type
# --------------------------------------------------------------------------
def test_a_scoped_and_a_wildcard_handler_share_an_event_type(session: Session) -> None:
    task = make_task(session)
    a = make_event(session, task, seq=0, channel="lab-a")
    b = make_event(session, task, seq=1, channel="lab-b")

    registry = HandlerRegistry()
    audit: list = []
    lab_a: list = []
    registry.register(name="audit-trail", event_types=TYPES,
                      channels=ALL_CHANNELS, handle=collector(audit))
    registry.register(name="lab-a-accession", event_types=TYPES,
                      channels=["lab-a"], handle=collector(lab_a))

    dispatch_once(session, registry)

    assert set(audit) == {a.event_id, b.event_id}, audit
    assert lab_a == [a.event_id], lab_a


def test_their_checkpoints_do_not_interfere(session: Session) -> None:
    """Idempotency is (handler_name, event_id); channel is selection, not identity."""
    task = make_task(session)
    a = make_event(session, task, seq=0, channel="lab-a")

    audit: list = []
    lab_a: list = []
    poll_and_dispatch(session, handler_name="audit-trail", event_types=TYPES,
                      channels=ALL_CHANNELS, handle=collector(audit))
    poll_and_dispatch(session, handler_name="lab-a-accession", event_types=TYPES,
                      channels=["lab-a"], handle=collector(lab_a))

    assert audit == [a.event_id] and lab_a == [a.event_id], \
        "one event, one checkpoint per handler -- neither hides it from the other"

    audit2: list = []
    poll_and_dispatch(session, handler_name="audit-trail", event_types=TYPES,
                      channels=ALL_CHANNELS, handle=collector(audit2))
    assert audit2 == [], "and each still no-ops on its own second pass"


# --------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------
def expect_value_error(fn, fragment: str, what: str) -> None:
    try:
        fn()
    except ValueError as exc:
        assert fragment in str(exc), f"{what}: expected {fragment!r} in {exc}"
    else:
        raise AssertionError(f"{what}: expected ValueError")


def test_a_bare_string_channel_is_rejected(session: Session) -> None:
    """channels="lab-a" would iterate into one channel per character."""
    registry = HandlerRegistry()
    expect_value_error(
        lambda: registry.register(name="h", event_types=TYPES, handle=noop,
                                  channels="lab-a"),
        "one channel per character",
        "a bare string",
    )


def test_an_empty_channel_list_is_rejected(session: Session) -> None:
    registry = HandlerRegistry()
    expect_value_error(
        lambda: registry.register(name="h", event_types=TYPES, handle=noop, channels=[]),
        "subscribes to no channels",
        "an empty list",
    )


def test_the_wildcard_string_is_reserved(session: Session) -> None:
    registry = HandlerRegistry()
    expect_value_error(
        lambda: registry.register(name="h", event_types=TYPES, handle=noop,
                                  channels=["*"]),
        "reserved",
        "'*' inside a list",
    )


def test_the_wildcard_string_alone_means_all_channels(session: Session) -> None:
    """So a configuration file, which cannot hold a Python object, can say it."""
    registry = HandlerRegistry()
    registry.register(name="h", event_types=TYPES, handle=noop, channels="*")

    assert registry.get("h").channels is ALL_CHANNELS


def test_the_sentinel_is_not_iterable(session: Session) -> None:
    try:
        list(ALL_CHANNELS)
    except TypeError as exc:
        assert "sentinel" in str(exc), exc
    else:
        raise AssertionError("iterating ALL_CHANNELS must not silently yield nothing")


# --------------------------------------------------------------------------
# The fifth reason a poll finds nothing
# --------------------------------------------------------------------------
class Captured:
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

    def text(self) -> str:
        return "\n".join(r.getMessage() for r in self.records)


def test_an_empty_poll_says_wrong_channel(session: Session) -> None:
    """A channel typo must not read as "nothing has happened yet"."""
    task = make_task(session)
    make_event(session, task, seq=0, channel="lab-a")

    with Captured() as log:
        poll_and_dispatch(session, handler_name="h", event_types=TYPES,
                          channels=["lab_a"], handle=noop)   # underscore, not hyphen

    text = log.text()
    assert "none in channel(s)" in text, text
    assert "lab_a" in text and "lab-a" in text, \
        "the typo is only obvious next to the channel the events are actually in"
    assert "exist yet" not in text, \
        "reporting no such events would send the reader to the producer"


def test_the_breakdown_is_scoped_to_the_handlers_channels(session: Session) -> None:
    task = make_task(session)
    make_event(session, task, seq=0, channel="lab-a")
    make_event(session, task, seq=1, channel="lab-b")
    make_event(session, task, seq=2, channel="lab-b")

    poll_and_dispatch(session, handler_name="h", event_types=TYPES,
                      channels=["lab-a"], handle=noop)

    with Captured() as log:
        poll_and_dispatch(session, handler_name="h", event_types=TYPES,
                          channels=["lab-a"], handle=noop)

    text = log.text()
    assert "of 1 event(s)" in text, f"must count only this handler's channels: {text}"
    assert "1 already processed" in text, text
    assert "lab-a" in text, text


# --------------------------------------------------------------------------
# Replay
# --------------------------------------------------------------------------
def test_replay_ignores_channel(session: Session) -> None:
    """An operator naming an id means it, whatever the subscription says."""
    task = make_task(session)
    ev = make_event(session, task, seq=0, channel="lab-b")

    seen: list = []
    dispatched = replay(session, handler_name="lab-a-only", event_ids=[ev.event_id],
                        handle=collector(seen))

    assert dispatched == 1 and seen == [ev.event_id], (dispatched, seen)


TESTS = [
    test_an_event_defaults_to_the_default_channel,
    test_fire_event_puts_an_event_in_a_named_channel,
    test_a_scoped_handler_sees_only_its_channel,
    test_a_handler_can_name_several_channels,
    test_naming_no_channel_means_the_default_not_everything,
    test_all_channels_consumes_every_channel,
    test_all_channels_picks_up_a_channel_invented_later,
    test_a_scoped_and_a_wildcard_handler_share_an_event_type,
    test_their_checkpoints_do_not_interfere,
    test_a_bare_string_channel_is_rejected,
    test_an_empty_channel_list_is_rejected,
    test_the_wildcard_string_is_reserved,
    test_the_wildcard_string_alone_means_all_channels,
    test_the_sentinel_is_not_iterable,
    test_an_empty_poll_says_wrong_channel,
    test_the_breakdown_is_scoped_to_the_handlers_channels,
    test_replay_ignores_channel,
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
