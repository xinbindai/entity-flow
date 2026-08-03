"""
Tests for entitymodel.celery_tasks -- submitting Celery tasks that are also
Entities, and driving the Task row through its lifecycle.

No broker is needed: Celery's eager mode runs tasks in-process, which is
exactly the part under test here. What eager mode does NOT cover is routing,
serialisation and the send itself, so submit_task is tested against a recording
double as well.

    python test/test_celery_tasks.py
"""

from __future__ import annotations

import json
import sys
import traceback
import uuid
from pathlib import Path

from celery import Celery
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from testdata import drop_schema, fresh_session, make_engine  # noqa: E402

from entitymodel.models import EventRecord, Task  # noqa: E402
from entitymodel.celery_tasks import (  # noqa: E402
    entity_task,
    load_payload,
    pending_submissions,
    submit_task,
)

SUBCATEGORY = "bioinformatics_pipeline_analysis"


class RecordingResult:
    def __init__(self, task_id): self.id = task_id


class RecordingApp:
    """Stands in for a Celery app: records sends instead of brokering them."""

    def __init__(self, fail: bool = False):
        self.sent: list[dict] = []
        self.fail = fail

    def send_task(self, name, kwargs=None, queue=None, **_):
        if self.fail:
            raise ConnectionError("broker unreachable")
        self.sent.append({"name": name, "kwargs": kwargs or {}, "queue": queue})
        return RecordingResult(f"celery-{len(self.sent)}")


def events_for(session: Session, task_id) -> list[str]:
    return [
        e.event_type
        for e in session.scalars(
            select(EventRecord).where(EventRecord.entity_id == task_id)
            .order_by(EventRecord.occurred_at, EventRecord.recorded_at)
        )
    ]


# --------------------------------------------------------------------------
# Submitting
# --------------------------------------------------------------------------
def test_submit_writes_the_task_and_sends_the_message(session: Session) -> None:
    app = RecordingApp()
    task = submit_task(session, app, celery_task_name="pkg.run", subcategory=SUBCATEGORY,
                       name="run-1", payload={"sample": "SEQ-001"}, queue="pipeline")

    assert task.status == "Queued"
    assert task.attributes["payload"] == {"sample": "SEQ-001"}
    assert task.attributes["celery_task_id"] == "celery-1"

    assert app.sent == [{"name": "pkg.run",
                         "kwargs": {"entity_task_id": str(task.id)},
                         "queue": "pipeline"}], app.sent

    session.expire_all()
    stored = session.get(Task, task.id)
    assert stored is not None and stored.status == "Queued"
    assert events_for(session, task.id) == ["TaskQueued"]


def test_the_row_is_committed_before_the_message_is_sent(session: Session) -> None:
    """
    A worker looks the Task up by id, so the row must already be visible when
    the message lands. Checked by reading the row from a second connection at
    the moment of the send.
    """
    engine = session.get_bind()
    seen: dict = {}

    class CheckingApp(RecordingApp):
        def send_task(self, name, kwargs=None, queue=None, **_):
            with Session(engine) as other:
                task_id = uuid.UUID(kwargs["entity_task_id"])
                row = other.get(Task, task_id)
                seen["visible"] = row is not None
                seen["status"] = row.status if row else None
            return super().send_task(name, kwargs, queue)

    submit_task(session, CheckingApp(), celery_task_name="pkg.run", subcategory=SUBCATEGORY,
                name="run-2", payload={})

    assert seen["visible"], "the Task was not committed before the message was sent"
    assert seen["status"] == "Queued"


def test_a_failed_send_leaves_a_recoverable_task(session: Session) -> None:
    """The gap the design accepts: the row survives, the message doesn't."""
    app = RecordingApp(fail=True)
    try:
        submit_task(session, app, celery_task_name="pkg.run", subcategory=SUBCATEGORY,
                    name="run-3", payload={"a": 1})
    except ConnectionError:
        pass
    else:
        raise AssertionError("expected the broker error to propagate")

    session.rollback()
    stranded = pending_submissions(session, subcategory=SUBCATEGORY)
    assert [t.name for t in stranded] == ["run-3"], [t.name for t in stranded]
    assert "celery_task_id" not in stranded[0].attributes


def test_pending_submissions_ignores_sent_tasks(session: Session) -> None:
    app = RecordingApp()
    submit_task(session, app, celery_task_name="pkg.run", subcategory=SUBCATEGORY,
                name="sent", payload={})
    assert pending_submissions(session) == []


def test_submit_propagates_correlation_and_causation(session: Session) -> None:
    correlation = uuid.uuid4()
    cause = uuid.uuid4()
    task = submit_task(session, RecordingApp(), celery_task_name="pkg.run",
                       subcategory=SUBCATEGORY, name="run-4", payload={},
                       correlation_id=correlation, causation_type="event", causation_id=cause)

    event = session.scalars(
        select(EventRecord).where(EventRecord.entity_id == task.id)).one()
    assert task.correlation_id == correlation
    assert event.correlation_id == correlation
    assert event.causation_type == "event" and event.causation_id == cause


def test_an_invalid_status_name_is_rejected_by_the_trigger(session: Session) -> None:
    """TaskStatuses is configurable, but the taxonomy still has the last word."""
    from entitymodel.celery_tasks import TaskStatuses

    try:
        submit_task(session, RecordingApp(), celery_task_name="pkg.run",
                    subcategory=SUBCATEGORY, name="run-5", payload={},
                    statuses=TaskStatuses(queued="NotAStatus"))
    except Exception as exc:
        assert "invalid status" in str(exc), exc
        session.rollback()
    else:
        raise AssertionError("expected the status trigger to reject 'NotAStatus'")


# --------------------------------------------------------------------------
# Executing -- Celery in eager mode, so the task body runs in-process
# --------------------------------------------------------------------------
def _eager_app(name="test") -> Celery:
    app = Celery(name)
    app.conf.update(task_always_eager=True, task_eager_propagates=False, broker_url="memory://")
    return app


def test_a_successful_run_walks_queued_running_succeeded(session: Session) -> None:
    app = _eager_app()
    factory = sessionmaker(session.get_bind())

    @entity_task(app, factory, name="test.ok")
    def run(sess, task, payload):
        return {"output": f"processed {payload['sample']}"}

    task = submit_task(session, RecordingApp(), celery_task_name="test.ok",
                       subcategory=SUBCATEGORY, name="ok-1", payload={"sample": "S1"})
    run.apply(kwargs={"entity_task_id": str(task.id)})

    session.expire_all()
    stored = session.get(Task, task.id)
    assert stored.status == "Succeeded", stored.status
    assert events_for(session, task.id) == ["TaskQueued", "TaskStarted", "TaskSucceeded"]

    succeeded = session.scalars(
        select(EventRecord).where(EventRecord.entity_id == task.id,
                                  EventRecord.event_type == "TaskSucceeded")).one()
    assert succeeded.payload == {"output": "processed S1"}


def test_the_body_receives_the_payload_and_a_usable_session(session: Session) -> None:
    app = _eager_app()
    factory = sessionmaker(session.get_bind())
    seen: dict = {}

    @entity_task(app, factory, name="test.inspect")
    def run(sess, task, payload):
        seen["payload"] = payload
        seen["status_during_run"] = task.status
        seen["can_query"] = sess.scalar(select(Task.name).where(Task.id == task.id))
        return {}

    task = submit_task(session, RecordingApp(), celery_task_name="test.inspect",
                       subcategory=SUBCATEGORY, name="inspect-1", payload={"k": "v"})
    run.apply(kwargs={"entity_task_id": str(task.id)})

    assert seen["payload"] == {"k": "v"}
    assert seen["status_during_run"] == "Running", "the body should see Running, not Queued"
    assert seen["can_query"] == "inspect-1"


def test_a_failing_body_marks_the_task_failed(session: Session) -> None:
    app = _eager_app()
    factory = sessionmaker(session.get_bind())

    @entity_task(app, factory, name="test.boom")
    def run(sess, task, payload):
        raise RuntimeError("pipeline exploded")

    task = submit_task(session, RecordingApp(), celery_task_name="test.boom",
                       subcategory=SUBCATEGORY, name="boom-1", payload={})
    result = run.apply(kwargs={"entity_task_id": str(task.id)})
    assert result.failed()

    session.expire_all()
    stored = session.get(Task, task.id)
    assert stored.status == "Failed", stored.status
    assert events_for(session, task.id) == ["TaskQueued", "TaskStarted", "TaskFailed"]

    failed = session.scalars(
        select(EventRecord).where(EventRecord.entity_id == task.id,
                                  EventRecord.event_type == "TaskFailed")).one()
    assert "RuntimeError: pipeline exploded" in failed.payload["error"]
    assert failed.payload["attempt"] == 1


def test_a_partial_write_by_a_failing_body_is_rolled_back(session: Session) -> None:
    """The body's own writes must not survive a failure, but the Failed status
    must -- they are separate transactions on purpose."""
    app = _eager_app()
    factory = sessionmaker(session.get_bind())

    @entity_task(app, factory, name="test.partial")
    def run(sess, task, payload):
        sess.add(Task(subcategory=SUBCATEGORY, name="side-effect",
                      status="Queued", correlation_id=uuid.uuid4(), attributes={}))
        sess.flush()
        raise RuntimeError("after the write")

    task = submit_task(session, RecordingApp(), celery_task_name="test.partial",
                       subcategory=SUBCATEGORY, name="partial-1", payload={})
    run.apply(kwargs={"entity_task_id": str(task.id)})

    session.expire_all()
    assert session.get(Task, task.id).status == "Failed"
    leaked = session.scalars(select(Task).where(Task.name == "side-effect")).all()
    assert leaked == [], "the failing body's write survived the rollback"


def test_retries_mark_retrying_then_fail_when_exhausted(session: Session) -> None:
    """
    autoretry_for is what makes a retry actually happen; max_retries alone does
    not, which is why the wrapper checks both. In eager mode Celery runs the
    whole retry sequence inside one apply(), so a single call exercises all
    three attempts.
    """
    app = _eager_app()
    factory = sessionmaker(session.get_bind())
    calls: list[int] = []

    @entity_task(app, factory, name="test.retry",
                 autoretry_for=(RuntimeError,), max_retries=2)
    def run(sess, task, payload):
        calls.append(1)
        raise RuntimeError("flaky")

    task = submit_task(session, RecordingApp(), celery_task_name="test.retry",
                       subcategory=SUBCATEGORY, name="retry-1", payload={})
    run.apply(kwargs={"entity_task_id": str(task.id)}, throw=False)

    assert len(calls) == 3, f"expected the initial run plus 2 retries, got {len(calls)}"

    session.expire_all()
    stored = session.get(Task, task.id)
    assert stored.status == "Failed", stored.status
    assert stored.attributes["retry_count"] == 2, stored.attributes

    # The event log carries every attempt, not just the outcome.
    assert events_for(session, task.id) == [
        "TaskQueued",
        "TaskStarted", "TaskRetrying",
        "TaskStarted", "TaskRetrying",
        "TaskStarted", "TaskFailed",
    ]


def test_max_retries_alone_does_not_mark_retrying(session: Session) -> None:
    """
    The bug this guards: max_retries defaults to 3 on every Celery task, so
    treating a remaining budget as proof of a retry marked ordinary failures
    Retrying and stranded them there.
    """
    app = _eager_app()
    factory = sessionmaker(session.get_bind())

    @entity_task(app, factory, name="test.no-autoretry", max_retries=5)
    def run(sess, task, payload):
        raise RuntimeError("no autoretry_for, so Celery will not try again")

    task = submit_task(session, RecordingApp(), celery_task_name="test.no-autoretry",
                       subcategory=SUBCATEGORY, name="no-retry", payload={})
    run.apply(kwargs={"entity_task_id": str(task.id)}, throw=False)

    session.expire_all()
    assert session.get(Task, task.id).status == "Failed", "marked Retrying but nothing retries it"


def test_a_missing_task_row_is_reported_clearly(session: Session) -> None:
    app = _eager_app()
    factory = sessionmaker(session.get_bind())

    @entity_task(app, factory, name="test.missing")
    def run(sess, task, payload):
        return {}

    result = run.apply(kwargs={"entity_task_id": str(uuid.uuid4())}, throw=False)
    assert result.failed()
    assert isinstance(result.result, LookupError), result.result


# --------------------------------------------------------------------------
# Payload files
# --------------------------------------------------------------------------
def test_load_payload_reads_json(session: Session, tmp: Path) -> None:
    path = tmp / "p.json"
    path.write_text(json.dumps({"sample": "SEQ-9", "n": 3}))
    assert load_payload(path) == {"sample": "SEQ-9", "n": 3}


def test_load_payload_rejects_a_missing_file(session: Session, tmp: Path) -> None:
    try:
        load_payload(tmp / "nope.json")
    except SystemExit as exc:
        assert "not found" in str(exc), exc
    else:
        raise AssertionError("expected SystemExit for a missing payload file")


def test_load_payload_rejects_bad_json(session: Session, tmp: Path) -> None:
    path = tmp / "bad.json"
    path.write_text('{"unclosed": ')
    try:
        load_payload(path)
    except SystemExit as exc:
        assert "invalid JSON" in str(exc) and "line" in str(exc), exc
    else:
        raise AssertionError("expected SystemExit for malformed JSON")


def test_load_payload_rejects_a_non_object(session: Session, tmp: Path) -> None:
    path = tmp / "list.json"
    path.write_text("[1, 2, 3]")
    try:
        load_payload(path)
    except SystemExit as exc:
        assert "expected a JSON object" in str(exc), exc
    else:
        raise AssertionError("expected SystemExit for a non-object payload")


# --------------------------------------------------------------------------
TESTS = [
    test_submit_writes_the_task_and_sends_the_message,
    test_the_row_is_committed_before_the_message_is_sent,
    test_a_failed_send_leaves_a_recoverable_task,
    test_pending_submissions_ignores_sent_tasks,
    test_submit_propagates_correlation_and_causation,
    test_an_invalid_status_name_is_rejected_by_the_trigger,
    test_a_successful_run_walks_queued_running_succeeded,
    test_the_body_receives_the_payload_and_a_usable_session,
    test_a_failing_body_marks_the_task_failed,
    test_a_partial_write_by_a_failing_body_is_rolled_back,
    test_retries_mark_retrying_then_fail_when_exhausted,
    test_max_retries_alone_does_not_mark_retrying,
    test_a_missing_task_row_is_reported_clearly,
    test_load_payload_reads_json,
    test_load_payload_rejects_a_missing_file,
    test_load_payload_rejects_bad_json,
    test_load_payload_rejects_a_non_object,
]


def main(keep: bool = False) -> int:
    import inspect
    import tempfile

    engine = make_engine()
    passed, failed = 0, []

    for test in TESTS:
        session = fresh_session(engine)
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                if "tmp" in inspect.signature(test).parameters:
                    test(session, Path(tmpdir))
                else:
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
