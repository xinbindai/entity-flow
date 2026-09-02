"""
Launch Celery tasks that are also Entities, and keep the two in step.

A Task row is the durable record of a unit of work; the Celery message is only
the means of getting it executed. Submitting writes the row and its event
first, then sends the message:

    task = submit_task(session, celery_app,
                       celery_task_name="myapp.run_pipeline",
                       subcategory="bioinformatics_pipeline_analysis",
                       name="run-2026-08-02", payload={"sample": "SEQ-001"},
                       channel="lab-a")

and the worker side wraps the body so the row follows the execution:

    @entity_task(celery_app, session_factory, name="myapp.run_pipeline")
    def run_pipeline(session, task, payload):
        ...
        return {"output": "s3://..."}

    Queued --> Running --> Succeeded
                       \\-> Retrying --> Running
                       \\-> Failed

Each transition goes through fire_event, so the event log carries the whole
history and every downstream handler sees it, exactly as with any other
entity.

The order of writes matters. The row and the TaskQueued event commit before
the Celery message is sent, so a worker can never receive an id that isn't in
the database yet. The cost is the opposite gap: a crash between the commit and
the send leaves a Task in Queued that no worker will ever pick up. Those are
recoverable rather than lost -- pending_submissions() finds them, because a
Task that was never sent has no celery_task_id in its attributes.

    python -m entitymodel.celery_tasks \\
        --app myapp.celery:app --db-url postgresql+psycopg2://localhost/lab \\
        --task myapp.run_pipeline --subcategory bioinformatics_pipeline_analysis \\
        --name run-2026-08-02 --payload payload.json --queue pipeline
"""

from __future__ import annotations

import argparse
import functools
import json
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from entitymodel.importing import import_attr
from entitymodel.models import DEFAULT_CHANNEL, Task
from entitymodel.outbox import fire_event

__all__ = [
    "DEFAULT_STATUSES",
    "TaskStatuses",
    "entity_task",
    "load_payload",
    "pending_submissions",
    "submit_task",
]


@dataclass(frozen=True)
class TaskStatuses:
    """
    The status names this deployment uses for a Task. They must exist in
    entity_status for the subcategory, or the validation trigger rejects the
    transition -- these defaults match the ones in entity_statuses.csv.
    """

    queued: str = "Queued"
    running: str = "Running"
    succeeded: str = "Succeeded"
    failed: str = "Failed"
    retrying: str = "Retrying"
    cancelled: str = "Cancelled"


DEFAULT_STATUSES = TaskStatuses()


def load_payload(path: str | Path) -> dict:
    """Read a JSON payload file, failing with the path rather than a bare
    JSONDecodeError -- these come from a command line and are usually typos."""
    path = Path(path)
    if not path.exists():
        raise SystemExit(f"payload file not found: {path}")
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{path}: invalid JSON at line {exc.lineno}: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"{path}: expected a JSON object, got {type(payload).__name__}")
    return payload


# --------------------------------------------------------------------------
# Producer side
# --------------------------------------------------------------------------
def submit_task(
    session: Session,
    celery_app,
    *,
    celery_task_name: str,
    subcategory: str,
    name: str,
    payload: dict,
    channel: str = DEFAULT_CHANNEL,
    queue: str | None = None,
    source: str = "cli",
    actor_type: str = "user",
    actor_id: str | None = None,
    correlation_id: uuid.UUID | None = None,
    causation_type: str | None = None,
    causation_id: uuid.UUID | None = None,
    trace_id: str | None = None,
    statuses: TaskStatuses = DEFAULT_STATUSES,
    event_type: str = "TaskQueued",
) -> Task:
    """
    Record the Task and emit its queued event in one transaction, commit, then
    send the Celery message. Returns the committed Task.

    Committing first is deliberate: the worker looks the Task up by id, so the
    row has to be visible before the message can be picked up. See the module
    docstring for the gap this leaves and how to recover from it.
    """
    task = Task(
        subcategory=subcategory,
        name=name,
        status=statuses.queued,
        correlation_id=correlation_id or uuid.uuid4(),
        # trace_id is kept on the Task as well as on the event so the worker
        # can carry it into TaskStarted/Succeeded/Failed without a lookup. The
        # worker runs in a different process minutes later; the alternative is
        # a query per transition to find the event that queued it.
        # channel is kept on the Task for the same reason trace_id is: the
        # worker runs in another process minutes later and has to stamp
        # TaskStarted/Succeeded/Failed into the same channel the submission
        # went to. The alternative is a query per transition to find the event
        # that queued it.
        attributes={
            "payload": payload,
            "retry_count": 0,
            "trace_id": trace_id,
            "channel": channel,
        },
    )
    session.add(task)
    session.flush()  # need task.id before the event can reference it

    fire_event(
        session,
        task,
        event_type=event_type,
        new_status=statuses.queued,
        payload={"celery_task_name": celery_task_name, "queue": queue},
        channel=channel,
        source=source,
        actor_type=actor_type,
        actor_id=actor_id,
        causation_type=causation_type,
        causation_id=causation_id,
        trace_id=trace_id,
    )
    session.commit()

    async_result = celery_app.send_task(
        celery_task_name, kwargs={"entity_task_id": str(task.id)}, queue=queue
    )

    # Recorded after the send, so its absence is exactly the signal that the
    # send did not happen. Written with a dict copy because SQLAlchemy tracks
    # JSONB by identity and would not notice an in-place mutation.
    task.attributes = {**task.attributes, "celery_task_id": async_result.id}
    session.commit()
    return task


def pending_submissions(
    session: Session, *, subcategory: str | None = None, statuses: TaskStatuses = DEFAULT_STATUSES
) -> list[Task]:
    """
    Tasks stuck between the commit and the send: queued, but with no Celery
    message behind them. Re-send these after a crash, or alert on them.
    """
    query = select(Task).where(Task.status == statuses.queued)
    if subcategory is not None:
        query = query.where(Task.subcategory == subcategory)
    return [
        task
        for task in session.scalars(query)
        if not task.attributes.get("celery_task_id")
    ]


# --------------------------------------------------------------------------
# Worker side
# --------------------------------------------------------------------------
def entity_task(
    celery_app,
    session_factory,
    *,
    name: str | None = None,
    statuses: TaskStatuses = DEFAULT_STATUSES,
    **celery_options,
):
    """
    Register a Celery task that drives an Entity Task through its lifecycle.

    The wrapped function is called as fn(session, task, payload) inside a
    session of its own -- the worker is a different process from the
    submitter, so it cannot share one -- with the Task already moved to
    Running. Its return value, if a dict, is merged into the succeeded event's
    payload.

    On an exception the Task goes to Retrying if Celery will try again, or
    Failed if it will not, and the exception is re-raised so Celery's own
    retry and result handling still apply. The status is committed before
    re-raising, so a task that dies mid-flight does not leave a row claiming
    to be Running forever.
    """

    def decorate(fn):
        @celery_app.task(bind=True, name=name or f"{fn.__module__}.{fn.__name__}", **celery_options)
        @functools.wraps(fn)
        def wrapper(self, entity_task_id: str):
            task_id = uuid.UUID(entity_task_id)
            with session_factory() as session:
                task = session.get(Task, task_id)
                if task is None:
                    raise LookupError(f"no Task entity {entity_task_id}")

                payload = task.attributes.get("payload", {})
                _transition(session, task, statuses.running, "TaskStarted", {})

                try:
                    result = fn(session, task, payload)
                except Exception as exc:
                    will_retry = _will_retry(self, exc)
                    session.rollback()
                    task = session.get(Task, task_id)
                    _transition(
                        session,
                        task,
                        statuses.retrying if will_retry else statuses.failed,
                        "TaskRetrying" if will_retry else "TaskFailed",
                        {"error": f"{type(exc).__name__}: {exc}"[:2000],
                         "attempt": (self.request.retries or 0) + 1},
                        bump_retry_count=will_retry,
                    )
                    raise

                _transition(
                    session, task, statuses.succeeded, "TaskSucceeded",
                    result if isinstance(result, dict) else {"result": result},
                )
                return result

        return wrapper

    return decorate


def _will_retry(celery_task, exc: BaseException) -> bool:
    """
    Will Celery actually run this task again?

    Not the same question as "are there retries left". max_retries defaults to
    3 on every task, but Celery only retries a plain exception when
    autoretry_for names it, or when something raises Retry by calling
    self.retry(). Treating a leftover retry budget as proof of a retry marks
    ordinary failures as Retrying and leaves them there for good.
    """
    from celery.exceptions import Retry

    if isinstance(exc, Retry):
        return True

    autoretry_for = tuple(getattr(celery_task, "autoretry_for", ()) or ())
    if not autoretry_for or not isinstance(exc, autoretry_for):
        return False

    retries = celery_task.request.retries or 0
    max_retries = celery_task.max_retries
    return max_retries is not None and retries < max_retries


def _transition(
    session: Session,
    task: Task,
    status: str,
    event_type: str,
    payload: dict,
    *,
    bump_retry_count: bool = False,
) -> None:
    """One status change plus its event, committed together."""
    if bump_retry_count:
        task.attributes = {
            **task.attributes,
            "retry_count": task.attributes.get("retry_count", 0) + 1,
        }
    fire_event(
        session,
        task,
        event_type=event_type,
        new_status=status,
        payload=payload,
        source="celery-worker",
        actor_type="worker",
        # Carried from submission, so a task's whole lifecycle lands in the
        # channel that asked for it rather than leaking into the default.
        channel=task.attributes.get("channel", DEFAULT_CHANNEL),
        # Carried from submission, so the whole execution lines up against the
        # request that asked for it.
        trace_id=task.attributes.get("trace_id"),
    )
    session.commit()


# --------------------------------------------------------------------------
# Command line
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m entitymodel.celery_tasks",
        description="Submit a Celery task and record it in the entity table.",
    )
    parser.add_argument("--app", required=True, help="Celery app, as module:attr")
    parser.add_argument("--db-url", required=True, help="SQLAlchemy database URL")
    parser.add_argument("--task", required=True, help="registered Celery task name")
    parser.add_argument("--subcategory", required=True, help="Task subcategory in the taxonomy")
    parser.add_argument("--name", required=True, help="name for the Task entity")
    parser.add_argument("--payload", required=True, help="path to a JSON payload file")
    parser.add_argument("--queue", default=None, help="queue to route to")
    parser.add_argument(
        "--channel", default=DEFAULT_CHANNEL,
        help="channel to publish this task's events into",
    )
    parser.add_argument("--source", default="cli")
    parser.add_argument("--actor-type", default="user")
    parser.add_argument("--actor-id", default=None)
    args = parser.parse_args(argv)

    from sqlalchemy import create_engine

    try:
        celery_app = import_attr(args.app)
    except (ValueError, ImportError, AttributeError) as exc:
        raise SystemExit(str(exc)) from exc
    payload = load_payload(args.payload)
    engine = create_engine(args.db_url)

    with Session(engine) as session:
        task = submit_task(
            session,
            celery_app,
            celery_task_name=args.task,
            subcategory=args.subcategory,
            name=args.name,
            payload=payload,
            channel=args.channel,
            queue=args.queue,
            source=args.source,
            actor_type=args.actor_type,
            actor_id=args.actor_id,
        )
        print(f"task entity   {task.id}")
        print(f"celery task   {task.attributes.get('celery_task_id')}")
        print(f"status        {task.status}")
    engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
