"""
Reusable primitives for the entity-event-task model -- the two halves of the
transactional outbox pattern, independent of any particular domain workflow.

    fire_event()        producer side: an entity transition and the event
                        describing it, written in one transaction
    poll_and_dispatch() consumer side: dispatch events a named handler hasn't
                        processed yet, checkpointing each one
    replay()            operational escape hatch: re-run one handler over
                        specific events it has already processed
    dead_lettered()     events a handler has failed max_attempts times and
                        stopped being offered

Those take a Session and none creates or configures one, so they compose with
whatever transaction the caller is already running. On top of them:

    HandlerRegistry     declare which handlers subscribe to which event types
    dispatch_once()     one pass over every registered handler
    listen()            the worker loop -- dispatch_once until told to stop

See demo.py for a worked example and test/ for the behaviour they guarantee.
"""

from __future__ import annotations

import hashlib
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import delete, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from entitymodel.models import Entity, EventHandlerCheckpoint, EventHandlerFailure, EventRecord

# A handler receives the open session and one event, and does its own writes
# on that session. It must not commit -- poll_and_dispatch commits the
# handler's writes together with the checkpoint row, which is what makes the
# pair atomic.
Handler = Callable[[Session, EventRecord], None]

# How many consecutive failures before poll_and_dispatch stops selecting an
# event for a handler. Past this the event is dead-lettered: it sits in
# event_handler_failures until someone looks at it and calls replay().
DEFAULT_MAX_ATTEMPTS = 5

# Exponential backoff between retries: the nth consecutive failure holds the
# event back for BACKOFF_SECONDS * 2**(n-1), capped at MAX_BACKOFF_SECONDS.
# With the defaults that's 30s, 1m, 2m, 4m -- so the five-attempt budget spans
# about eight minutes rather than five consecutive polls.
DEFAULT_BACKOFF_SECONDS = 30.0
DEFAULT_MAX_BACKOFF_SECONDS = 3600.0

# Bound on how much of a traceback we keep in last_error.
_MAX_ERROR_CHARS = 2000

__all__ = [
    "DEFAULT_BACKOFF_SECONDS",
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_MAX_BACKOFF_SECONDS",
    "Handler",
    "HandlerRegistry",
    "Registration",
    "dead_lettered",
    "dispatch_once",
    "fire_event",
    "listen",
    "poll_and_dispatch",
    "replay",
]


# --------------------------------------------------------------------------
# Producer side: entity transition + its own event, one transaction --
# "Transaction 1" from the design discussion. Doesn't commit itself, so it
# composes with whatever else the caller needs in the same transaction.
# --------------------------------------------------------------------------
def fire_event(
    session: Session,
    entity: Entity,
    *,
    event_type: str,
    new_status: str,
    payload: dict,
    source: str,
    actor_type: str,
    actor_id: str | None = None,
    causation_type: str | None = None,
    causation_id: uuid.UUID | None = None,
    occurred_at: datetime | None = None,
) -> EventRecord:
    """
    Move `entity` to `new_status` and record the event describing it, both on
    the caller's transaction. Returns the unflushed EventRecord; the caller
    commits.

    occurred_at is business time -- when the fact actually happened. Pass it
    whenever the producer knows it (a pipeline that finished at 10:00:00 but
    commits at 10:00:04 must record 10:00:00). The fallback is the moment this
    event was constructed, which is still far closer than a server-side now(),
    since now() is transaction start time.

    recorded_at is left to the database (clock_timestamp()). Note that even
    that is insert time, not commit time: a transaction can insert at T and
    commit at T+30s, and rows only become visible at commit. So don't use
    either timestamp as a consumption cursor -- drain on published_at IS NULL
    with FOR UPDATE SKIP LOCKED instead.
    """
    entity.status = new_status
    ev = EventRecord(
        event_type=event_type,
        entity_type=entity.category,
        entity_id=entity.id,
        correlation_id=entity.correlation_id or uuid.uuid4(),
        causation_type=causation_type,
        causation_id=causation_id,
        source=source,
        actor_type=actor_type,
        actor_id=actor_id,
        payload=payload,
        occurred_at=occurred_at or datetime.now(timezone.utc),
    )
    session.add(ev)
    return ev


# --------------------------------------------------------------------------
# Consumer side: poll for events this handler hasn't processed yet (no
# checkpoint row for this handler), dispatch each one, commit the handler's
# writes together with its own checkpoint row.
#
# NOT EXISTS rather than NOT IN: Postgres won't rewrite `NOT IN (subquery)`
# as an anti-join -- NULL semantics block it even though event_id is NOT
# NULL -- so it hashes every checkpoint row for this handler on each poll,
# and degrades to a per-row rescan once that exceeds work_mem. Correlated
# NOT EXISTS plans as a real anti-join against the (handler_name, event_id)
# primary key instead.
#
# The batch_size limit caps how much one poll pulls into memory. Note that
# this still scans all history for these event types to find the new rows;
# bounding that needs a watermark or a claim-based drain (see the
# published_at/publish_attempts columns on EventRecord).
#
# Safe to run from several worker processes at once. This SELECT is only a
# candidate list -- two workers will happily return overlapping rows -- so
# mutual exclusion happens per event at dispatch time, via an advisory lock
# on the (handler_name, event_id) pair. See _dispatch and _lock_key.
# --------------------------------------------------------------------------
def poll_and_dispatch(
    session: Session,
    *,
    handler_name: str,
    event_types: list[str],
    handle: Handler,
    batch_size: int = 100,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
    max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
) -> int:
    """
    Dispatch up to `batch_size` events of `event_types` that `handler_name`
    has neither checkpointed nor dead-lettered, and whose retry backoff (if
    any) has elapsed -- oldest first.

    Returns how many were dispatched *successfully*. A handler that raises
    does not abort the batch: the failure is recorded against that event and
    the loop moves on, so one poison event can't block the queue. Because
    failures aren't counted in the return value, a drain loop -- call until it
    returns 0 -- exits instead of spinning on events that keep failing.

    A failed event is held back for backoff_seconds * 2**(attempts-1), capped
    at max_backoff_seconds, so retries spread out instead of firing on every
    poll.

    Safe to run concurrently from several workers under the same
    handler_name: each event is claimed with an advisory lock before its
    handler runs, and an event another worker already holds is skipped rather
    than waited on, so the batch keeps moving.

    Note the two limits behave differently on a config change. max_attempts is
    compared against the stored counter at selection time, so raising it
    re-offers dead-lettered events immediately. The backoff is baked into
    `next_attempt_at` when the failure is recorded, so changing it only
    affects failures from then on -- an event already held stays held until
    its stored deadline passes.
    """
    already_processed = (
        select(EventHandlerCheckpoint.event_id)
        .where(
            EventHandlerCheckpoint.handler_name == handler_name,
            EventHandlerCheckpoint.event_id == EventRecord.event_id,
        )
        .exists()
    )
    # Second anti-join, same shape as the first: skip events this handler has
    # given up on (attempts exhausted) or is still backing off from. Both
    # conditions live in one subquery so this stays two anti-joins, not three,
    # and both ride the (handler_name, event_id) primary keys.
    not_yet_eligible = (
        select(EventHandlerFailure.event_id)
        .where(
            EventHandlerFailure.handler_name == handler_name,
            EventHandlerFailure.event_id == EventRecord.event_id,
            or_(
                EventHandlerFailure.attempts >= max_attempts,
                # statement_timestamp(), deliberately, and neither of the
                # obvious alternatives:
                #   now()             is transaction start time, and a poll
                #                     that dispatches nothing leaves its
                #                     transaction open -- a long-lived session
                #                     would compare against a stale "now" and
                #                     never release a backed-off event.
                #   clock_timestamp() advances correctly but is VOLATILE, so
                #                     the planner can't flatten this subquery
                #                     and falls back to a per-row SubPlan.
                # statement_timestamp() is STABLE (keeps the anti-join) and
                # still advances on every statement (releases on time).
                EventHandlerFailure.next_attempt_at > func.statement_timestamp(),
            ),
        )
        .exists()
    )
    pending = session.scalars(
        select(EventRecord)
        .where(EventRecord.event_type.in_(event_types))
        .where(~already_processed)
        .where(~not_yet_eligible)
        .order_by(EventRecord.occurred_at)
        .limit(batch_size)
    ).all()

    return _dispatch(
        session, handler_name, pending, handle,
        overwrite_checkpoint=False, record_failures=True, claim="try",
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds, max_backoff_seconds=max_backoff_seconds,
    )


def dead_lettered(
    session: Session,
    *,
    handler_name: str,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> list[EventHandlerFailure]:
    """
    Events this handler has given up on, oldest failure first. These are
    invisible to poll_and_dispatch; recovering one means fixing whatever
    broke and calling replay() with its event_id.
    """
    return list(
        session.scalars(
            select(EventHandlerFailure)
            .where(
                EventHandlerFailure.handler_name == handler_name,
                EventHandlerFailure.attempts >= max_attempts,
            )
            .order_by(EventHandlerFailure.last_failed_at)
        )
    )


# --------------------------------------------------------------------------
# Claiming, so several worker processes can run the same handler safely.
#
# The lock is on the (handler_name, event_id) pair, not on the event row. A
# row lock would be wrong twice over: `events` is shared by every handler, so
# locking a row for one handler would block all the others and destroy the
# fan-out the event log exists to provide; and events are immutable facts,
# not queue entries, so nothing should be taking write locks on them at all.
#
# A transaction-scoped advisory lock has exactly the right lifetime -- it is
# released by the same commit that writes the checkpoint, so the claim and
# the work it protects can never disagree, even if the process is killed.
#
# The key is hashed in Python rather than with hashtext()/hashtextextended(),
# which are undocumented internals. Advisory locks share a database-wide
# namespace, hence the "outbox:" prefix; the one-argument form used here is a
# separate space from the two-argument form, so it can't collide with code
# using pg_advisory_lock(int, int).
# --------------------------------------------------------------------------
def _lock_key(handler_name: str, event_id: uuid.UUID) -> int:
    digest = hashlib.blake2b(
        f"outbox:{handler_name}:{event_id}".encode(), digest_size=8
    ).digest()
    return int.from_bytes(digest, "big", signed=True)


def _settled(session: Session, handler_name: str, event_id: uuid.UUID, max_attempts: int) -> bool:
    """
    Has another worker finished (or given up on) this event since we selected
    it? Must run as its own statement *after* the claim is held: under READ
    COMMITTED each statement takes a fresh snapshot, so folding this into the
    same SELECT as the lock would test a snapshot taken before the lock was
    acquired and reintroduce the race it exists to close.
    """
    return bool(
        session.scalar(
            select(
                select(EventHandlerCheckpoint.event_id)
                .where(
                    EventHandlerCheckpoint.handler_name == handler_name,
                    EventHandlerCheckpoint.event_id == event_id,
                )
                .exists()
                | select(EventHandlerFailure.event_id)
                .where(
                    EventHandlerFailure.handler_name == handler_name,
                    EventHandlerFailure.event_id == event_id,
                    or_(
                        EventHandlerFailure.attempts >= max_attempts,
                        EventHandlerFailure.next_attempt_at > func.statement_timestamp(),
                    ),
                )
                .exists()
            )
        )
    )


# --------------------------------------------------------------------------
# Shared dispatch loop. Each event is its own "Transaction 2": the handler's
# writes and that event's checkpoint commit together, so a crash mid-batch
# leaves earlier events durably processed and the rest untouched.
#
# overwrite_checkpoint distinguishes the two callers. poll_and_dispatch has
# already filtered out anything checkpointed, so a plain INSERT is correct.
# replay() targets rows that usually DO have a checkpoint, so it upserts.
#
# claim likewise: pollers try for the claim and skip an event another worker
# already holds, which is the SKIP LOCKED behaviour applied to the right
# object. replay() waits for it instead -- an operator asked for this event
# specifically, so silently skipping it would be the wrong answer.
# --------------------------------------------------------------------------
def _dispatch(
    session: Session,
    handler_name: str,
    events: Sequence[EventRecord],
    handle: Handler,
    *,
    overwrite_checkpoint: bool,
    record_failures: bool,
    claim: str = "try",  # "try" | "wait"
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
    max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
) -> int:
    succeeded = 0

    for ev in events:
        # Captured before dispatch: a rollback expires `ev`, and reading the
        # attribute afterwards would cost an extra round trip.
        event_id = ev.event_id
        key = _lock_key(handler_name, event_id)

        if claim == "try":
            if not session.scalar(select(func.pg_try_advisory_xact_lock(key))):
                # Another worker holds this (handler, event). It is theirs to
                # finish; we move on rather than block the rest of the batch.
                session.rollback()
                continue
            if _settled(session, handler_name, event_id, max_attempts):
                # They finished it between our SELECT and our claim.
                session.rollback()
                continue
        else:
            session.execute(select(func.pg_advisory_xact_lock(key)))

        try:
            handle(session, ev)
            if overwrite_checkpoint:
                session.execute(
                    pg_insert(EventHandlerCheckpoint)
                    .values(handler_name=handler_name, event_id=event_id)
                    .on_conflict_do_update(
                        index_elements=["handler_name", "event_id"],
                        set_={"processed_at": func.clock_timestamp()},
                    )
                )
            else:
                session.add(
                    EventHandlerCheckpoint(handler_name=handler_name, event_id=event_id)
                )
            # Success clears any failure history, so `attempts` always counts
            # consecutive failures rather than lifetime ones. Same transaction
            # as the checkpoint, so the two can't disagree.
            session.execute(
                delete(EventHandlerFailure).where(
                    EventHandlerFailure.handler_name == handler_name,
                    EventHandlerFailure.event_id == event_id,
                )
            )
            session.commit()
            succeeded += 1
        except Exception as exc:
            # Discard the handler's partial writes. This also leaves any
            # pre-existing checkpoint intact, so a failed dispatch can't make
            # a processed event look unprocessed.
            session.rollback()
            if not record_failures:
                raise
            _record_failure(
                session, handler_name, event_id, exc,
                backoff_seconds=backoff_seconds, max_backoff_seconds=max_backoff_seconds,
            )

    return succeeded


def _backoff_interval(attempts_before: int | object, base: float, cap: float):
    """
    SQL expression for how long to hold an event back after a failure:
    base * 2**(attempts_after - 1), capped at `cap`.

    `attempts_before` is the count *before* this failure -- an int for the
    first failure, or the table column for the ON CONFLICT branch, where it
    refers to the existing row. Computed in SQL rather than Python so the
    delay is derived from the stored counter atomically, and the deadline
    never depends on a worker's clock.
    """
    delay = func.least(base * func.power(2.0, attempts_before), cap)
    return func.clock_timestamp() + func.make_interval(0, 0, 0, 0, 0, 0, delay)


def _record_failure(
    session: Session,
    handler_name: str,
    event_id: uuid.UUID,
    exc: BaseException,
    *,
    backoff_seconds: float,
    max_backoff_seconds: float,
) -> None:
    """Increment this (handler, event)'s consecutive-failure count and push
    out its next-attempt deadline, in its own transaction so it survives
    whatever the handler did."""
    message = f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_CHARS]
    failures = EventHandlerFailure.__table__
    session.execute(
        pg_insert(EventHandlerFailure)
        .values(
            handler_name=handler_name,
            event_id=event_id,
            attempts=1,
            last_error=message,
            next_attempt_at=_backoff_interval(0, backoff_seconds, max_backoff_seconds),
        )
        .on_conflict_do_update(
            index_elements=["handler_name", "event_id"],
            set_={
                "attempts": failures.c.attempts + 1,  # the existing row's value
                "last_error": message,
                "last_failed_at": func.clock_timestamp(),
                "next_attempt_at": _backoff_interval(
                    failures.c.attempts, backoff_seconds, max_backoff_seconds
                ),
            },
        )
    )
    session.commit()


# --------------------------------------------------------------------------
# Targeted replay: re-run one handler over specific events, whether or not it
# has already processed them.
#
# Deliberately not a flag on poll_and_dispatch. "A checkpoint means done" is
# the invariant the whole consumer side rests on, and replay is the one
# operation that overrides it -- worth being a separate, greppable call rather
# than a keyword someone can pass by accident.
#
# Replay re-runs side effects. The checkpoint ledger exists precisely because
# some handlers aren't naturally idempotent, so replaying one may duplicate
# work (or fail against a constraint, which is the lucky case). That is the
# caller's decision to make, so this does not try to guess: it dispatches, and
# lets any handler error propagate.
# --------------------------------------------------------------------------
def replay(
    session: Session,
    *,
    handler_name: str,
    event_ids: Sequence[uuid.UUID],
    handle: Handler,
) -> int:
    """
    Re-dispatch `event_ids` to `handler_name`, oldest first, ignoring whether
    each was already processed or dead-lettered. Returns how many were
    dispatched. This is the recovery path out of the dead-letter state: fix
    whatever broke, then replay the ids that `dead_lettered()` reports.

    Unlike poll_and_dispatch, a handler error propagates rather than being
    counted -- replay is operator-invoked, so the error should surface. A
    successful dispatch clears any failure history for that (handler, event).

    Also unlike poll_and_dispatch, this waits for each event's claim instead
    of skipping it: if a worker is mid-dispatch on the same event, replay
    blocks until that finishes rather than running concurrently with it or
    silently doing nothing.

    Raises ValueError if any id doesn't exist -- during an incident a typo'd
    UUID should say so, not silently do nothing.

    The checkpoint is upserted after each event, not deleted up front, so a
    handler that raises part-way leaves the pre-existing checkpoints in place
    and normal polling unaffected. Events dispatched before the failure stay
    committed and re-checkpointed.
    """
    if not event_ids:
        return 0

    events = session.scalars(
        select(EventRecord)
        .where(EventRecord.event_id.in_(event_ids))
        .order_by(EventRecord.occurred_at)
    ).all()

    missing = set(event_ids) - {ev.event_id for ev in events}
    if missing:
        raise ValueError(f"no such event(s): {sorted(str(m) for m in missing)}")

    return _dispatch(
        session, handler_name, events, handle,
        overwrite_checkpoint=True, record_failures=False, claim="wait",
    )


# --------------------------------------------------------------------------
# Subscriptions: which handler consumes which event types, and with what
# retry settings. One Registration is one durable consumer.
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class Registration:
    """A handler's subscription.

    `name` is the durable identity of this consumer -- it is the handler_name
    written to event_handler_checkpoints, so it decides what counts as
    "already processed". Changing it is not a rename: the new name has no
    checkpoints, so the handler re-processes the entire event history under
    its new identity. Treat it like a table name, not a variable name.
    """

    name: str
    event_types: tuple[str, ...]
    handle: Handler
    batch_size: int = 100
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS
    max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS


class HandlerRegistry:
    """
    The set of handlers a worker process runs.

    Handlers are independent by construction -- each keeps its own checkpoints
    under its own name, so adding one never disturbs the others and a new
    subscriber to an existing event type needs no change to the producer or to
    any sibling handler. That independence is the point of the event log; this
    class is just where it gets declared.

        registry = HandlerRegistry()

        @registry.on("AnalysisTaskSucceeded", name="create-sample-result")
        def create_sample_result(session, ev):
            ...
    """

    def __init__(self) -> None:
        self._registrations: dict[str, Registration] = {}

    def register(
        self,
        *,
        name: str,
        event_types: Sequence[str],
        handle: Handler,
        batch_size: int = 100,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
        max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
    ) -> Registration:
        """
        Add a subscription. `name` must be unique and is required rather than
        derived from the function: two handlers sharing a name would share a
        checkpoint ledger and each would mark the other's events processed,
        and deriving it from `handle.__name__` would turn an ordinary Python
        rename into a silent full replay.
        """
        if not name:
            raise ValueError("handler name is required -- it is the checkpoint key")
        if name in self._registrations:
            raise ValueError(
                f"handler {name!r} is already registered; two handlers sharing a name "
                f"would share checkpoints and hide each other's events"
            )
        if not event_types:
            raise ValueError(f"handler {name!r} subscribes to no event types")

        registration = Registration(
            name=name,
            event_types=tuple(event_types),
            handle=handle,
            batch_size=batch_size,
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
            max_backoff_seconds=max_backoff_seconds,
        )
        self._registrations[name] = registration
        return registration

    def on(self, *event_types: str, name: str, **kwargs) -> Callable[[Handler], Handler]:
        """Decorator form of register(). Returns the function untouched, so
        the handler stays directly callable and testable."""

        def decorate(handle: Handler) -> Handler:
            self.register(name=name, event_types=event_types, handle=handle, **kwargs)
            return handle

        return decorate

    def get(self, name: str) -> Registration:
        return self._registrations[name]

    def names(self) -> list[str]:
        return list(self._registrations)

    def __iter__(self) -> Iterator[Registration]:
        return iter(self._registrations.values())

    def __len__(self) -> int:
        return len(self._registrations)

    def __contains__(self, name: object) -> bool:
        return name in self._registrations


# --------------------------------------------------------------------------
# Running the handlers.
# --------------------------------------------------------------------------
def dispatch_once(session: Session, registry: HandlerRegistry) -> int:
    """
    One pass: poll for every registered handler in turn. Returns the total
    number of events dispatched successfully across all of them.

    Useful on its own for a cron-style consumer, and it's what listen() calls.
    Handler errors are recorded against the event rather than raised (see
    poll_and_dispatch), so one broken handler doesn't stop the others.
    """
    return sum(
        poll_and_dispatch(
            session,
            handler_name=reg.name,
            event_types=list(reg.event_types),
            handle=reg.handle,
            batch_size=reg.batch_size,
            max_attempts=reg.max_attempts,
            backoff_seconds=reg.backoff_seconds,
            max_backoff_seconds=reg.max_backoff_seconds,
        )
        for reg in registry
    )


def listen(
    session_factory: Callable[[], Session],
    registry: HandlerRegistry,
    *,
    poll_interval: float = 1.0,
    stop: threading.Event | None = None,
    max_cycles: int | None = None,
) -> int:
    """
    The worker loop: dispatch_once repeatedly until told to stop. Returns the
    total number of events dispatched.

    A cycle that dispatched something loops again immediately, so a backlog
    drains at full speed; a cycle that found nothing sleeps poll_interval.

    `session_factory` is called once per cycle and the session is closed at
    the end of it -- deliberately, rather than holding one session open for
    the process lifetime. A poll that dispatches nothing still leaves a read
    transaction open, and an idle-in-transaction connection blocks vacuum and
    pins an old snapshot.

    Shutdown: pass a threading.Event as `stop` and set it (the idle sleep
    waits on the event, so shutdown is immediate rather than up to
    poll_interval late). `max_cycles` bounds the loop instead, which is what
    the tests use. KeyboardInterrupt returns cleanly.

    Errors raised by handlers are already recorded per event and never reach
    here. Anything that does reach here is infrastructure -- a dropped
    connection, a missing table -- and is left to propagate, so a supervisor
    restarts the process instead of the loop spinning on a broken database.
    """
    total = 0
    cycles = 0

    try:
        while not (stop is not None and stop.is_set()):
            if max_cycles is not None and cycles >= max_cycles:
                break

            with session_factory() as session:
                dispatched = dispatch_once(session, registry)

            total += dispatched
            cycles += 1

            if dispatched == 0:
                if stop is not None:
                    if stop.wait(poll_interval):
                        break
                else:
                    time.sleep(poll_interval)
    except KeyboardInterrupt:
        pass

    return total
