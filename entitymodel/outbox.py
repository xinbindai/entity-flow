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
import logging
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import delete, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from entitymodel.importing import import_attr
from entitymodel.models import Entity, EventHandlerCheckpoint, EventHandlerFailure, EventRecord

# A handler receives the open session and one event, and does its own writes
# on that session. It must not commit -- poll_and_dispatch commits the
# handler's writes together with the checkpoint row, which is what makes the
# pair atomic.
Handler = Callable[[Session, EventRecord], None]


class HandlerCancelled(Exception):
    """
    Raised by a handler to decline an event for good: it will not be retried.

    A handler that has nothing to do with an event -- a rule that no longer
    applies, a record superseded before the handler reached it, work another
    system already did -- has not failed, and retrying it four more times with
    backoff before dead-lettering it is wasted effort that ends in a warning
    nobody should act on.

    Cancelling settles the event exactly as success does: a checkpoint is
    written, any failure history is cleared, and the handler is never offered
    it again. What differs is only the log line. The handler's own writes are
    rolled back first, on the grounds that a handler which declined an event
    should not leave half its work behind; do the work or decline, not both.

        def handle(session, ev):
            if ev.payload["assay"] != "CGP":
                raise HandlerCancelled("not a CGP order")
            ...

    Subclass it for a reason worth distinguishing in logs; the message is
    otherwise the whole record of why. replay() runs a cancelled event again,
    which is the way back if the decision was wrong.
    """

# Nothing is configured here. A library that calls basicConfig() decides where
# the whole application's logs go, which is not its call to make.
#
# Turning these on takes two steps, and doing only the second is the usual
# mistake -- a record has to pass this logger's level AND reach a handler, and
# setLevel alone gives it nowhere to go:
#
#     import logging
#     logging.basicConfig(                                        # 1. a handler
#         level=logging.INFO,
#         format="%(asctime)s [%(process)d] %(levelname)-5s %(name)s: %(message)s",
#     )
#     logging.getLogger("entitymodel.outbox").setLevel(logging.DEBUG)   # 2. the level
#
# %(process)d is not decoration. Several workers share a log, and it is what lets
# entitymodel.log_search tell one worker's lines from another's when their runs
# interleave; without it that attribution degrades to a guess.
#
# Done that way round the debug detail is limited to this module, rather than
# also turning on every SQL statement SQLAlchemy emits. basicConfig(DEBUG)
# alone works too, and is much noisier.
#
# Messages carry handler_name, event_id and event_type -- all three columns, so
# a line found in the log leads back to a row and a row leads back to its
# lines. The handler call is bracketed by an entering/left pair, which is what
# separates time spent in the handler from time spent committing it. All
# arguments are passed lazily, so a disabled DEBUG level costs one comparison
# rather than a format.
log = logging.getLogger(__name__)

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
    "HandlerCancelled",
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
    trace_id: str | None = None,
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

    trace_id is the ephemeral handle for the inbound call that set all this
    off -- an API request id, or a W3C traceparent trace-id -- and is the one
    field here worth passing on unchanged. A handler reacting to an event
    should forward `ev.trace_id`, so everything one request causes can be
    lined up against that request's logs. It is not a domain fact and will
    outlive nothing: the logs it points at are rotated away long before the
    events are. Keep durable provenance in actor_id and correlation_id.

    Note it is not inherited automatically. fire_event has no reference to the
    causing event -- only its id -- and looking one up behind the caller's
    back to copy a debugging field would be a poor trade.
    """
    entity.status = new_status
    ev = EventRecord(
        event_type=event_type,
        entity_type=entity.category,
        entity_id=entity.id,
        correlation_id=entity.correlation_id or uuid.uuid4(),
        causation_type=causation_type,
        causation_id=causation_id,
        trace_id=trace_id,
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

    Returns how many events were settled: handled successfully, or declined by
    the handler raising HandlerCancelled. A handler that raises anything else
    does not abort the batch -- the failure is recorded against that event and
    the loop moves on, so one poison event can't block the queue. Failures are
    deliberately not counted, so a drain loop -- call until it returns 0 --
    exits instead of spinning on events that keep failing.

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

    # The candidate count is the first thing worth knowing: zero means the
    # query excluded everything -- already checkpointed, dead-lettered, backed
    # off, or simply no event of these types -- rather than the handler
    # failing.
    log.debug(
        "%s: polled %s -> %d candidate(s) (batch_size=%d, max_attempts=%d)",
        handler_name, event_types, len(pending), batch_size, max_attempts,
    )
    # "Nothing was dispatched" is the usual complaint, and the reason lives in
    # the anti-joins above, which exclude rows silently. Break the exclusion
    # down -- but only when the answer will actually be logged, since this is
    # a second query.
    if not pending and log.isEnabledFor(logging.DEBUG):
        log.debug("%s: nothing to do because %s", handler_name,
                  _explain_empty_poll(session, handler_name, event_types, max_attempts))

    return _dispatch(
        session, handler_name, pending, handle,
        overwrite_checkpoint=False, record_failures=True, claim="try",
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds, max_backoff_seconds=max_backoff_seconds,
    )


def _explain_empty_poll(
    session: Session, handler_name: str, event_types: list[str], max_attempts: int
) -> str:
    """
    Why a poll found no candidates: no such events at all, or every one of
    them already accounted for by this handler. Debug-only, so the extra
    round trip is paid only by someone who is watching.
    """
    total = session.scalar(
        select(func.count()).select_from(EventRecord)
        .where(EventRecord.event_type.in_(event_types))
    )
    if not total:
        return f"no events of type {event_types} exist yet"

    processed = session.scalar(
        select(func.count()).select_from(EventHandlerCheckpoint)
        .join(EventRecord, EventRecord.event_id == EventHandlerCheckpoint.event_id)
        .where(
            EventHandlerCheckpoint.handler_name == handler_name,
            EventRecord.event_type.in_(event_types),
        )
    )
    dead, waiting = session.execute(
        select(
            func.count().filter(EventHandlerFailure.attempts >= max_attempts),
            func.count().filter(
                EventHandlerFailure.attempts < max_attempts,
                EventHandlerFailure.next_attempt_at > func.statement_timestamp(),
            ),
        )
        .select_from(EventHandlerFailure)
        .join(EventRecord, EventRecord.event_id == EventHandlerFailure.event_id)
        .where(
            EventHandlerFailure.handler_name == handler_name,
            EventRecord.event_type.in_(event_types),
        )
    ).one()

    return (
        f"of {total} event(s) of type {event_types}: {processed} already processed, "
        f"{dead} dead-lettered, {waiting} backing off"
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


def _settled(
    session: Session, handler_name: str, event_id: uuid.UUID, max_attempts: int
) -> str | None:
    """
    Why this event should be skipped now, or None to go ahead.

    Returns a reason rather than a bool so the debug log can answer the
    question people actually ask when a handler seems not to run -- "why was
    it skipped" -- without a second round trip to find out.

    Must run as its own statement *after* the claim is held: under READ
    COMMITTED each statement takes a fresh snapshot, so folding this into the
    same SELECT as the lock would test a snapshot taken before the lock was
    acquired and reintroduce the race it exists to close.
    """
    mine = (
        EventHandlerFailure.handler_name == handler_name,
        EventHandlerFailure.event_id == event_id,
    )
    row = session.execute(
        select(
            select(EventHandlerCheckpoint.event_id)
            .where(
                EventHandlerCheckpoint.handler_name == handler_name,
                EventHandlerCheckpoint.event_id == event_id,
            )
            .exists()
            .label("processed"),
            select(EventHandlerFailure.attempts).where(*mine).scalar_subquery().label("attempts"),
            select(EventHandlerFailure.next_attempt_at)
            .where(*mine)
            .scalar_subquery()
            .label("next_attempt_at"),
            func.statement_timestamp().label("now"),
        )
    ).one()

    if row.processed:
        return "already processed by this handler"
    if row.attempts is not None and row.attempts >= max_attempts:
        return f"dead-lettered after {row.attempts} attempt(s)"
    if row.next_attempt_at is not None and row.next_attempt_at > row.now:
        return f"backing off until {row.next_attempt_at.isoformat()}"
    return None


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
    settled = 0  # cancelled by the handler: finished with, but no work done

    log.debug("%s: %d event(s) to dispatch", handler_name, len(events))

    for ev in events:
        # Captured before dispatch: a rollback expires `ev`, and reading the
        # attribute afterwards would cost an extra round trip.
        event_id = ev.event_id
        key = _lock_key(handler_name, event_id)

        if claim == "try":
            if not session.scalar(select(func.pg_try_advisory_xact_lock(key))):
                # Another worker holds this (handler, event). It is theirs to
                # finish; we move on rather than block the rest of the batch.
                log.debug(
                    "%s: skipping event %s -- claimed by another worker",
                    handler_name, event_id,
                )
                session.rollback()
                continue
            reason = _settled(session, handler_name, event_id, max_attempts)
            if reason is not None:
                # Something changed between our SELECT and our claim.
                log.debug("%s: skipping event %s -- %s", handler_name, event_id, reason)
                session.rollback()
                continue
        else:
            session.execute(select(func.pg_advisory_xact_lock(key)))

        # event_type is read before the call: a handler that raises rolls the
        # session back, which expires `ev`, and the exit line would otherwise
        # pay for a reload just to name the type it already knew.
        event_type = ev.event_type

        try:
            log.debug(
                "%s: dispatching event %s (%s) -- entering handler",
                handler_name, event_id, event_type,
            )
            started = time.monotonic()
            try:
                handle(session, ev)
            finally:
                # Both ends name handler_name, event_id and event_type -- all
                # three are columns, so a line found in the log leads back to a
                # row and a row leads back to its lines. In a finally so the
                # pair is always balanced: an "entering" with no "left" means
                # the process died inside the handler, which is a different
                # thing from it raising, and the next line says which.
                log.debug(
                    "%s: left handler for event %s (%s) after %.1f ms",
                    handler_name, event_id, event_type,
                    (time.monotonic() - started) * 1000,
                )
            _settle(session, handler_name, event_id, overwrite=overwrite_checkpoint)
            session.commit()
            succeeded += 1
            log.debug(
                "%s: event %s (%s) handled and checkpointed",
                handler_name, event_id, event_type,
            )
        except HandlerCancelled as exc:
            # Not a failure: the handler decided this event is not its
            # business. Roll its partial work back, then settle the event so
            # nothing offers it again.
            session.rollback()
            _settle(
                session, handler_name, event_id, overwrite=overwrite_checkpoint,
                cancelled_reason=(str(exc) or type(exc).__name__)[:_MAX_ERROR_CHARS],
            )
            session.commit()
            settled += 1
            log.info(
                "%s: event %s (%s) cancelled by the handler -- %s. It will not be retried",
                handler_name, event_id, event_type, exc or "no reason given",
            )
        except Exception as exc:
            # Discard the handler's partial writes. This also leaves any
            # pre-existing checkpoint intact, so a failed dispatch can't make
            # a processed event look unprocessed.
            session.rollback()
            log.debug(
                "%s: event %s (%s) raised %s: %s",
                handler_name, event_id, event_type, type(exc).__name__, exc,
            )
            if not record_failures:
                raise
            _record_failure(
                session, handler_name, event_id, exc,
                max_attempts=max_attempts,
                backoff_seconds=backoff_seconds, max_backoff_seconds=max_backoff_seconds,
            )

    log.debug(
        "%s: %d of %d event(s) succeeded, %d cancelled",
        handler_name, succeeded, len(events), settled,
    )
    # Cancelled events count towards the return value even though no work was
    # done. The number drives the drain loop, which needs to know whether the
    # pass made progress, and settling events it will never see again is
    # progress.
    return succeeded + settled


def _settle(
    session: Session,
    handler_name: str,
    event_id: uuid.UUID,
    *,
    overwrite: bool,
    cancelled_reason: str | None = None,
) -> None:
    """
    Mark this (handler, event) finished: checkpoint it and drop any failure
    history, so `attempts` always counts *consecutive* failures. Both writes
    go in the caller's transaction, so the two can never disagree.

    cancelled_reason is None when the handler did the work and set when it
    declined. On the upsert path it is written unconditionally rather than
    only when set, so replaying a cancelled event and succeeding clears the
    old reason instead of leaving a checkpoint that claims to be both.

    See poll_and_dispatch for why the plain INSERT is right there and the
    upsert is right for replay.
    """
    if overwrite:
        session.execute(
            pg_insert(EventHandlerCheckpoint)
            .values(
                handler_name=handler_name,
                event_id=event_id,
                cancelled_reason=cancelled_reason,
            )
            .on_conflict_do_update(
                index_elements=["handler_name", "event_id"],
                set_={
                    "processed_at": func.clock_timestamp(),
                    "cancelled_reason": cancelled_reason,
                },
            )
        )
    else:
        session.add(
            EventHandlerCheckpoint(
                handler_name=handler_name,
                event_id=event_id,
                cancelled_reason=cancelled_reason,
            )
        )

    session.execute(
        delete(EventHandlerFailure).where(
            EventHandlerFailure.handler_name == handler_name,
            EventHandlerFailure.event_id == event_id,
        )
    )


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
    max_attempts: int,
    backoff_seconds: float,
    max_backoff_seconds: float,
) -> None:
    """Increment this (handler, event)'s consecutive-failure count and push
    out its next-attempt deadline, in its own transaction so it survives
    whatever the handler did."""
    message = f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_CHARS]
    failures = EventHandlerFailure.__table__
    stmt = (
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
        .returning(EventHandlerFailure.attempts, EventHandlerFailure.next_attempt_at)
    )
    attempts, next_attempt_at = session.execute(stmt).one()
    session.commit()

    if attempts >= max_attempts:
        # Not debug. An event nobody will retry, dropped without anyone being
        # told, is the failure mode that costs the most to discover late.
        log.warning(
            "%s: event %s dead-lettered after %d attempt(s); "
            "last error %s. It will not be retried -- see dead_lettered() and replay()",
            handler_name, event_id, attempts, message,
        )
    else:
        log.debug(
            "%s: event %s failed (attempt %d of %d), next attempt at %s",
            handler_name, event_id, attempts, max_attempts,
            next_attempt_at.isoformat() if next_attempt_at else "unknown",
        )


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

    HandlerCancelled is the exception to that: a handler declining an event is
    an answer, not an error, so it is settled and logged here exactly as it
    would be during a poll rather than raised at the operator.

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

    log.debug("%s: replaying %d event(s): %s",
              handler_name, len(events), [str(ev.event_id) for ev in events])

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
    # The dotted path this handler was named by, when it came from
    # configuration rather than a direct reference. Kept so a status dump or
    # a log line can say which code a subscription resolved to.
    handle_ref: str | None = None
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
        handle: Handler | str,
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

        `handle` may be the callable itself, or a dotted path naming it
        ("myapp.handlers:create_sample_result"), so a deployment can list its
        subscriptions in configuration. A path is resolved here, at
        registration, not at first dispatch -- a typo should stop the worker
        starting rather than surface hours later when a matching event finally
        arrives, by which time it looks like an event problem.
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

        handle_ref = handle if isinstance(handle, str) else None
        if handle_ref is not None:
            try:
                handle = import_attr(handle_ref)
            except (ValueError, ImportError, AttributeError) as exc:
                raise ValueError(f"handler {name!r}: {exc}") from exc
        if not callable(handle):
            what = handle_ref or type(handle).__name__
            raise ValueError(f"handler {name!r}: {what} is not callable")

        registration = Registration(
            name=name,
            event_types=tuple(event_types),
            handle=handle,
            handle_ref=handle_ref,
            batch_size=batch_size,
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
            max_backoff_seconds=max_backoff_seconds,
        )
        self._registrations[name] = registration
        log.debug(
            "registered handler %r for %s -> %s",
            name, list(registration.event_types),
            handle_ref or getattr(handle, "__qualname__", repr(handle)),
        )
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
    total = 0
    for reg in registry:
        total += poll_and_dispatch(
            session,
            handler_name=reg.name,
            event_types=list(reg.event_types),
            handle=reg.handle,
            batch_size=reg.batch_size,
            max_attempts=reg.max_attempts,
            backoff_seconds=reg.backoff_seconds,
            max_backoff_seconds=reg.max_backoff_seconds,
        )

    log.debug("dispatch pass over %d handler(s) dispatched %d event(s)", len(registry), total)
    return total


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
    log.debug(
        "listening with %d handler(s): %s (poll_interval=%.3gs)",
        len(registry), registry.names(), poll_interval,
    )

    try:
        while not (stop is not None and stop.is_set()):
            if max_cycles is not None and cycles >= max_cycles:
                break

            with session_factory() as session:
                dispatched = dispatch_once(session, registry)

            total += dispatched
            cycles += 1

            if dispatched == 0:
                log.debug("cycle %d idle, sleeping %.3gs", cycles, poll_interval)
                if stop is not None:
                    if stop.wait(poll_interval):
                        break
                else:
                    time.sleep(poll_interval)
    except KeyboardInterrupt:
        pass

    return total
