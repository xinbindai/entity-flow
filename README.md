# entity-flow

An entity–event–task model for a clinical sequencing lab, on PostgreSQL and SQLAlchemy 2.0.

State lives in one polymorphic `entity` table, facts live in an append-only event log, and
work (tasks) is just another kind of entity. Entities and tasks never call each other — the
event log is the only channel between them. Every state change and the event describing it
are written in one transaction (the **transactional outbox** pattern), and consumers track
what they've handled per-handler, so redelivery and replay are safe.

The full design, including the alternative schema that was evaluated and rejected, is in
[entity-event-task-architecture.md](entity-event-task-architecture.md).

## Layout

| Path | What |
|---|---|
| `entitymodel/` | The reusable half — schema and outbox machinery, no domain knowledge. This is what gets packaged. |
| `entitymodel/models.py` | SQLAlchemy models: `Entity` and its polymorphic subclasses, the event log, checkpoint and failure tables |
| `entitymodel/outbox.py` | `fire_event`, `poll_and_dispatch`, `replay`, `dead_lettered`, `HandlerRegistry`, `listen` |
| `entitymodel/*.sql` | The same schema as reference DDL, with the design rationale in comments |
| `taxonomy.py` | **This** lab's entity types and their state machines. Another deployment replaces this file. |
| `demo.py` | A worked example: one handler, a scripted walkthrough, and a runnable worker |
| `test/` | Seven suites, 74 tests, run against a real PostgreSQL |

## Setup

Needs PostgreSQL — the model uses `JSONB`, `gen_random_uuid()` and a PL/pgSQL trigger, so
SQLite won't do.

```bash
uv sync
cp .env.example .env      # then set POSTGRES_URL
```

`.env` is only read by the test suite; `demo.py` and `taxonomy.py` take the URL as an
argument.

## Quick start

Against an empty database, `demo.py` builds the schema, seeds the taxonomy, produces one
event and consumes it twice — the second poll showing that a handled event is not handled
again:

```console
$ python demo.py postgresql+psycopg2://localhost/lab_demo
Producer: task succeeds, firing AnalysisTaskSucceeded

Consumer: first poll
  -> created Result 'result-SEQ-001' (id=afc80f17-…), emitted SampleResultCreated
  processed 1 event(s)

Consumer: second poll (idempotency check)
  processed 0 event(s) -- already-handled event correctly skipped

Event log:
  AnalysisTaskSucceeded  entity=Task     causation_id=None
  SampleResultCreated    entity=Result   causation_id=29532bd6-…
```

Note the second event's `causation_id`: the Result was created *because* of the first event,
and the log records that link. That chain is what makes a patient-to-result journey
reconstructable after the fact.

> `demo.py` drops and recreates every table. Point it at a scratch database.

## Writing a handler

A handler subscribes to event types and does its work on the session it is given. It must not
commit — `poll_and_dispatch` commits the handler's writes together with the checkpoint that
records the event as handled, and that is what makes the pair atomic.

```python
from entitymodel.outbox import HandlerRegistry, fire_event

registry = HandlerRegistry()

@registry.on("AnalysisTaskSucceeded", name="create-sample-result")
def create_sample_result(session, ev):
    result = Result(...)
    session.add(result)
    session.flush()
    fire_event(session, result, event_type="SampleResultCreated", ...)
```

`name` is the handler's durable identity — it is the `handler_name` stored in
`event_handler_checkpoints`, so renaming it makes the handler re-process every event it has
ever seen. Treat it like a table name, not a variable name.

Adding a second handler for the same event type is one more `@registry.on` and nothing else:
no change to the producer, none to the existing handler. Each keeps its own checkpoints.

## Running a worker

```bash
python taxonomy.py postgresql+psycopg2://localhost/lab   # once: create schema + seed taxonomy
python demo.py     postgresql+psycopg2://localhost/lab --worker
```

`run_worker` in [demo.py](demo.py) is the shape you would deploy: a session factory, the
registry, and `listen()` looping until SIGTERM. Several copies can run against the same
database — each event is claimed before its handler runs, so workers skip what another is
already doing rather than duplicating it.

Handlers that raise are retried with exponential backoff (30s, 1m, 2m, 4m…) and
dead-lettered after five consecutive failures, rather than blocking the queue or spinning.
`dead_lettered()` lists what a handler has given up on; `replay()` re-runs it once the cause
is fixed.

## Tests

```bash
for f in test/test_*.py; do python "$f"; done
```

```
test/test_concurrency.py          5 passed
test/test_dispatch_retry.py      16 passed
test/test_entity_subclasses.py    7 passed
test/test_event_timing.py        14 passed
test/test_poll_and_dispatch.py    8 passed
test/test_registry.py            15 passed
test/test_replay.py               9 passed
```

Each suite creates a dedicated `poll_test` schema, runs, and drops it, so nothing else in the
database is touched. They are plain scripts using `assert`, so pytest is not required — but
they are pytest-shaped, so `pytest test/` works if you add it.

## Known gaps

- **No migrations.** `create_all()` builds a correct schema from scratch but never issues
  `ALTER`, so it cannot upgrade an existing database. Worth setting up Alembic before the
  next schema change.
