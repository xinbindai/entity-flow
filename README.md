# entity-flow

An entity–event–task model for a clinical sequencing lab, on PostgreSQL and SQLAlchemy 2.0.

State lives in one polymorphic `entity` table, facts live in an append-only event log, and
work (tasks) is just another kind of entity. Entities and tasks never call each other — the
event log is the only channel between them. Every state change and the event describing it
are written in one transaction (the **transactional outbox** pattern), and consumers track
what they've handled per-handler, so redelivery and replay are safe.

The full design, including the alternative schema that was evaluated and rejected, is in
[entity-event-task-architecture.md](https://github.com/xinbindai/entity-flow/blob/main/entity-event-task-architecture.md).

## Layout

| Path | What |
|---|---|
| `entitymodel/` | The reusable half — schema and outbox machinery, no domain knowledge. This is what gets packaged. |
| `entitymodel/models.py` | SQLAlchemy models: `Entity`, `Task`, the event log, checkpoint and failure tables |
| `entitymodel/outbox.py` | `fire_event`, `poll_and_dispatch`, `replay`, `dead_lettered`, `HandlerRegistry`, `listen` |
| `entitymodel/taxonomy_sync.py` | Load a taxonomy from CSV and reconcile the database with it |
| `entitymodel/celery_tasks.py` | Submit Celery tasks that are also `Task` entities, and drive the row through its lifecycle |
| `entitymodel/celery_workers.py` | Start, stop and supervise a Celery worker fleet from a config dict |
| `entitymodel/*.sql` | The schema as reference DDL, with the design rationale in comments. Tables only — the taxonomy rows live in the CSVs |
| `taxonomy.py` | **This** lab's categories — the taxonomy CSVs plus the `Patient`/`Sample`/… subclasses. Another deployment replaces this file. |
| `entity_types.csv`, `entity_statuses.csv` | The taxonomy itself — editable without touching Python |
| `demo.py` | A worked example: one handler, a scripted walkthrough, and a runnable worker |
| `test/` | Ten suites, 123 tests, run against a real PostgreSQL |
| `migrations/` | Alembic revisions; `alembic.ini` at the root |

## Setup

Needs PostgreSQL — the model uses `JSONB`, `gen_random_uuid()` and a PL/pgSQL trigger, so
SQLite won't do.

The `psycopg2` dependency is the source distribution, not `psycopg2-binary`, so the client
library and a compiler have to be present before installing — the binary wheel bundles its
own libpq and OpenSSL, which is not a choice a library should make for its consumers:

```bash
sudo apt install libpq-dev python3-dev build-essential   # Debian/Ubuntu
brew install postgresql                                  # macOS
```

Without them the install fails with `Error: pg_config executable not found`. If you would
rather not build from source, install `psycopg2-binary` yourself — it provides the same
`psycopg2` module, so nothing else changes.

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

`run_worker` in [demo.py](https://github.com/xinbindai/entity-flow/blob/main/demo.py) is the shape you would deploy: a session factory, the
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
test/test_taxonomy_sync.py       17 passed
test/test_celery_tasks.py        17 passed
test/test_celery_workers.py      15 passed
```

Each suite creates a dedicated `poll_test` schema, runs, and drops it, so nothing else in the
database is touched. They are plain scripts using `assert`, so pytest is not required — but
they are pytest-shaped, so `pytest test/` works if you add it.

## Using it from another project

`entitymodel` ships the schema and the outbox machinery, with no domain knowledge. `Entity`
and `Task` are model concepts and live in the package; everything else is vocabulary you
supply.

```python
from entitymodel.models import Base, Entity
from entitymodel.outbox import HandlerRegistry, dispatch_once, fire_event
from entitymodel.taxonomy_sync import sync_taxonomy_from_csv

class Ticket(Entity):
    __mapper_args__ = {"polymorphic_identity": "Ticket"}
```

Declaring a subclass per category is optional but usually wanted: `category` is the
polymorphic discriminator, so a category with no subclass loads as a plain `Entity` — and
constructing `Entity(category="Ticket", ...)` instead of `Ticket(...)` produces a
`SAWarning` and a row that won't load back as your type. The `polymorphic_identity` must
equal the category in your CSV exactly, and the module declaring it has to be imported
before rows of that category are loaded.

`taxonomy.py` is this repo's worked example of that file.

## The taxonomy

Which `(category, subcategory)` pairs exist and what statuses each may hold is **data, not
schema** — a new entity subcategory is an `INSERT`, not a migration. It lives in two CSVs at
the root, so a domain expert can edit it without touching Python:

```
entity_types.csv      category,subcategory,description
entity_statuses.csv   category,subcategory,status,is_terminal
```

Edit them, then reconcile the database:

```console
$ python taxonomy.py postgresql+psycopg2://localhost/lab --dry-run
types +1 ~1 -0; statuses +2 ~1 -0

$ python taxonomy.py postgresql+psycopg2://localhost/lab --sync
types +1 ~1 -0; statuses +2 ~1 -0

$ python taxonomy.py postgresql+psycopg2://localhost/lab --sync
taxonomy already matches the CSVs
```

New rows are inserted, changed descriptions and `is_terminal` flags are updated, and rows the
CSVs no longer mention are **reported but not deleted**. Deleting is opt-in
(`--delete-missing`) and refuses outright if live entities still use the row — nothing
references `entity_status` by foreign key, so an unchecked delete would succeed and then
strand those entities, unable to be updated because the trigger rejects the status they
already hold.

The loader also refuses a subcategory with no statuses, which is impossible to insert rather
than merely empty, and a status naming a subcategory the types file doesn't declare.

`entitymodel.taxonomy_sync` is the reusable half — `sync_taxonomy_from_csv(session, types,
statuses)` works for any deployment.


## Running tasks on Celery

`Task` is already an entity, so a Celery job and its durable record are the same thing. The
row is written first and the message sent second, so a worker can never receive an id that
isn't in the database yet.

Submit from the command line, payload in a JSON file:

```bash
python -m entitymodel.celery_tasks \
    --app myapp.celery:app --db-url postgresql+psycopg2://localhost/lab \
    --task myapp.run_pipeline --subcategory bioinformatics_pipeline_analysis \
    --name run-2026-08-02 --payload payload.json --queue pipeline
```

Wrap the worker-side body and the row follows the execution:

```python
from entitymodel.celery_tasks import entity_task

@entity_task(celery_app, session_factory, name="myapp.run_pipeline")
def run_pipeline(session, task, payload):
    return {"output": "s3://..."}
```

```
Queued --> Running --> Succeeded
                   \-> Retrying --> Running
                   \-> Failed
```

Every transition goes through `fire_event`, so the whole history is in the event log and any
handler can subscribe to it. A crash between the commit and the send leaves a Task in
`Queued` that no worker will pick up; `pending_submissions()` finds those, because a Task
that was never sent has no `celery_task_id`.

## Troubleshooting a handler

`entitymodel.outbox` logs its dispatch decisions at DEBUG. Turning them on takes two steps,
and doing only the second is the usual mistake — a record has to pass the logger's level
**and** reach a handler:

```python
import logging

logging.basicConfig(level=logging.INFO)                            # 1. somewhere to go
logging.getLogger("entitymodel.outbox").setLevel(logging.DEBUG)    # 2. the level
```

That way round keeps the detail to this module. `basicConfig(level=logging.DEBUG)` works
too, but also turns on every SQL statement SQLAlchemy emits.

```
my-handler: polled ['AnalysisTaskSucceeded'] -> 1 candidate(s) (batch_size=100, max_attempts=5)
my-handler: dispatching event 3a947502-… (AnalysisTaskSucceeded)
my-handler: event 3a947502-… handled and checkpointed
```

The message worth knowing about is the one for a poll that found nothing, since "my handler
isn't running" is the usual complaint and the reason is otherwise invisible — the query
excludes rows silently:

```
my-handler: nothing to do because no events of type ['Nope'] exist yet
my-handler: nothing to do because of 2 event(s): 2 already processed, 0 dead-lettered, 0 backing off
my-handler: nothing to do because of 2 event(s): 0 already processed, 0 dead-lettered, 2 backing off
my-handler: nothing to do because of 2 event(s): 0 already processed, 2 dead-lettered, 0 backing off
```

That explanation costs an extra query, so it only runs when DEBUG is actually enabled.

Dead-lettering logs at **WARNING**, not DEBUG — an event nobody will retry, dropped with no
signal, is the failure that costs most to find late:

```
WARNING  my-handler: event af7c41e7-… dead-lettered after 5 attempt(s); last error
         RuntimeError: disk on fire. It will not be retried -- see dead_lettered() and replay()
```

## Managing workers

```python
WORKERS = {
    "pipeline": {"queue": "pipeline", "concurrency": 4, "log_path": "logs/pipeline.log"},
    "archive":  {"queue": "archive",  "concurrency": 1, "log_path": "logs/archive.log"},
}
```

```bash
python -m entitymodel.celery_workers --config myapp.conf:WORKERS --app myapp.celery:app
python -m entitymodel.celery_workers --config myapp.conf:WORKERS --app myapp.celery:app --force
python -m entitymodel.celery_workers --config myapp.conf:WORKERS --app myapp.celery:app --stop
```

Re-running is harmless: workers already up are skipped, since restarting would drop whatever
they have in flight. `--force` stops them first. The script then supervises in the
foreground, and **SIGTERM stops the workers it started** — which is why it does not detach:
a detached fleet has nothing left to signal. Liveness comes from pid files, so a second
invocation can see workers from a previous one without needing the broker; a pid file whose
process is gone reads as "not running" and is cleaned up.

## Migrations

Schema changes go through Alembic. `create_all()` builds a correct schema from scratch but
never issues `ALTER`, so it cannot upgrade a database that already exists.

```bash
alembic upgrade head                       # apply everything
alembic check                              # fail if the models have drifted
alembic revision --autogenerate -m "..."   # draft the next revision
```

The URL comes from `POSTGRES_URL` — environment or `.env` — so `alembic.ini` holds no
credentials. If you have a database that already matches the models, `alembic stamp head`
adopts it without re-running anything.

One thing autogenerate will not do for you: it compares tables, columns, indexes and
constraints, and is **blind to functions and triggers**. The entity status-validation
trigger is written by hand in the initial revision, and any revision that changes it has to
say so explicitly.
