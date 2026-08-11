"""
Test data + database setup shared by every suite in this directory.

Everything is created inside a dedicated `poll_test` Postgres schema rather
than `public`, so running these tests never touches anything else in the
database and teardown is a single DROP SCHEMA ... CASCADE.

Connection URL comes from .env at the repo root (POSTGRES_URL).
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
# Puts the entitymodel package, taxonomy.py and demo.py on the path, so these
# suites run as plain scripts from anywhere without installing the project.
sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy import create_engine, text  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from entitymodel.models import Base, EventRecord, Task  # noqa: E402
from taxonomy import seed_taxonomy  # noqa: E402

TEST_SCHEMA = "poll_test"

# occurred_at is business time with no server default, so producers always
# supply it. Aware datetimes only -- the column is timestamptz.
BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def load_env(path: Path | None = None) -> dict[str, str]:
    """Minimal .env reader -- avoids a python-dotenv dependency."""
    path = path or (REPO_ROOT / ".env")
    if not path.exists():
        raise SystemExit(
            f"{path} not found. Copy {REPO_ROOT / '.env.example'} to .env "
            f"and set POSTGRES_URL to a database you can create schemas in."
        )
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def make_engine(echo: bool = False):
    url = load_env()["POSTGRES_URL"]
    # search_path pins every table, the trigger function and gen_random_uuid()
    # lookups into the test schema.
    #
    # timezone=UTC pins what the driver hands back. timestamptz stores an
    # absolute instant and is rendered in the *session's* zone, so on a server
    # set to a local zone the same correct instant comes back as, say,
    # 06:00-06:00 instead of 12:00+00:00. The timing tests assert
    # utcoffset() == 0, which would then fail on data that is perfectly fine.
    # Pinning it makes that assumption explicit and the tests reproducible on
    # any machine, rather than passing by accident where the server is UTC.
    return create_engine(
        url,
        echo=echo,
        connect_args={"options": f"-csearch_path={TEST_SCHEMA},public -ctimezone=UTC"},
    )


def reset_schema(engine) -> None:
    """Drop and recreate the test schema, then build the full model schema."""
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {TEST_SCHEMA}"))
    Base.metadata.create_all(engine)


def drop_schema(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE"))


# --------------------------------------------------------------------------
# Test data builders
# --------------------------------------------------------------------------
def fresh_session(engine) -> Session:
    """A schema reset to empty, taxonomy seeded, ready for a test."""
    reset_schema(engine)
    session = Session(engine)
    seed_taxonomy(session)
    return session


def make_task(session: Session, name: str = "pipeline-run-1") -> Task:
    """One Running analysis task, the entity the test events hang off."""
    task = Task(
        subcategory="bioinformatics_pipeline_analysis",
        name=name,
        status="Running",
        correlation_id=uuid.uuid4(),
        attributes={"retry_count": 0},
    )
    session.add(task)
    session.commit()
    return task


def make_event(
    session: Session,
    task: Task,
    *,
    event_type: str = "AnalysisTaskSucceeded",
    seq: int = 0,
    sample_id: str | None = None,
    occurred_at: datetime | None = None,
    trace_id: str | None = None,
    commit: bool = True,
) -> EventRecord:
    """
    One event, built directly rather than through fire_event() so tests can
    control occurred_at and can create events that don't imply a valid status
    transition on the task.

    commit=False leaves the event in the open transaction, which is how the
    timing tests observe several rows sharing one transaction.
    """
    ev = EventRecord(
        event_type=event_type,
        entity_type=task.category,
        entity_id=task.id,
        correlation_id=task.correlation_id,
        source="pipeline-worker",
        actor_type="worker",
        payload={
            "sequencing_sample_id": sample_id or f"SEQ-{seq:03d}",
            "pipeline_version": "v2.3.1",
            "reference_genome": "GRCh38",
            "seq": seq,
        },
        occurred_at=occurred_at or (BASE_TIME + timedelta(seconds=seq)),
        trace_id=trace_id,
    )
    session.add(ev)
    if commit:
        session.commit()
    else:
        session.flush()
    return ev


def make_event_series(session: Session, task: Task, count: int) -> list[EventRecord]:
    """`count` AnalysisTaskSucceeded events with strictly increasing occurred_at."""
    return [make_event(session, task, seq=i) for i in range(count)]
