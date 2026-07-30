"""
Tests for entitymodel.taxonomy_sync -- loading a taxonomy from CSV and
reconciling the database with it.

Unlike the other suites these start from an *empty* taxonomy, since driving
the sync is the point.

    python test/test_taxonomy_sync.py
"""

from __future__ import annotations

import csv
import sys
import tempfile
import traceback
import uuid
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from testdata import drop_schema, make_engine, reset_schema  # noqa: E402

from entitymodel.models import Entity, EntityStatus, EntityType  # noqa: E402
from entitymodel.taxonomy_sync import (  # noqa: E402
    load_taxonomy_csv,
    sync_taxonomy_from_csv,
)

TYPES = [("Task", "analysis", "An analysis task")]
STATUSES = [("Task", "analysis", "Queued", "false"), ("Task", "analysis", "Done", "true")]


def write_csvs(tmp: Path, types=None, statuses=None, *, type_header=None, status_header=None):
    types_path, statuses_path = tmp / "types.csv", tmp / "statuses.csv"
    with types_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(type_header or ["category", "subcategory", "description"])
        w.writerows(TYPES if types is None else types)
    with statuses_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(status_header or ["category", "subcategory", "status", "is_terminal"])
        w.writerows(STATUSES if statuses is None else statuses)
    return types_path, statuses_path


def expect_error(fn, fragment: str, what: str):
    try:
        fn()
    except ValueError as exc:
        assert fragment in str(exc), f"{what}: expected {fragment!r} in {exc}"
    else:
        raise AssertionError(f"{what}: expected a ValueError")


def db_types(session):
    return {(t.category, t.subcategory): t.description for t in session.scalars(select(EntityType))}


def db_statuses(session):
    return {
        (s.category, s.subcategory, s.status): s.is_terminal
        for s in session.scalars(select(EntityStatus))
    }


# --------------------------------------------------------------------------
def test_seeds_an_empty_database(session: Session, tmp: Path) -> None:
    t, s = write_csvs(tmp)
    diff = sync_taxonomy_from_csv(session, t, s)
    session.commit()

    assert len(diff.types_added) == 1 and len(diff.statuses_added) == 2, diff.summary()
    assert db_types(session) == {("Task", "analysis"): "An analysis task"}
    assert db_statuses(session) == {
        ("Task", "analysis", "Queued"): False,
        ("Task", "analysis", "Done"): True,
    }


def test_is_idempotent(session: Session, tmp: Path) -> None:
    t, s = write_csvs(tmp)
    sync_taxonomy_from_csv(session, t, s)
    session.commit()

    diff = sync_taxonomy_from_csv(session, t, s)
    session.commit()

    assert not diff, f"a second run should be a no-op, got {diff.summary()}"
    assert diff.summary().startswith("types +0 ~0 -0")


def test_adds_a_new_subcategory(session: Session, tmp: Path) -> None:
    t, s = write_csvs(tmp)
    sync_taxonomy_from_csv(session, t, s)
    session.commit()

    t, s = write_csvs(
        tmp,
        types=TYPES + [("Task", "archiving", "An archiving task")],
        statuses=STATUSES + [("Task", "archiving", "Queued", "false")],
    )
    diff = sync_taxonomy_from_csv(session, t, s)
    session.commit()

    assert diff.types_added == [("Task", "archiving")], diff.types_added
    assert diff.statuses_added == [("Task", "archiving", "Queued")], diff.statuses_added
    assert ("Task", "archiving") in db_types(session)


def test_updates_a_changed_description(session: Session, tmp: Path) -> None:
    t, s = write_csvs(tmp)
    sync_taxonomy_from_csv(session, t, s)
    session.commit()

    t, s = write_csvs(tmp, types=[("Task", "analysis", "Rewritten description")])
    diff = sync_taxonomy_from_csv(session, t, s)
    session.commit()

    assert diff.types_updated == [("Task", "analysis")], diff.types_updated
    assert diff.types_added == [] and diff.types_removed == []
    assert db_types(session)[("Task", "analysis")] == "Rewritten description"


def test_updates_a_changed_is_terminal(session: Session, tmp: Path) -> None:
    t, s = write_csvs(tmp)
    sync_taxonomy_from_csv(session, t, s)
    session.commit()

    flipped = [("Task", "analysis", "Queued", "true"), ("Task", "analysis", "Done", "true")]
    t, s = write_csvs(tmp, statuses=flipped)
    diff = sync_taxonomy_from_csv(session, t, s)
    session.commit()

    assert diff.statuses_updated == [("Task", "analysis", "Queued")], diff.statuses_updated
    assert db_statuses(session)[("Task", "analysis", "Queued")] is True


def test_rows_dropped_from_csv_are_reported_not_deleted(session: Session, tmp: Path) -> None:
    """The default is conservative: tell the caller, change nothing."""
    t, s = write_csvs(tmp)
    sync_taxonomy_from_csv(session, t, s)
    session.commit()

    t, s = write_csvs(tmp, statuses=[("Task", "analysis", "Queued", "false")])
    diff = sync_taxonomy_from_csv(session, t, s)
    session.commit()

    assert diff.statuses_orphaned == [("Task", "analysis", "Done")], diff.statuses_orphaned
    assert diff.statuses_removed == []
    assert ("Task", "analysis", "Done") in db_statuses(session), "row was deleted by default"
    assert "not in the files (kept)" in diff.summary()


def test_delete_missing_removes_them(session: Session, tmp: Path) -> None:
    t, s = write_csvs(tmp)
    sync_taxonomy_from_csv(session, t, s)
    session.commit()

    t, s = write_csvs(tmp, statuses=[("Task", "analysis", "Queued", "false")])
    diff = sync_taxonomy_from_csv(session, t, s, delete_missing=True)
    session.commit()

    assert diff.statuses_removed == [("Task", "analysis", "Done")], diff.statuses_removed
    assert ("Task", "analysis", "Done") not in db_statuses(session)


def test_delete_missing_refuses_a_status_in_use(session: Session, tmp: Path) -> None:
    """
    Nothing references entity_status by foreign key, so this delete would
    succeed and then strand the entity: the trigger would reject the status it
    already holds, making it impossible to update.
    """
    t, s = write_csvs(tmp)
    sync_taxonomy_from_csv(session, t, s)
    session.commit()

    session.add(
        Entity(category="Task", subcategory="analysis", name="live-one",
               status="Done", correlation_id=uuid.uuid4(), attributes={})
    )
    session.commit()

    t, s = write_csvs(tmp, statuses=[("Task", "analysis", "Queued", "false")])
    expect_error(
        lambda: sync_taxonomy_from_csv(session, t, s, delete_missing=True),
        "live entities still use",
        "deleting an in-use status",
    )
    session.rollback()
    assert ("Task", "analysis", "Done") in db_statuses(session), "deleted despite the refusal"


def test_delete_missing_refuses_a_type_in_use(session: Session, tmp: Path) -> None:
    t, s = write_csvs(
        tmp,
        types=TYPES + [("Task", "archiving", "An archiving task")],
        statuses=STATUSES + [("Task", "archiving", "Queued", "false")],
    )
    sync_taxonomy_from_csv(session, t, s)
    session.commit()

    session.add(
        Entity(category="Task", subcategory="archiving", name="live-two",
               status="Queued", correlation_id=uuid.uuid4(), attributes={})
    )
    session.commit()

    t, s = write_csvs(tmp)  # archiving dropped from both files
    expect_error(
        lambda: sync_taxonomy_from_csv(session, t, s, delete_missing=True),
        "live entities still use",
        "deleting an in-use type",
    )
    session.rollback()
    assert ("Task", "archiving") in db_types(session)


def test_dry_run_changes_nothing(session: Session, tmp: Path) -> None:
    t, s = write_csvs(tmp)
    diff = sync_taxonomy_from_csv(session, t, s, dry_run=True)
    session.commit()

    assert diff.types_added == [("Task", "analysis")], "dry run should still report the diff"
    assert session.scalar(select(func.count()).select_from(EntityType)) == 0, "dry run wrote rows"
    assert session.scalar(select(func.count()).select_from(EntityStatus)) == 0


# --------------------------------------------------------------------------
# Validation, all before any database work
# --------------------------------------------------------------------------
def test_rejects_a_status_for_an_undeclared_subcategory(session: Session, tmp: Path) -> None:
    t, s = write_csvs(tmp, statuses=STATUSES + [("Ghost", "nowhere", "Queued", "false")])
    expect_error(lambda: load_taxonomy_csv(t, s), "missing from", "undeclared subcategory")


def test_rejects_a_subcategory_with_no_statuses(session: Session, tmp: Path) -> None:
    """The Patient trap: such an entity can never be inserted at all."""
    t, s = write_csvs(tmp, types=TYPES + [("Task", "orphan", "no statuses")])
    expect_error(lambda: load_taxonomy_csv(t, s), "can never be inserted", "statusless subcategory")


def test_rejects_a_missing_column(session: Session, tmp: Path) -> None:
    t, s = write_csvs(tmp, type_header=["category", "description"])
    expect_error(lambda: load_taxonomy_csv(t, s), "missing column", "missing column")


def test_rejects_a_duplicate_row(session: Session, tmp: Path) -> None:
    t, s = write_csvs(tmp, statuses=STATUSES + [("Task", "analysis", "Queued", "true")])
    expect_error(lambda: load_taxonomy_csv(t, s), "duplicate entry", "duplicate status")


def test_rejects_an_unparseable_boolean(session: Session, tmp: Path) -> None:
    t, s = write_csvs(tmp, statuses=[("Task", "analysis", "Queued", "perhaps")])
    expect_error(lambda: load_taxonomy_csv(t, s), "expected a boolean", "bad is_terminal")


def test_accepts_common_boolean_spellings(session: Session, tmp: Path) -> None:
    rows = [
        ("Task", "analysis", "A", "TRUE"), ("Task", "analysis", "B", "1"),
        ("Task", "analysis", "C", "yes"), ("Task", "analysis", "D", "False"),
        ("Task", "analysis", "E", "0"), ("Task", "analysis", "F", ""),
    ]
    t, s = write_csvs(tmp, statuses=rows)
    _, statuses = load_taxonomy_csv(t, s)
    assert [statuses[("Task", "analysis", k)] for k in "ABCDEF"] == \
        [True, True, True, False, False, False]


def _project_csvs() -> tuple[Path, Path]:
    from taxonomy import ENTITY_STATUSES_CSV, ENTITY_TYPES_CSV

    return ENTITY_TYPES_CSV, ENTITY_STATUSES_CSV


def test_the_projects_own_csvs_load(session: Session, tmp: Path) -> None:
    """Guards the real files, not a fixture."""
    types, statuses = load_taxonomy_csv(*_project_csvs())
    assert len(types) == 9, len(types)
    assert len(statuses) == 46, len(statuses)
    assert ("Client", "ordering_institution") in types
    assert statuses[("Task", "data_archiving", "Succeeded")] is True


# --------------------------------------------------------------------------
TESTS = [
    test_seeds_an_empty_database,
    test_is_idempotent,
    test_adds_a_new_subcategory,
    test_updates_a_changed_description,
    test_updates_a_changed_is_terminal,
    test_rows_dropped_from_csv_are_reported_not_deleted,
    test_delete_missing_removes_them,
    test_delete_missing_refuses_a_status_in_use,
    test_delete_missing_refuses_a_type_in_use,
    test_dry_run_changes_nothing,
    test_rejects_a_status_for_an_undeclared_subcategory,
    test_rejects_a_subcategory_with_no_statuses,
    test_rejects_a_missing_column,
    test_rejects_a_duplicate_row,
    test_rejects_an_unparseable_boolean,
    test_accepts_common_boolean_spellings,
    test_the_projects_own_csvs_load,
]


def main(keep: bool = False) -> int:
    engine = make_engine()
    passed, failed = 0, []

    for test in TESTS:
        # Empty schema, no taxonomy seeded -- these drive the sync themselves.
        reset_schema(engine)
        session = Session(engine)
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                test(session, Path(tmpdir))
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
