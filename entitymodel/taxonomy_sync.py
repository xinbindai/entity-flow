"""
Load an entity taxonomy from CSV and reconcile the database with it.

The taxonomy -- which (category, subcategory) pairs exist and which statuses
each may hold -- is data, not schema, so it belongs in files a domain expert
can edit rather than in a Python literal or a migration. This module reads
those files and makes `entity_type` and `entity_status` match them: inserting
what is new, updating what changed, and reporting what the files no longer
mention.

Two files, not one, because the two tables have different columns and
different cardinality -- one subcategory has many statuses. A single
denormalised sheet would repeat each description on every status row and
invite the copies to disagree.

    entity_types.csv      category,subcategory,description
    entity_statuses.csv   category,subcategory,status,is_terminal

Typical use:

    from entitymodel.taxonomy_sync import sync_taxonomy_from_csv

    diff = sync_taxonomy_from_csv(session, "entity_types.csv", "entity_statuses.csv")
    print(diff.summary())

Idempotent: running it again against an unchanged pair of files reports no
changes and issues no writes.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from entitymodel.models import Entity, EntityStatus, EntityType

__all__ = [
    "TaxonomyDiff",
    "load_taxonomy_csv",
    "sync_taxonomy",
    "sync_taxonomy_from_csv",
]

# (category, subcategory) -> description
TypeRows = dict[tuple[str, str], str | None]
# (category, subcategory, status) -> is_terminal
StatusRows = dict[tuple[str, str, str], bool]

_TRUE = {"true", "t", "yes", "y", "1"}
_FALSE = {"false", "f", "no", "n", "0", ""}


@dataclass
class TaxonomyDiff:
    """What sync_taxonomy did, or would do. Falsy when nothing changed."""

    types_added: list[tuple[str, str]] = field(default_factory=list)
    types_updated: list[tuple[str, str]] = field(default_factory=list)
    types_removed: list[tuple[str, str]] = field(default_factory=list)
    statuses_added: list[tuple[str, str, str]] = field(default_factory=list)
    statuses_updated: list[tuple[str, str, str]] = field(default_factory=list)
    statuses_removed: list[tuple[str, str, str]] = field(default_factory=list)
    # Present in the database but absent from the files, and left alone
    # because delete_missing was False.
    types_orphaned: list[tuple[str, str]] = field(default_factory=list)
    statuses_orphaned: list[tuple[str, str, str]] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(
            self.types_added or self.types_updated or self.types_removed
            or self.statuses_added or self.statuses_updated or self.statuses_removed
        )

    def summary(self) -> str:
        parts = [
            f"types +{len(self.types_added)} ~{len(self.types_updated)} -{len(self.types_removed)}",
            f"statuses +{len(self.statuses_added)} ~{len(self.statuses_updated)} "
            f"-{len(self.statuses_removed)}",
        ]
        orphans = len(self.types_orphaned) + len(self.statuses_orphaned)
        if orphans:
            parts.append(f"{orphans} in database but not in the files (kept)")
        return "; ".join(parts)


def _parse_bool(value: str, *, where: str) -> bool:
    text = (value or "").strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    raise ValueError(f"{where}: expected a boolean, got {value!r}")


def _require_columns(reader: csv.DictReader, needed: set[str], path: Path) -> None:
    present = {(f or "").strip() for f in (reader.fieldnames or [])}
    missing = needed - present
    if missing:
        raise ValueError(f"{path}: missing column(s) {sorted(missing)}; found {sorted(present)}")


def load_taxonomy_csv(types_csv: str | Path, statuses_csv: str | Path) -> tuple[TypeRows, StatusRows]:
    """
    Parse both files and validate them against each other, before any database
    work. Two rules are enforced here because the database cannot express
    them helpfully on its own:

    - Every status must name a (category, subcategory) that the types file
      declares, or the insert would fail on a foreign key with a message that
      doesn't say which CSV row was wrong.
    - Every type must have at least one status. A subcategory with none can
      never be inserted at all, because the status-validation trigger rejects
      every value for it -- a silent trap rather than an error.
    """
    types_path, statuses_path = Path(types_csv), Path(statuses_csv)

    types: TypeRows = {}
    with types_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_columns(reader, {"category", "subcategory"}, types_path)
        for line, row in enumerate(reader, start=2):
            key = (row["category"].strip(), row["subcategory"].strip())
            if not all(key):
                raise ValueError(f"{types_path}:{line}: category and subcategory are required")
            if key in types:
                raise ValueError(f"{types_path}:{line}: duplicate entry for {key}")
            description = (row.get("description") or "").strip() or None
            types[key] = description

    statuses: StatusRows = {}
    with statuses_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_columns(reader, {"category", "subcategory", "status"}, statuses_path)
        for line, row in enumerate(reader, start=2):
            key = (row["category"].strip(), row["subcategory"].strip(), row["status"].strip())
            if not all(key):
                raise ValueError(
                    f"{statuses_path}:{line}: category, subcategory and status are required"
                )
            if key in statuses:
                raise ValueError(f"{statuses_path}:{line}: duplicate entry for {key}")
            statuses[key] = _parse_bool(
                row.get("is_terminal", ""), where=f"{statuses_path}:{line} is_terminal"
            )

    undeclared = sorted({(c, s) for c, s, _ in statuses} - set(types))
    if undeclared:
        raise ValueError(
            f"{statuses_path}: status rows for subcategories missing from {types_path}: {undeclared}"
        )

    statusless = sorted(set(types) - {(c, s) for c, s, _ in statuses})
    if statusless:
        raise ValueError(
            f"{types_path}: subcategories with no status in {statuses_path}: {statusless}. "
            f"An entity of such a subcategory can never be inserted -- the "
            f"validate_entity_status trigger rejects every status for it."
        )

    return types, statuses


def sync_taxonomy(
    session: Session,
    types: TypeRows,
    statuses: StatusRows,
    *,
    delete_missing: bool = False,
    dry_run: bool = False,
) -> TaxonomyDiff:
    """
    Make entity_type and entity_status match `types` and `statuses`.

    Only descriptions and is_terminal flags can be *updated* -- everything
    else in these tables is part of the primary key, so a change there is an
    add plus a remove, not an edit.

    delete_missing is off by default. Dropping a status still held by live
    entities is quietly destructive: nothing references entity_status by
    foreign key, so the delete succeeds, and afterwards those entities cannot
    be updated at all because the trigger rejects the status they already
    have. When enabled, this refuses rather than doing that.

    dry_run computes the diff and rolls nothing forward, for previewing a
    change before applying it. The caller commits; nothing here does.
    """
    diff = TaxonomyDiff()

    existing_types: TypeRows = {
        (t.category, t.subcategory): t.description for t in session.scalars(select(EntityType))
    }
    existing_statuses: StatusRows = {
        (s.category, s.subcategory, s.status): s.is_terminal
        for s in session.scalars(select(EntityStatus))
    }

    for key, description in types.items():
        if key not in existing_types:
            diff.types_added.append(key)
        elif existing_types[key] != description:
            diff.types_updated.append(key)

    for key, is_terminal in statuses.items():
        if key not in existing_statuses:
            diff.statuses_added.append(key)
        elif existing_statuses[key] != is_terminal:
            diff.statuses_updated.append(key)

    missing_types = sorted(set(existing_types) - set(types))
    missing_statuses = sorted(set(existing_statuses) - set(statuses))

    if delete_missing:
        _refuse_if_in_use(session, missing_types, missing_statuses)
        diff.types_removed, diff.statuses_removed = missing_types, missing_statuses
    else:
        diff.types_orphaned, diff.statuses_orphaned = missing_types, missing_statuses

    if dry_run:
        return diff

    # Order matters throughout: entity_status has a composite foreign key onto
    # entity_type, so parents are created before children and children are
    # dropped before parents.
    for category, subcategory in diff.types_added:
        session.add(
            EntityType(
                category=category, subcategory=subcategory, description=types[(category, subcategory)]
            )
        )
    for key in diff.types_updated:
        session.get(EntityType, key).description = types[key]
    session.flush()

    for key in diff.statuses_added:
        category, subcategory, status = key
        session.add(
            EntityStatus(
                category=category,
                subcategory=subcategory,
                status=status,
                is_terminal=statuses[key],
            )
        )
    for key in diff.statuses_updated:
        session.get(EntityStatus, key).is_terminal = statuses[key]
    session.flush()

    for key in diff.statuses_removed:
        session.delete(session.get(EntityStatus, key))
    session.flush()
    for key in diff.types_removed:
        session.delete(session.get(EntityType, key))
    session.flush()

    return diff


def _refuse_if_in_use(
    session: Session,
    types_to_remove: list[tuple[str, str]],
    statuses_to_remove: list[tuple[str, str, str]],
) -> None:
    """
    A type still referenced by an entity is caught by the foreign key anyway,
    but the message names a constraint rather than the row that caused it. A
    status still held by an entity is caught by nothing at all, which is the
    dangerous one.
    """
    if not types_to_remove and not statuses_to_remove:
        return

    in_use_statuses = {
        (c, s, st)
        for c, s, st in session.execute(
            select(Entity.category, Entity.subcategory, Entity.status).distinct()
        )
    }
    in_use_types = {(c, s) for c, s, _ in in_use_statuses}

    blocked_statuses = sorted(set(statuses_to_remove) & in_use_statuses)
    blocked_types = sorted(set(types_to_remove) & in_use_types)
    if blocked_statuses or blocked_types:
        raise ValueError(
            "refusing to delete taxonomy rows that live entities still use: "
            f"types={blocked_types} statuses={blocked_statuses}. "
            f"Migrate those entities first, or leave delete_missing off."
        )


def sync_taxonomy_from_csv(
    session: Session,
    types_csv: str | Path,
    statuses_csv: str | Path,
    *,
    delete_missing: bool = False,
    dry_run: bool = False,
) -> TaxonomyDiff:
    """Parse both CSVs and reconcile the database with them."""
    types, statuses = load_taxonomy_csv(types_csv, statuses_csv)
    return sync_taxonomy(
        session, types, statuses, delete_missing=delete_missing, dry_run=dry_run
    )
