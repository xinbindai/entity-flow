"""
The lab's entity taxonomy: which (category, subcategory) pairs exist and what
statuses each one may hold. Mirrors the INSERT statements in
entitymodel/entity_schema_unified.sql -- keep the two in step.

This is domain configuration, not schema, which is why it lives here rather
than in entitymodel/models.py: models.py defines the shape of any entity-event-task
system, while this file says what *this* lab's entities are. A different
deployment keeps models.py verbatim and replaces this file.

It is also not test or demo data -- a real deployment has to seed these rows
before it can insert a single entity, because the validate_entity_status
trigger rejects any status with no matching entity_status row.

Bootstrap a dev/test database (create_all + seed) in one call:

    python taxonomy.py postgresql+psycopg2://localhost/lab_platform
"""

from __future__ import annotations

import sys

from sqlalchemy.orm import Session

from entitymodel.models import EntityStatus, EntityType

# (category, subcategory, description)
ENTITY_TYPES = [
    ("Client", "ordering_institution", "Ordering institution or practice that submits lab orders"),
    ("Patient", "patient", "Root clinical identity"),
    ("Order", "lab_order", "Test requisition"),
    ("Sample", "raw_specimen", "Physical specimen as collected"),
    ("Sample", "library_sample", "Prepped library/aliquot ready for sequencing"),
    ("Batch", "illumina_run", "A sequencing run pooling many library samples"),
    ("Result", "sample_result", "Pipeline output for one or more library samples"),
    ("Task", "bioinformatics_pipeline_analysis", "Async pipeline execution task"),
    ("Task", "data_archiving", "Async data archival task"),
]

# (category, subcategory, status, is_terminal)
#
# Every subcategory in ENTITY_TYPES needs at least one row here: the
# validate_entity_status trigger rejects any status without a matching
# (category, subcategory, status) row, so a subcategory with none is
# impossible to insert at all. test_entity_subclasses.py asserts this.
ENTITY_STATUSES = [
    ("Client", "ordering_institution", "Onboarding", False),
    ("Client", "ordering_institution", "Active", False),
    ("Client", "ordering_institution", "Suspended", False),
    ("Client", "ordering_institution", "Offboarded", True),
    ("Patient", "patient", "Active", False), ("Patient", "patient", "Inactive", False),
    ("Patient", "patient", "Deceased", True), ("Patient", "patient", "Merged", True),
    ("Order", "lab_order", "Placed", False), ("Order", "lab_order", "InProgress", False),
    ("Order", "lab_order", "Completed", True), ("Order", "lab_order", "Cancelled", True),
    ("Sample", "raw_specimen", "Received", False), ("Sample", "raw_specimen", "Accessioned", False),
    ("Sample", "raw_specimen", "QC_Passed", False), ("Sample", "raw_specimen", "Rejected", True),
    ("Sample", "raw_specimen", "Consumed", True),
    ("Sample", "library_sample", "Prepped", False), ("Sample", "library_sample", "Loaded", False),
    ("Sample", "library_sample", "Sequencing", False), ("Sample", "library_sample", "Sequenced", True),
    ("Sample", "library_sample", "Failed", True),
    ("Batch", "illumina_run", "Planned", False), ("Batch", "illumina_run", "Loading", False),
    ("Batch", "illumina_run", "Running", False), ("Batch", "illumina_run", "Complete", True),
    ("Batch", "illumina_run", "Failed", True),
    ("Result", "sample_result", "Pending", False), ("Result", "sample_result", "Processing", False),
    ("Result", "sample_result", "Complete", False), ("Result", "sample_result", "Reviewed", False),
    ("Result", "sample_result", "Released", False), ("Result", "sample_result", "Archived", True),
    ("Result", "sample_result", "Failed", True),
    ("Task", "bioinformatics_pipeline_analysis", "Queued", False),
    ("Task", "bioinformatics_pipeline_analysis", "Running", False),
    ("Task", "bioinformatics_pipeline_analysis", "Succeeded", True),
    ("Task", "bioinformatics_pipeline_analysis", "Failed", True),
    ("Task", "bioinformatics_pipeline_analysis", "Retrying", False),
    ("Task", "bioinformatics_pipeline_analysis", "Cancelled", True),
    ("Task", "data_archiving", "Queued", False), ("Task", "data_archiving", "Running", False),
    ("Task", "data_archiving", "Succeeded", True), ("Task", "data_archiving", "Failed", True),
    ("Task", "data_archiving", "Retrying", False), ("Task", "data_archiving", "Cancelled", True),
]

__all__ = ["ENTITY_STATUSES", "ENTITY_TYPES", "seed_taxonomy"]


def seed_taxonomy(session: Session) -> None:
    """Insert the taxonomy above. Call once against a fresh database."""
    session.add_all(
        EntityType(category=c, subcategory=s, description=d) for c, s, d in ENTITY_TYPES
    )
    # Flush the parent rows before adding the children: the entity_type ->
    # entity_status dependency is a table-level ForeignKeyConstraint with no
    # ORM relationship() behind it, so the unit of work has nothing to sort
    # these two INSERT batches by and may emit entity_status first.
    session.flush()
    session.add_all(
        EntityStatus(category=c, subcategory=s, status=st, is_terminal=t)
        for c, s, st, t in ENTITY_STATUSES
    )
    session.commit()


if __name__ == "__main__":
    # Bootstrap a dev/test database in one call. Pair schema changes with
    # Alembic rather than create_all() in anything longer-lived than this.
    from sqlalchemy import create_engine

    from entitymodel.models import Base

    url = sys.argv[1] if len(sys.argv) > 1 else "postgresql+psycopg2://localhost/lab_platform"
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        seed_taxonomy(session)
    print(f"created schema and seeded taxonomy in {url}")
