"""index events by trace_id

Revision ID: 27f88ce75b7b
Revises: 5a072b1fb587
Create Date: 2026-08-11 18:19:19.264528

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '27f88ce75b7b'
down_revision: Union[str, Sequence[str], None] = '5a072b1fb587'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # CONCURRENTLY, unlike what autogenerate wrote. A plain CREATE INDEX takes
    # a SHARE lock, which blocks every INSERT into events for as long as the
    # build runs -- on an append-only log that is the whole write path.
    #
    # It cannot run inside a transaction, hence the autocommit block. The cost
    # is that a failure here does not roll back and can leave an INVALID
    # index; drop it and re-run rather than assuming the index is usable:
    #
    #     SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid;
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_trace "
            "ON events (trace_id) WHERE trace_id IS NOT NULL"
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_events_trace")
