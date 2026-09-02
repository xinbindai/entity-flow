"""add channel to events

Revision ID: 9c4e1a7b2d05
Revises: 27f88ce75b7b
Create Date: 2026-08-30 10:14:02.115330

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9c4e1a7b2d05'
down_revision: Union[str, Sequence[str], None] = '27f88ce75b7b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # NOT NULL with a default rather than nullable-then-backfill. On
    # PostgreSQL 11+ this is metadata-only: the default is recorded in the
    # catalog and materialised on read, so a large events table is not
    # rewritten and no separate backfill pass is needed. Every existing row
    # reads back as 'default', which is what makes the column inert until
    # somebody starts using it.
    #
    # Nullable would have been the wrong shape regardless of the rewrite:
    # `channel = NULL` is never true, so a null routing key means silent
    # under-delivery rather than an error.
    op.add_column(
        "events",
        sa.Column(
            "channel", sa.Text(), nullable=False, server_default=sa.text("'default'")
        ),
    )

    # CONCURRENTLY, as with idx_events_trace before it: a plain CREATE INDEX
    # takes a SHARE lock, which blocks every INSERT into events for the
    # duration, and on an append-only log that is the entire write path.
    #
    # It cannot run inside a transaction, hence the autocommit block. A
    # failure here does not roll back and can leave an INVALID index; drop it
    # and re-run rather than assuming it is usable:
    #
    #     SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid;
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_channel_type_occurred "
            "ON events (channel, event_type, occurred_at)"
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_events_channel_type_occurred")
    op.drop_column("events", "channel")
