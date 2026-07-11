"""add raw_events.duplicate_of

Revision ID: 0002_duplicate_of
Revises: 0001_initial
Create Date: 2026-07-11

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_duplicate_of"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "raw_events",
        sa.Column(
            "duplicate_of",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("raw_events.id"),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("raw_events", "duplicate_of")
