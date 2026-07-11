"""add form4_clusters

Revision ID: 0003_form4_clusters
Revises: 0002_duplicate_of
Create Date: 2026-07-11

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003_form4_clusters"
down_revision: str | None = "0002_duplicate_of"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "form4_clusters",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("member_event_ids", postgresql.JSONB(), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("alerted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_form4_clusters_ticker_created",
        "form4_clusters",
        ["ticker", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_form4_clusters_ticker_created", table_name="form4_clusters")
    op.drop_table("form4_clusters")
