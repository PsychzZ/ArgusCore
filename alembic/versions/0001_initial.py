"""initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2026-07-05

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "raw_events",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("external_id", sa.Text, nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("ticker", sa.String(16)),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("body", sa.Text, nullable=False),
        sa.Column("url", sa.Text, nullable=False),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("status", sa.String(16), server_default="new", nullable=False),
        sa.Column("error_message", sa.Text),
        sa.Column("retry_count", sa.Integer, server_default="0", nullable=False),
        sa.Column("relevance_score", sa.Integer),
        sa.Column("sentiment", sa.String(16)),
        sa.Column("llm_summary", sa.Text),
        sa.Column("llm_classified_at", sa.DateTime(timezone=True)),
        sa.Column("sent_at", sa.DateTime(timezone=True)),
        sa.Column("poller_meta", postgresql.JSONB),
        sa.UniqueConstraint("source", "external_id", name="uq_source_external"),
    )
    op.create_index("idx_events_status", "raw_events", ["status", "fetched_at"])
    op.create_index(
        "idx_events_score",
        "raw_events",
        ["relevance_score"],
        postgresql_where=sa.text("status = 'processed'"),
    )
    op.create_index("idx_events_hash", "raw_events", ["content_hash"])

    op.create_table(
        "llm_calls",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "event_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("raw_events.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("model", sa.String(64), nullable=False),
        sa.Column("prompt_tokens", sa.Integer),
        sa.Column("completion_tokens", sa.Integer),
        sa.Column("cost_usd", sa.Numeric(10, 6)),
        sa.Column("latency_ms", sa.Integer),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index("idx_llm_event", "llm_calls", ["event_id"])

    op.create_table(
        "notifications",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "event_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("raw_events.id"),
            nullable=False,
        ),
        sa.Column("channel", sa.String(16), nullable=False),
        sa.Column("webhook_id", sa.String(64)),
        sa.Column("delivered", sa.Boolean, server_default="false", nullable=False),
        sa.Column("discord_msg_id", sa.String(64)),
        sa.Column("error", sa.Text),
        sa.Column("sent_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )

    op.create_table(
        "polling_state",
        sa.Column("source", sa.String(128), primary_key=True),
        sa.Column("cursor", sa.Text, nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_table("polling_state")
    op.drop_table("notifications")
    op.drop_table("llm_calls")
    op.drop_index("idx_events_hash", table_name="raw_events")
    op.drop_index("idx_events_score", table_name="raw_events")
    op.drop_index("idx_events_status", table_name="raw_events")
    op.drop_table("raw_events")
