import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    event,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    """Declarative base for all ArgusCore ORM models."""


@event.listens_for(Base, "init", propagate=True)
def _apply_python_defaults(target: Base, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
    """Eagerly apply column Python-side defaults at construction time.

    SQLAlchemy's ``mapped_column(default=...)`` only fires at flush/INSERT
    time, leaving instance attributes ``None`` immediately after construction.
    We copy ``ColumnDefault`` values onto the instance so that columns omitted
    from the constructor read as their configured default before any session
    interaction (matching the ``NOT NULL DEFAULT ...`` contract in the schema).
    """
    for col in target.__table__.columns:
        if col.name in kwargs or col.default is None:
            continue
        if getattr(target, col.name, None) is not None:
            continue
        arg = col.default.arg
        if callable(arg):
            try:
                value = arg()
            except TypeError:
                value = arg(None)
        else:
            value = arg
        # The ``init`` event fires from inside ``_initialize_instance`` before
        # SQLAlchemy's attribute instrumentation is fully wired, so the public
        # ``setattr`` would raise ``'NoneType' object has no attribute 'set'``.
        # Writing into ``__dict__`` mirrors how SQLAlchemy records attribute
        # state and is picked up by attribute access on the transient instance.
        target.__dict__[col.name] = value


class RawEvent(Base):
    __tablename__ = "raw_events"
    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_source_external"),
        Index("idx_events_status", "status", "fetched_at"),
        Index("idx_events_score", "relevance_score", postgresql_where=text("status = 'processed'")),
        Index("idx_events_hash", "content_hash"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    external_id: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    ticker: Mapped[str | None] = mapped_column(String(16))
    title: Mapped[str] = mapped_column(Text, nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    status: Mapped[str] = mapped_column(String(16), default="new")
    error_message: Mapped[str | None] = mapped_column(Text)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    duplicate_of: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("raw_events.id"), nullable=True
    )

    relevance_score: Mapped[int | None] = mapped_column(Integer)
    sentiment: Mapped[str | None] = mapped_column(String(16))
    llm_summary: Mapped[str | None] = mapped_column(Text)
    llm_classified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    poller_meta: Mapped[dict[str, Any] | None] = mapped_column(JSONB)

    llm_calls: Mapped[list["LlmCall"]] = relationship(back_populates="event")
    notifications: Mapped[list["Notification"]] = relationship(back_populates="event")


class LlmCall(Base):
    __tablename__ = "llm_calls"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("raw_events.id", ondelete="CASCADE")
    )
    provider: Mapped[str] = mapped_column(String(32))
    model: Mapped[str] = mapped_column(String(64))
    prompt_tokens: Mapped[int | None] = mapped_column(Integer)
    completion_tokens: Mapped[int | None] = mapped_column(Integer)
    cost_usd: Mapped[float | None] = mapped_column(Numeric(10, 6))
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    event: Mapped["RawEvent"] = relationship(back_populates="llm_calls")


class Notification(Base):
    __tablename__ = "notifications"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("raw_events.id"))
    channel: Mapped[str] = mapped_column(String(16))
    webhook_id: Mapped[str | None] = mapped_column(String(64))
    delivered: Mapped[bool] = mapped_column(default=False)
    discord_msg_id: Mapped[str | None] = mapped_column(String(64))
    error: Mapped[str | None] = mapped_column(Text)
    sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    event: Mapped["RawEvent"] = relationship(back_populates="notifications")


class PollingState(Base):
    __tablename__ = "polling_state"
    source: Mapped[str] = mapped_column(String(128), primary_key=True)
    cursor: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class Form4Cluster(Base):
    """A detected Form-4 filing cluster (same ticker, >= N filings in a window).

    Used for idempotent cluster alerting: one row per ticker per ~window, with
    ``alerted_at`` set only after a successful Discord send. Later scans refresh
    ``member_event_ids`` / ``window_end`` on the open row without re-sending.
    """

    __tablename__ = "form4_clusters"
    __table_args__ = (Index("idx_form4_clusters_ticker_created", "ticker", "created_at"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False)
    member_event_ids: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    alerted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now
    )
