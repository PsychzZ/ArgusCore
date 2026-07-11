from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.common.models import RawEvent

CLUSTER_STATUSES = ("new", "processed", "sent")

SessionInput = AsyncSession | async_sessionmaker[AsyncSession]


@asynccontextmanager
async def _session_scope(session: SessionInput) -> AsyncIterator[AsyncSession]:
    """Yield an open session whether given a live session or a factory.

    Accepting both keeps the query usable from callers that already own a
    session (e.g. a job writing a cluster in the same transaction) and from
    callers that only have the session factory.
    """
    if isinstance(session, AsyncSession):
        yield session
    else:
        async with session() as s:
            yield s


async def find_form4_clusters(
    session: SessionInput, since: datetime, min_filings: int
) -> list[tuple[str, list[RawEvent]]]:
    """Group ``sec_form4`` events since ``since`` by ticker.

    Returns ``(ticker, members)`` pairs for tickers with at least ``min_filings``
    events (excluding ``duplicate``/``failed`` and events with no ticker). Used by
    ``Form4ClusterJob`` to decide which tickers form a cluster.
    """
    async with _session_scope(session) as s:
        stmt = (
            select(RawEvent)
            .where(
                RawEvent.source == "sec_form4",
                RawEvent.fetched_at >= since,
                RawEvent.ticker.is_not(None),
                RawEvent.status.in_(CLUSTER_STATUSES),
            )
            .order_by(RawEvent.ticker, RawEvent.fetched_at)
        )
        rows = list((await s.execute(stmt)).scalars().all())
    groups: dict[str, list[RawEvent]] = {}
    for event in rows:
        assert event.ticker is not None  # filtered by the query above
        groups.setdefault(event.ticker, []).append(event)
    return [(ticker, members) for ticker, members in groups.items() if len(members) >= min_filings]
