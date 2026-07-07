from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.common.config import WatchlistConfig
from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent
from argus.rss_poller.pipeline import RssPipeline

FIXTURE = Path(__file__).parent.parent / "fixtures" / "rss" / "rss20.xml"


@pytest.fixture
async def db(testcontainer_postgres: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(testcontainer_postgres)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Truncate to start from a clean slate — the postgres testcontainer is
        # session-scoped and shared with the other integration tests, which
        # would otherwise leak rows into our assertions. ``sorted_tables`` is
        # parent-first; reverse so child rows (notifications, llm_calls) are
        # deleted before their raw_events parents, avoiding FK violations on
        # tables without ON DELETE CASCADE.
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())
    sessions = create_session_factory(engine)
    yield sessions
    await engine.dispose()


@pytest.fixture
def watchlist() -> WatchlistConfig:
    return WatchlistConfig.model_validate({
        "watchlist": [{"ticker": "NVDA", "sector": "Semiconductors"}],
        "keywords": ["Phase 3"],
        "thresholds": {},
    })


class _FakeResponse:
    """Minimal httpx.Response stand-in: ``content`` (bytes) + sync ``raise_for_status``."""

    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


async def test_inserts_only_matching_items(
    db: async_sessionmaker[AsyncSession],
    watchlist: WatchlistConfig,
) -> None:
    client = AsyncMock()
    client.get = AsyncMock(return_value=_FakeResponse(FIXTURE.read_bytes()))

    pipeline = RssPipeline(
        client=client,
        session_factory=db,
        watchlist=watchlist,
        feed={"name": "TC", "url": "https://tc.com/feed", "category": "tech"},
    )
    inserted = await pipeline.run()
    # Item 1 has "NVDA" ticker; item 2 has nothing → 1 inserted
    assert inserted == 1
    async with db() as session:
        events = (await session.execute(select(RawEvent))).scalars().all()
        assert len(events) == 1
        assert events[0].title.startswith("Nvidia")


async def test_dedup_by_guid(
    db: async_sessionmaker[AsyncSession],
    watchlist: WatchlistConfig,
) -> None:
    client = AsyncMock()
    client.get = AsyncMock(return_value=_FakeResponse(FIXTURE.read_bytes()))
    pipeline = RssPipeline(
        client=client,
        session_factory=db,
        watchlist=watchlist,
        feed={"name": "TC", "url": "https://tc.com/feed", "category": "tech"},
    )
    await pipeline.run()
    inserted_again = await pipeline.run()
    assert inserted_again == 0
