from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, PollingState, RawEvent
from argus.sec_poller.pipeline import Form4Filter, SecPipeline

FIXTURE = Path(__file__).parent.parent / "fixtures" / "sec" / "form4_sample.xml"


@pytest.fixture
async def db(testcontainer_postgres):
    engine = create_engine(testcontainer_postgres)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Truncate so each test starts from a clean slate — the postgres
        # testcontainer is session-scoped and shared with test_db.py, which
        # leaves rows behind that would otherwise leak into our assertions.
        for table in Base.metadata.sorted_tables:
            await conn.execute(table.delete())
    sessions = create_session_factory(engine)
    yield engine, sessions
    await engine.dispose()


async def test_pipeline_inserts_filtered_event(db):
    _engine, sessions = db
    client = AsyncMock()
    client.list_recent_form4_urls.return_value = ["https://www.sec.gov/Archives/edgar/data/123/abc/index.xml"]
    client.fetch_filing_xml.return_value = FIXTURE.read_bytes()

    pipeline = SecPipeline(
        client=client,
        session_factory=sessions,
        form4_filter=Form4Filter(buy_min_usd=100_000, sell_min_usd=1_000_000),
        source_cursor_key="sec_form4",
    )
    inserted = await pipeline.run(since="2026-07-04")
    assert inserted == 1

    async with sessions() as session:
        result = await session.execute(select(RawEvent))
        events = result.scalars().all()
        assert len(events) == 1
        e = events[0]
        assert e.source == "sec_form4"
        assert e.ticker == "NVDA"
        assert e.status == "new"
        assert e.poller_meta["transaction_type"] == "P"
        assert e.poller_meta["value_usd"] == 2_410_000.0


async def test_pipeline_skips_below_threshold(db):
    _engine, sessions = db
    client = AsyncMock()
    client.list_recent_form4_urls.return_value = ["https://www.sec.gov/x"]
    # Make a small purchase: modify fixture in-memory
    xml = FIXTURE.read_text()
    xml = xml.replace("<value>50000</value>", "<value>100</value>")
    client.fetch_filing_xml.return_value = xml.encode()

    pipeline = SecPipeline(
        client=client, session_factory=sessions,
        form4_filter=Form4Filter(buy_min_usd=100_000, sell_min_usd=1_000_000),
        source_cursor_key="sec_form4",
    )
    inserted = await pipeline.run(since="2026-07-04")
    assert inserted == 0


async def test_pipeline_writes_cursor(db):
    _engine, sessions = db
    client = AsyncMock()
    client.list_recent_form4_urls.return_value = []
    pipeline = SecPipeline(
        client=client, session_factory=sessions,
        form4_filter=Form4Filter(buy_min_usd=100_000, sell_min_usd=1_000_000),
        source_cursor_key="sec_form4",
    )
    await pipeline.run(since="2026-07-04")
    async with sessions() as session:
        result = await session.execute(
            select(PollingState).where(PollingState.source == "sec_form4")
        )
        state = result.scalar_one_or_none()
        assert state is not None
        assert state.cursor == "2026-07-04"
