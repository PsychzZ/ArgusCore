from datetime import UTC, datetime, timedelta

import pytest

from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent
from argus.processor.dedup import find_duplicate


@pytest.fixture
async def db(testcontainer_postgres):
    engine = create_engine(testcontainer_postgres)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())
    sessions = create_session_factory(engine)
    yield sessions
    await engine.dispose()


def make_event(**kw) -> RawEvent:
    defaults = dict(
        source="rss",
        external_id=kw.get("external_id", "x1"),
        content_hash="h",
        ticker="NVDA",
        title="NVIDIA CEO buys 50,000 shares",
        body="b",
        url="u",
        status="processed",
    )
    defaults.update(kw)
    return RawEvent(**defaults)


async def test_finds_rephrased_duplicate_same_ticker(db):
    async with db() as session:
        original = make_event(external_id="a")
        session.add(original)
        await session.commit()
        candidate = make_event(
            external_id="b",
            source="sec_form4",
            title="Nvidia CEO Huang buys 50000 shares!",
        )
        session.add(candidate)
        await session.flush()
        dup = await find_duplicate(session, candidate)
        assert dup is not None and dup.id == original.id


async def test_no_duplicate_outside_window(db):
    async with db() as session:
        old = make_event(
            external_id="a",
            fetched_at=datetime.now(UTC) - timedelta(hours=49),
        )
        session.add(old)
        await session.commit()
        candidate = make_event(external_id="b")
        session.add(candidate)
        await session.flush()
        assert await find_duplicate(session, candidate) is None


async def test_no_ticker_only_matches_same_source(db):
    async with db() as session:
        session.add(make_event(external_id="a", ticker=None, source="rss"))
        await session.commit()
        candidate = make_event(external_id="b", ticker=None, source="sec_form4")
        session.add(candidate)
        await session.flush()
        assert await find_duplicate(session, candidate) is None
