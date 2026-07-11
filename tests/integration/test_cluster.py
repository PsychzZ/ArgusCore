from datetime import UTC, datetime, timedelta

import pytest

from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent
from argus.processor.cluster import find_form4_clusters


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


def _mk(**kw) -> RawEvent:
    defaults = dict(
        source="sec_form4",
        external_id=kw.get("external_id", "x1"),
        content_hash="h",
        ticker="NVDA",
        title="t",
        body="b",
        url="u",
        status="new",
        poller_meta={"transaction_type": "P", "value_usd": 1},
    )
    defaults.update(kw)
    return RawEvent(**defaults)


async def test_groups_by_ticker_respects_min(db):
    async with db() as session:
        session.add_all(
            [
                _mk(external_id="a"),
                _mk(external_id="b"),
                _mk(external_id="c", ticker="TSLA"),
            ]
        )
        await session.commit()
    since = datetime.now(UTC) - timedelta(hours=48)
    result = await find_form4_clusters(session=db, since=since, min_filings=2)
    tickers = {t for t, _ in result}
    assert tickers == {"NVDA"}  # TSLA has only 1


async def test_excludes_old_filings(db):
    async with db() as session:
        session.add_all(
            [
                _mk(external_id="a"),
                _mk(external_id="b", fetched_at=datetime.now(UTC) - timedelta(hours=49)),
            ]
        )
        await session.commit()
    since = datetime.now(UTC) - timedelta(hours=48)
    result = await find_form4_clusters(session=db, since=since, min_filings=2)
    assert result == []


async def test_excludes_null_ticker(db):
    async with db() as session:
        session.add_all([_mk(external_id="a"), _mk(external_id="b", ticker=None)])
        await session.commit()
    since = datetime.now(UTC) - timedelta(hours=48)
    result = await find_form4_clusters(session=db, since=since, min_filings=2)
    assert result == []


async def test_excludes_non_cluster_status(db):
    async with db() as session:
        session.add_all([_mk(external_id="a"), _mk(external_id="b", status="duplicate")])
        await session.commit()
    since = datetime.now(UTC) - timedelta(hours=48)
    result = await find_form4_clusters(session=db, since=since, min_filings=2)
    assert result == []
