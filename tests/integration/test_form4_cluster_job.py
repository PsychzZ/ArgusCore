from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, Form4Cluster, RawEvent
from argus.notifier.form4_cluster import Form4ClusterJob


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


def _mk_form4(external_id, ticker, filer_name, role, ttype, value, **kw) -> RawEvent:
    return RawEvent(
        source="sec_form4",
        external_id=external_id,
        content_hash=f"h-{external_id}",
        ticker=ticker,
        title=f"Form 4 - {filer_name} ({role})",
        body="b",
        url="u",
        status=kw.get("status", "new"),
        poller_meta={
            "filer_name": filer_name,
            "filer_role": role,
            "transaction_type": ttype,
            "value_usd": value,
        },
        **{k: v for k, v in kw.items() if k != "status"},
    )


async def test_cluster_sends_once_and_records_row(db):
    discord = AsyncMock()
    discord.send_embed.return_value = True
    async with db() as session:
        session.add_all(
            [
                _mk_form4("a", "NVDA", "Alice", "CFO", "P", 100_000),
                _mk_form4("b", "NVDA", "Bob", "Director", "S", 50_000),
            ]
        )
        await session.commit()

    job = Form4ClusterJob(sessions=db, discord=discord, window_h=48, min_filings=2)
    assert await job.run_once() == 1
    discord.send_embed.assert_called_once()

    # Second run must not re-alert the open cluster.
    assert await job.run_once() == 0
    discord.send_embed.assert_called_once()

    async with db() as session:
        from sqlalchemy import select

        row = (await session.execute(select(Form4Cluster))).scalars().one()
        assert row.ticker == "NVDA"
        assert row.alerted_at is not None
        assert len(row.member_event_ids) == 2


async def test_single_filing_no_alert(db):
    discord = AsyncMock()
    async with db() as session:
        session.add(_mk_form4("a", "NVDA", "Alice", "CFO", "P", 100_000))
        await session.commit()
    job = Form4ClusterJob(sessions=db, discord=discord, window_h=48, min_filings=2)
    assert await job.run_once() == 0
    discord.send_embed.assert_not_called()


async def test_old_filing_excluded(db):
    discord = AsyncMock()
    async with db() as session:
        session.add_all(
            [
                _mk_form4("a", "NVDA", "Alice", "CFO", "P", 100_000),
                _mk_form4(
                    "b",
                    "NVDA",
                    "Bob",
                    "Director",
                    "S",
                    50_000,
                    fetched_at=datetime.now(UTC) - timedelta(hours=49),
                ),
            ]
        )
        await session.commit()
    job = Form4ClusterJob(sessions=db, discord=discord, window_h=48, min_filings=2)
    assert await job.run_once() == 0
    discord.send_embed.assert_not_called()


async def test_send_failure_retries(db):
    discord = AsyncMock()
    discord.send_embed.return_value = False
    async with db() as session:
        session.add_all(
            [
                _mk_form4("a", "NVDA", "Alice", "CFO", "P", 100_000),
                _mk_form4("b", "NVDA", "Bob", "Director", "S", 50_000),
            ]
        )
        await session.commit()
    job = Form4ClusterJob(sessions=db, discord=discord, window_h=48, min_filings=2)
    assert await job.run_once() == 0
    async with db() as session:
        from sqlalchemy import select

        assert (await session.execute(select(Form4Cluster))).scalars().first() is None


async def test_multiple_tickers_one_embed_each(db):
    discord = AsyncMock()
    discord.send_embed.return_value = True
    async with db() as session:
        session.add_all(
            [
                _mk_form4("a", "NVDA", "Alice", "CFO", "P", 100_000),
                _mk_form4("b", "NVDA", "Bob", "Director", "S", 50_000),
                _mk_form4("c", "TSLA", "Carol", "CEO", "P", 200_000),
                _mk_form4("d", "TSLA", "Dan", "", "P", 80_000),
            ]
        )
        await session.commit()
    job = Form4ClusterJob(sessions=db, discord=discord, window_h=48, min_filings=2)
    assert await job.run_once() == 2
    assert discord.send_embed.call_count == 2
