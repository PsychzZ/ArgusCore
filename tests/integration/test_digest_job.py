from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, PollingState, RawEvent
from argus.notifier.digest import DigestJob


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


def _mk(external_id: str, score: int, status: str = "processed") -> RawEvent:
    return RawEvent(
        source="rss",
        external_id=external_id,
        content_hash="h",
        ticker="NVDA",
        title=f"story {external_id}",
        body="b",
        url="u",
        status=status,
        relevance_score=score,
    )


async def test_digest_sends_band_and_marks_sent(db):
    discord = AsyncMock()
    discord.send_embed.return_value = True
    async with db() as session:
        session.add_all([_mk("in-band", 75), _mk("instant", 95), _mk("low", 40)])
        await session.commit()

    job = DigestJob(sessions=db, discord=discord, min_score=70, instant_score=90)
    sent = await job.run_once()

    assert sent == 1
    discord.send_embed.assert_called_once()
    payload = discord.send_embed.call_args.args[0]
    assert "embeds" in payload  # full webhook payload, not bare embed
    async with db() as session:
        result = await session.execute(select(RawEvent))
        by_id = {e.external_id: e for e in result.scalars()}
        assert by_id["in-band"].status == "sent"
        assert by_id["instant"].status == "processed"  # belongs to instant path
        assert by_id["low"].status == "processed"
        cursor = await session.get(PollingState, "digest:last_sent")
        assert cursor is not None


async def test_digest_empty_band_sends_nothing(db):
    discord = AsyncMock()
    job = DigestJob(sessions=db, discord=discord, min_score=70, instant_score=90)
    assert await job.run_once() == 0
    discord.send_embed.assert_not_called()


async def test_digest_send_failure_keeps_cursor(db):
    discord = AsyncMock()
    discord.send_embed.return_value = False
    async with db() as session:
        session.add(_mk("in-band", 75))
        await session.commit()

    job = DigestJob(sessions=db, discord=discord, min_score=70, instant_score=90)
    assert await job.run_once() == 0
    async with db() as session:
        assert await session.get(PollingState, "digest:last_sent") is None
        result = await session.execute(select(RawEvent))
        assert result.scalars().one().status == "processed"
