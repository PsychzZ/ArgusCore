from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent
from argus.notifier.worker import NotifierWorker


@pytest.fixture
async def db(testcontainer_postgres):
    engine = create_engine(testcontainer_postgres)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Truncate so each test starts from a clean slate — the postgres
        # testcontainer is session-scoped and shared with other integration
        # modules, which leave rows behind that would otherwise leak in.
        # ``sorted_tables`` is parent-first; child tables (notifications,
        # llm_calls) must be deleted *before* their raw_events parents to
        # avoid FK violations on tables that lack ON DELETE CASCADE.
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())
    sessions = create_session_factory(engine)
    yield sessions
    await engine.dispose()


async def test_sends_only_above_threshold(db):
    discord = AsyncMock()
    discord.send_embed.return_value = True
    async with db() as session:
        session.add(
            RawEvent(
                source="sec_form4",
                external_id="hi",
                content_hash="h1",
                ticker="NVDA",
                title="t",
                body="b",
                url="u",
                status="processed",
                relevance_score=85,
            )
        )
        session.add(
            RawEvent(
                source="sec_form4",
                external_id="lo",
                content_hash="h2",
                ticker="AMD",
                title="t",
                body="b",
                url="u",
                status="processed",
                relevance_score=40,
            )
        )
        await session.commit()

    worker = NotifierWorker(sessions=db, discord=discord, min_score=70)
    sent = await worker.run_once()
    assert sent == 1
    discord.send_embed.assert_called_once()

    async with db() as session:
        result = await session.execute(select(RawEvent))
        events = {e.external_id: e.status for e in result.scalars().all()}
        assert events["hi"] == "sent"
        assert events["lo"] == "processed"  # remains


async def test_failed_send_keeps_status_processed(db):
    discord = AsyncMock()
    discord.send_embed.return_value = False
    async with db() as session:
        session.add(
            RawEvent(
                source="sec_form4",
                external_id="x",
                content_hash="h",
                ticker="NVDA",
                title="t",
                body="b",
                url="u",
                status="processed",
                relevance_score=85,
            )
        )
        await session.commit()

    worker = NotifierWorker(sessions=db, discord=discord, min_score=70)
    sent = await worker.run_once()
    assert sent == 0
    async with db() as session:
        e = (await session.execute(select(RawEvent))).scalar_one()
        assert e.status == "processed"
        assert e.retry_count == 1
