from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from argus.common.config import WatchlistConfig
from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, LlmCall, RawEvent
from argus.llm.prompts import Classification, ClassifyResult, LlmCallMeta
from argus.processor.classifier import Classifier
from argus.processor.filters import WatchlistFilter


@pytest.fixture
async def db(testcontainer_postgres):
    engine = create_engine(testcontainer_postgres)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Truncate so each test starts from a clean slate — the postgres
        # testcontainer is session-scoped and shared with other integration
        # modules, which leave rows behind that would otherwise leak in.
        for table in Base.metadata.sorted_tables:
            await conn.execute(table.delete())
    sessions = create_session_factory(engine)
    yield sessions
    await engine.dispose()


async def test_skipped_event_marked_skipped(db):
    llm = AsyncMock()
    wl = WatchlistConfig.model_validate(
        {"watchlist": [{"ticker": "NVDA", "sector": "X"}], "keywords": [], "thresholds": {}}
    )
    classifier = Classifier(filter_=WatchlistFilter(wl), llm=llm)
    e = RawEvent(
        source="rss",
        external_id="x",
        content_hash="h",
        title="random",
        body="boring",
        url="u",
    )
    async with db() as session:
        session.add(e)
        await session.flush()
        await classifier.classify(session, e)
        await session.commit()
    assert e.status == "skipped"
    llm.classify.assert_not_called()


async def test_classified_event_updates_fields_and_writes_llm_call(db):
    llm = AsyncMock()
    llm.classify.return_value = ClassifyResult(
        classification=Classification(
            sentiment="positive", relevance_score=92, summary="CEO bought shares."
        ),
        meta=LlmCallMeta(
            provider="deepseek",
            model="deepseek-chat",
            prompt_tokens=100,
            completion_tokens=30,
            cost_usd=0.0001,
            latency_ms=480,
        ),
    )
    wl = WatchlistConfig.model_validate(
        {"watchlist": [{"ticker": "NVDA", "sector": "X"}], "keywords": [], "thresholds": {}}
    )
    classifier = Classifier(filter_=WatchlistFilter(wl), llm=llm)
    e = RawEvent(
        source="sec_form4",
        external_id="x",
        content_hash="h",
        ticker="NVDA",
        title="t",
        body="b",
        url="u",
    )
    async with db() as session:
        session.add(e)
        await session.flush()
        await classifier.classify(session, e)
        await session.commit()

    assert e.status == "processed"
    assert e.relevance_score == 92
    assert e.sentiment == "positive"
    assert e.llm_summary == "CEO bought shares."
    assert e.llm_classified_at is not None

    async with db() as session:
        calls = (await session.execute(select(LlmCall))).scalars().all()
        assert len(calls) == 1
        assert calls[0].provider == "deepseek"
        assert calls[0].prompt_tokens == 100
        # cost_usd is Numeric(10,6) -> loads as Decimal; compare exactly.
        assert calls[0].cost_usd == Decimal("0.0001")
