from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from argus.common.config import WatchlistConfig
from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent
from argus.llm.prompts import Classification, ClassifyResult, LlmCallMeta
from argus.processor.classifier import Classifier
from argus.processor.filters import WatchlistFilter
from argus.processor.worker import ProcessorWorker


@pytest.fixture
async def db(testcontainer_postgres):
    engine = create_engine(testcontainer_postgres)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Truncate so each test starts from a clean slate — the postgres
        # testcontainer is session-scoped and shared with other integration
        # modules, which leave rows behind that would otherwise leak in.
        # ``sorted_tables`` is parent-first; reverse so child rows
        # (notifications, llm_calls) are deleted before their raw_events
        # parents, avoiding FK violations on tables without ON DELETE CASCADE.
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())
    sessions = create_session_factory(engine)
    yield engine, sessions
    await engine.dispose()


async def test_worker_classifies_new_events(db):
    _engine, sessions = db
    async with sessions() as session:
        for i in range(3):
            session.add(
                RawEvent(
                    source="rss",
                    external_id=f"e{i}",
                    content_hash=f"h{i}",
                    ticker="NVDA" if i < 2 else None,
                    title="t",
                    body="b",
                    url="u",
                )
            )
        await session.commit()

    llm = AsyncMock()
    llm.classify.return_value = ClassifyResult(
        classification=Classification(
            sentiment="positive", relevance_score=85, summary="ok"
        ),
        meta=LlmCallMeta(
            provider="deepseek",
            model="deepseek-chat",
            prompt_tokens=100,
            completion_tokens=20,
            cost_usd=0.0001,
            latency_ms=200,
        ),
    )
    wl = WatchlistConfig.model_validate(
        {"watchlist": [{"ticker": "NVDA", "sector": "X"}], "keywords": [], "thresholds": {}}
    )
    classifier = Classifier(filter_=WatchlistFilter(wl), llm=llm)
    worker = ProcessorWorker(sessions=sessions, classifier=classifier, batch_size=10)

    processed = await worker.run_once()
    # 2 with NVDA ticker get classified; 1 without gets skipped via classify()
    assert processed == 2

    async with sessions() as session:
        events = (await session.execute(select(RawEvent))).scalars().all()
        statuses = {e.external_id: e.status for e in events}
        # All 3 get processed by worker (it pulls 'new' events); classify() handles skip
        assert all(s in ("processed", "skipped") for s in statuses.values())
