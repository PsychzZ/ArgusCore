import pytest

from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent


@pytest.fixture
async def db_url(testcontainer_postgres):
    return testcontainer_postgres


async def test_can_insert_and_query(db_url):
    engine = create_engine(db_url)
    session_factory = create_session_factory(engine)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with session_factory() as session:
        e = RawEvent(
            source="sec_form4",
            external_id="ext-1",
            content_hash="h",
            title="t",
            body="b",
            url="u",
        )
        session.add(e)
        await session.commit()
    async with session_factory() as session:
        from sqlalchemy import select

        result = await session.execute(select(RawEvent).where(RawEvent.external_id == "ext-1"))
        assert result.scalar_one().source == "sec_form4"
    await engine.dispose()
