from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, PollingState, RawEvent
from argus.sec_poller.edgar import parse_form4
from argus.sec_poller.pipeline import Form4Filter, SecPipeline

FIXTURE = Path(__file__).parent.parent / "fixtures" / "sec" / "form4_sample.xml"

_FORM4_XML = b"""<ownershipDocument>
  <issuer><issuerTradingSymbol>NVDA</issuerTradingSymbol></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Jane Doe</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><officerTitle>CFO</officerTitle></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable><nonDerivativeTransaction>
    <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
    <transactionAmounts>
      <transactionShares><value>1000</value></transactionShares>
      <transactionPricePerShare><value>10.5</value></transactionPricePerShare>
    </transactionAmounts>
  </nonDerivativeTransaction></nonDerivativeTable>
</ownershipDocument>"""


class _FakeClient:
    async def list_recent_form4_urls(self, since: str) -> list[str]:
        return ["https://sec.gov/archives/nvda/form4.xml"]

    async def fetch_filing_xml(self, url: str) -> bytes:
        return _FORM4_XML

    async def close(self) -> None:
        return None


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


async def test_pipeline_inserts_filtered_event(db):
    sessions = db
    client = AsyncMock()
    client.list_recent_form4_urls.return_value = [
        "https://www.sec.gov/Archives/edgar/data/123/abc/index.xml"
    ]
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
        events = (await session.execute(select(RawEvent))).scalars().all()
        assert len(events) == 1
        e = events[0]
        assert e.source == "sec_form4"
        assert e.ticker == "NVDA"
        assert e.status == "new"
        assert e.poller_meta["transaction_type"] == "P"
        assert e.poller_meta["value_usd"] == 2_410_000.0


async def test_pipeline_skips_below_threshold(db):
    sessions = db
    client = AsyncMock()
    client.list_recent_form4_urls.return_value = ["https://www.sec.gov/x"]
    xml = FIXTURE.read_text()
    xml = xml.replace("<value>50000</value>", "<value>100</value>")
    client.fetch_filing_xml.return_value = xml.encode()
    pipeline = SecPipeline(
        client=client,
        session_factory=sessions,
        form4_filter=Form4Filter(buy_min_usd=100_000, sell_min_usd=1_000_000),
        source_cursor_key="sec_form4",
    )
    inserted = await pipeline.run(since="2026-07-04")
    assert inserted == 0


async def test_pipeline_writes_cursor(db):
    sessions = db
    client = AsyncMock()
    client.list_recent_form4_urls.return_value = []
    pipeline = SecPipeline(
        client=client,
        session_factory=sessions,
        form4_filter=Form4Filter(buy_min_usd=100_000, sell_min_usd=1_000_000),
        source_cursor_key="sec_form4",
    )
    await pipeline.run(since="2026-07-04")
    async with sessions() as session:
        state = (
            await session.execute(select(PollingState).where(PollingState.source == "sec_form4"))
        ).scalar_one_or_none()
        assert state is not None
        assert state.cursor == "2026-07-04"


async def test_pipeline_stores_filer_role_in_poller_meta(db):
    pipeline = SecPipeline(
        client=_FakeClient(),
        session_factory=db,
        form4_filter=Form4Filter(buy_min_usd=1, sell_min_usd=1),
    )
    inserted = await pipeline.run(since="2020-01-01")
    assert inserted == 1
    async with db() as session:
        event = (await session.execute(select(RawEvent))).scalars().one()
        assert event.poller_meta["filer_role"] == "CFO"
        assert event.poller_meta["filer_name"] == "Jane Doe"
        assert event.poller_meta["transaction_type"] == "P"


def test_parse_form4_role_from_title():
    data = parse_form4(_FORM4_XML)
    assert data.filer_role == "CFO"
