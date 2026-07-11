import pytest

from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent
from argus.sec_poller.edgar import parse_form4
from argus.sec_poller.pipeline import Form4Filter, SecPipeline

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


async def test_pipeline_stores_filer_role_in_poller_meta(db):
    pipeline = SecPipeline(
        client=_FakeClient(),
        session_factory=db,
        form4_filter=Form4Filter(buy_min_usd=1, sell_min_usd=1),
    )
    inserted = await pipeline.run(since="2020-01-01")
    assert inserted == 1

    async with db() as session:
        from sqlalchemy import select

        event = (await session.execute(select(RawEvent))).scalars().one()
        assert event.poller_meta["filer_role"] == "CFO"
        assert event.poller_meta["filer_name"] == "Jane Doe"
        assert event.poller_meta["transaction_type"] == "P"


def test_parse_form4_role_from_title():
    data = parse_form4(_FORM4_XML)
    assert data.filer_role == "CFO"
