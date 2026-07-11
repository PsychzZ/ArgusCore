import hashlib
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.common.logging import get_logger
from argus.common.models import PollingState, RawEvent
from argus.sec_poller.edgar import EdgarClient, parse_form4

log = get_logger(__name__)


class Form4Filter:
    """Threshold filter for Form 4 transactions.

    A "P" (purchase) is kept when its USD value is at least ``buy_min_usd``;
    an "S" (sale) is kept when its USD value is at least ``sell_min_usd``.
    Every other transaction code (A, G, M, ...) is dropped — the spec only
    surfaces open-market buys and sells.
    """

    def __init__(self, buy_min_usd: float, sell_min_usd: float) -> None:
        self.buy_min_usd = buy_min_usd
        self.sell_min_usd = sell_min_usd

    def keep(self, transaction_code: str, value_usd: float) -> bool:
        if transaction_code == "P":
            return value_usd >= self.buy_min_usd
        if transaction_code == "S":
            return value_usd >= self.sell_min_usd
        return False


class SecPipeline:
    """Fetch recent Form 4 filings, apply thresholds, and persist survivors.

    The pipeline is idempotent: re-running with the same URLs will not create
    duplicates because of the ``(source, external_id)`` unique constraint, and
    a ``PollingState`` cursor row is upserted each run so the scheduler can
    advance the ``since`` window.
    """

    SOURCE = "sec_form4"

    def __init__(
        self,
        client: EdgarClient,
        session_factory: async_sessionmaker[AsyncSession],
        form4_filter: Form4Filter,
        source_cursor_key: str = "sec_form4",
    ) -> None:
        self._client = client
        self._sessions = session_factory
        self._filter = form4_filter
        self._cursor_key = source_cursor_key

    async def run(self, since: str) -> int:
        urls = await self._client.list_recent_form4_urls(since=since)
        inserted = 0
        async with self._sessions() as session:
            for url in urls:
                try:
                    xml = await self._client.fetch_filing_xml(url)
                    data = parse_form4(xml)
                except Exception as e:  # log and move on — one bad URL shouldn't kill the run
                    log.warning("sec.parse_failed", url=url, error=str(e))
                    continue

                if not self._filter.keep(data.transaction_code, data.value_usd):
                    log.info(
                        "sec.filtered_out",
                        ticker=data.ticker,
                        code=data.transaction_code,
                        value_usd=data.value_usd,
                    )
                    continue

                external_id = url
                content_hash = hashlib.sha256(
                    f"{external_id}:{data.transaction_code}:{data.shares}".encode()
                ).hexdigest()

                existing = await session.execute(
                    select(RawEvent).where(
                        RawEvent.source == self.SOURCE,
                        RawEvent.external_id == external_id,
                    )
                )
                if existing.scalar_one_or_none() is not None:
                    continue

                event = RawEvent(
                    source=self.SOURCE,
                    external_id=external_id,
                    content_hash=content_hash,
                    ticker=data.ticker,
                    title=f"Form 4 - {data.filer_name} ({data.filer_role})",
                    body=(
                        f"Transaction Code: {data.transaction_code}\n"
                        f"Shares: {data.shares}\n"
                        f"Price: ${data.price_per_share}\n"
                        f"Total Value: ${data.value_usd:,.0f}\n"
                        f"Filer Role: {data.filer_role}"
                    ),
                    url=url,
                    poller_meta={
                        "filer_name": data.filer_name,
                        "transaction_type": data.transaction_code,
                        "value_usd": data.value_usd,
                    },
                )
                session.add(event)
                inserted += 1

            await self._upsert_cursor(session, since)
            await session.commit()

        log.info("sec.run_complete", since=since, urls_fetched=len(urls), inserted=inserted)
        return inserted

    async def _upsert_cursor(self, session: AsyncSession, since: str) -> None:
        existing = await session.get(PollingState, self._cursor_key)
        if existing is None:
            session.add(PollingState(source=self._cursor_key, cursor=since))
        else:
            existing.cursor = since
            existing.updated_at = datetime.now(UTC)
