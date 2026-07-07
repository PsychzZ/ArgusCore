import hashlib
import re

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.common.config import WatchlistConfig
from argus.common.http import with_retry
from argus.common.logging import get_logger
from argus.common.models import PollingState, RawEvent
from argus.rss_poller.parser import parse_feed

log = get_logger(__name__)

# 1-5 capital letter tokens — matched candidates are then filtered against the
# watchlist ticker set so we only treat actual watched tickers as matches.
TICKER_RE = re.compile(r"\b[A-Z]{1,5}\b")


class RssPipeline:
    """Fetch an RSS feed, filter by ticker/keyword, and persist survivors.

    Idempotent: a re-run with the same items will not insert duplicates thanks
    to the ``(source, external_id)`` unique constraint on ``raw_events``, and a
    ``PollingState`` cursor row keyed ``rss:<feed url>`` is upserted each run.
    """

    SOURCE = "rss"

    def __init__(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        watchlist: WatchlistConfig,
        feed: dict[str, str],
    ) -> None:
        self._client = client
        self._sessions = session_factory
        self._watchlist = watchlist
        self._feed = feed
        self._cursor_key = f"rss:{feed['url']}"
        self._ticker_set = {w["ticker"] for w in watchlist.watchlist}
        self._keywords_lower = {kw.lower() for kw in watchlist.keywords}

    @with_retry()
    async def fetch(self) -> bytes:
        res = await self._client.get(self._feed["url"])
        res.raise_for_status()
        content: bytes = res.content
        return content

    async def run(self) -> int:
        try:
            xml = await self.fetch()
        except Exception as e:
            log.warning("rss.fetch_failed", url=self._feed["url"], error=str(e))
            return 0

        items = parse_feed(xml, base_url=self._feed["url"])
        inserted = 0
        async with self._sessions() as session:
            for item in items:
                if not self._is_relevant(item.title, item.body):
                    continue

                existing = await session.execute(
                    select(RawEvent).where(
                        RawEvent.source == self.SOURCE,
                        RawEvent.external_id == item.guid,
                    )
                )
                if existing.scalar_one_or_none() is not None:
                    continue

                ticker = self._extract_ticker(item.title)
                content_hash = hashlib.sha256(item.guid.encode()).hexdigest()
                session.add(
                    RawEvent(
                        source=self.SOURCE,
                        external_id=item.guid,
                        content_hash=content_hash,
                        ticker=ticker,
                        title=item.title,
                        body=item.body,
                        url=item.url,
                    )
                )
                inserted += 1

            await self._upsert_cursor(session, items[0].guid if items else "")
            await session.commit()

        log.info(
            "rss.run_complete",
            feed=self._feed["name"],
            items_seen=len(items),
            inserted=inserted,
        )
        return inserted

    async def _upsert_cursor(self, session: AsyncSession, cursor_value: str) -> None:
        existing = await session.get(PollingState, self._cursor_key)
        if existing is None:
            session.add(PollingState(source=self._cursor_key, cursor=cursor_value))
        else:
            existing.cursor = cursor_value

    def _is_relevant(self, title: str, body: str) -> bool:
        if self._extract_ticker(title) is not None:
            return True
        text = f"{title} {body}".lower()
        return any(kw in text for kw in self._keywords_lower)

    def _extract_ticker(self, text: str) -> str | None:
        for match in TICKER_RE.findall(text):
            if match in self._ticker_set:
                return str(match)
        return None
