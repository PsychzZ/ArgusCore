from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.common.logging import get_logger
from argus.common.models import Form4Cluster
from argus.notifier.discord import DiscordClient, build_cluster_embed
from argus.processor.cluster import find_form4_clusters

log = get_logger(__name__)


class Form4ClusterJob:
    """Detect Form-4 clusters and send one instant Discord embed per cluster.

    A cluster is >= ``min_filings`` ``sec_form4`` events for the same ticker within
    ``window_h`` hours. Idempotency: an "open" cluster row (``created_at`` within
    the window) suppresses re-alerts; later scans refresh its member set without
    re-sending. The embed is sent BEFORE the row is persisted, so a failed send is
    retried on the next scan (at-least-once). The job is a singleton cron trigger,
    so no row locking is needed.
    """

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        discord: DiscordClient,
        window_h: int,
        min_filings: int,
    ) -> None:
        self._sessions = sessions
        self._discord = discord
        self._window_h = window_h
        self._min_filings = min_filings

    async def run_once(self) -> int:
        now = datetime.now(UTC)
        since = now - timedelta(hours=self._window_h)
        sent = 0
        try:
            async with self._sessions() as session:
                candidates = await find_form4_clusters(session, since, self._min_filings)
                for ticker, members in candidates:
                    open_cluster = await self._find_open_cluster(session, ticker, now)
                    if open_cluster is not None:
                        open_cluster.member_event_ids = [str(m.id) for m in members]
                        open_cluster.window_end = now
                        continue
                    embed = build_cluster_embed(ticker, members)
                    ok = await self._discord.send_embed({"embeds": [embed]})
                    if not ok:
                        log.warning("cluster.send_failed", ticker=ticker)
                        continue
                    session.add(
                        Form4Cluster(
                            ticker=ticker,
                            member_event_ids=[str(m.id) for m in members],
                            window_start=since,
                            window_end=now,
                            alerted_at=now,
                        )
                    )
                    sent += 1
                await session.commit()
        except Exception:
            log.exception("cluster.run_failed")
            return 0
        log.info("cluster.run_complete", sent=sent)
        return sent

    async def _find_open_cluster(
        self, session: AsyncSession, ticker: str, now: datetime
    ) -> Form4Cluster | None:
        cutoff = now - timedelta(hours=self._window_h)
        result = await session.execute(
            select(Form4Cluster).where(
                Form4Cluster.ticker == ticker,
                Form4Cluster.created_at >= cutoff,
            )
        )
        return result.scalars().first()
