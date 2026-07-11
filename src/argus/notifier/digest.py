from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.common.logging import get_logger
from argus.common.models import Notification, RawEvent
from argus.notifier.discord import DiscordClient, build_digest_embed

log = get_logger(__name__)


class DigestJob:
    """Sends one daily summary embed for events in the digest score band.

    Band: min_score <= relevance_score < instant_score. Events >= instant_score
    were already pinged by the instant path (NotifierWorker). Selection is
    purely status ("processed") plus score band — no time window or cursor.
    Marking picked rows "sent" provides exactly-once delivery; if the send
    fails, rows stay "processed" and are retried on the next run.
    """

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        discord: DiscordClient,
        min_score: int,
        instant_score: int,
    ) -> None:
        self._sessions = sessions
        self._discord = discord
        self._min_score = min_score
        self._instant_score = instant_score

    async def run_once(self) -> int:
        now = datetime.now(UTC)
        async with self._sessions() as session:
            result = await session.execute(
                select(RawEvent)
                .where(
                    RawEvent.status == "processed",
                    RawEvent.relevance_score >= self._min_score,
                    RawEvent.relevance_score < self._instant_score,
                )
                .with_for_update(skip_locked=True)
            )
            events = list(result.scalars().all())
            if not events:
                log.info("digest.empty")
                return 0

            ok = await self._discord.send_embed({"embeds": [build_digest_embed(events)]})
            if not ok:
                log.warning("digest.send_failed", count=len(events))
                return 0

            for event in events:
                event.status = "sent"
                event.sent_at = now
                session.add(Notification(event_id=event.id, channel="discord", delivered=True))
            await session.commit()
            log.info("digest.sent", count=len(events))
            return len(events)
