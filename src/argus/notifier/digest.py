from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.common.logging import get_logger
from argus.common.models import Notification, PollingState, RawEvent
from argus.notifier.discord import DiscordClient, build_digest_embed

log = get_logger(__name__)

CURSOR_KEY = "digest:last_sent"
FIRST_RUN_WINDOW_H = 24


class DigestJob:
    """Sends one daily summary embed for events in the digest score band.

    Band: min_score <= relevance_score < instant_score. Events >= instant_score
    were already pinged by the instant path (NotifierWorker). The cursor only
    advances after a successful send (at-least-once delivery).
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
            cursor_row = await session.get(PollingState, CURSOR_KEY)
            since = (
                datetime.fromisoformat(cursor_row.cursor)
                if cursor_row
                else now - timedelta(hours=FIRST_RUN_WINDOW_H)
            )
            result = await session.execute(
                select(RawEvent)
                .where(
                    RawEvent.status == "processed",
                    RawEvent.relevance_score >= self._min_score,
                    RawEvent.relevance_score < self._instant_score,
                    RawEvent.fetched_at >= since,
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
            if cursor_row is None:
                session.add(PollingState(source=CURSOR_KEY, cursor=now.isoformat()))
            else:
                cursor_row.cursor = now.isoformat()
            await session.commit()
            log.info("digest.sent", count=len(events))
            return len(events)
