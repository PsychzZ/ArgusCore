from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.common.logging import get_logger
from argus.common.models import Notification, RawEvent
from argus.notifier.discord import DiscordClient, build_embed

log = get_logger(__name__)

_MAX_RETRIES = 3


class NotifierWorker:
    """Drains batches of ``status='processed'`` events above the relevance
    threshold into Discord.

    Mirrors the processor worker: rows are pulled with
    ``SELECT ... FOR UPDATE SKIP LOCKED`` so concurrent notifier instances don't
    fight over the same events. On success the event is marked ``sent`` and a
    ``Notification(delivered=True)`` row is written; on failure the retry counter
    is bumped and a ``Notification(delivered=False, error=...)`` row records the
    attempt. After three failures the event is parked as ``failed`` so the
    notifier stops resending it.
    """

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        discord: DiscordClient,
        min_score: int,
        batch_size: int = 10,
    ) -> None:
        self._sessions = sessions
        self._discord = discord
        self._min_score = min_score
        self._batch_size = batch_size

    async def run_once(self) -> int:
        sent_count = 0
        async with self._sessions() as session:
            result = await session.execute(
                select(RawEvent)
                .where(
                    RawEvent.status == "processed",
                    RawEvent.relevance_score >= self._min_score,
                )
                .order_by(RawEvent.relevance_score.desc())
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            events = list(result.scalars().all())
            for event in events:
                payload = build_embed(event)
                ok = await self._discord.send_embed(payload)
                if ok:
                    event.status = "sent"
                    event.sent_at = datetime.now(UTC)
                    session.add(Notification(event_id=event.id, channel="discord", delivered=True))
                    sent_count += 1
                else:
                    event.retry_count += 1
                    if event.retry_count >= _MAX_RETRIES:
                        event.status = "failed"
                    session.add(
                        Notification(
                            event_id=event.id,
                            channel="discord",
                            delivered=False,
                            error=(
                                "max_retries_exceeded"
                                if event.retry_count >= _MAX_RETRIES
                                else "send_failed"
                            ),
                        )
                    )
            await session.commit()
        log.info("notifier.batch_complete", sent=sent_count)
        return sent_count
