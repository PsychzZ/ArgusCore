from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.common.logging import get_logger
from argus.common.models import RawEvent
from argus.processor.classifier import Classifier

log = get_logger(__name__)


class ProcessorWorker:
    """Drains batches of ``status='new'`` events through the classifier.

    Each batch is pulled with ``SELECT ... FOR UPDATE SKIP LOCKED`` so multiple
    workers can run concurrently without contending on the same rows. The
    classifier mutates each row (and writes the ``LlmCall`` cost record) inside
    the worker's transaction; SKIP results are counted separately from
    successfully classified events so the return value reflects real LLM
    throughput. ``classify()`` is responsible for marking failures with a retry
    bump rather than raising, so the loop keeps draining the queue even when a
    single event blows up the LLM call.
    """

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        classifier: Classifier,
        batch_size: int = 20,
    ) -> None:
        self._sessions = sessions
        self._classifier = classifier
        self._batch_size = batch_size

    async def run_once(self) -> int:
        processed = 0
        async with self._sessions() as session:
            result = await session.execute(
                select(RawEvent)
                .where(RawEvent.status == "new")
                .order_by(RawEvent.fetched_at)
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            events = list(result.scalars().all())
            for event in events:
                try:
                    await self._classifier.classify(session, event)
                except Exception:
                    log.exception("worker.event_failed", event_id=str(event.id))
                    event.retry_count += 1
                    if event.retry_count >= 3:
                        event.status = "failed"
                    else:
                        event.status = "new"
                    continue
                # Only count events the classifier actually pushed through the
                # LLM; skipped/failed rows are tracked separately for throughput
                # visibility.
                if event.status == "processed":
                    processed += 1
            await session.commit()
        log.info("processor.batch_complete", processed=processed)
        return processed
