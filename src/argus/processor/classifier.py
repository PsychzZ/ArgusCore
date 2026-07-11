from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from argus.common.logging import get_logger
from argus.common.models import LlmCall, RawEvent
from argus.llm.base import LLMProvider
from argus.processor.filters import Action, WatchlistFilter

log = get_logger(__name__)


class Classifier:
    """Orchestrates the pre-filter and the LLM provider for a single event.

    The worker (Task 21) opens a session per event and hands it here so the
    classifier can mutate the row and append the cost-tracking ``LlmCall``
    record in the same transaction. SKIP results short-circuit the LLM call;
    failures are logged and the event is marked ``failed`` with a retry bump
    rather than raised, so the worker can keep draining the queue.
    """

    def __init__(self, filter_: WatchlistFilter, llm: LLMProvider) -> None:
        self._filter = filter_
        self._llm = llm

    async def classify(self, session: AsyncSession, event: RawEvent) -> None:
        pre = self._filter.classify(event)
        if pre.action == Action.SKIP:
            event.status = "skipped"
            log.info("processor.skipped", event_id=str(event.id), ticker=event.ticker)
            return

        try:
            result = await self._llm.classify(event)
        except Exception as e:
            log.exception("processor.classify_failed", event_id=str(event.id))
            event.retry_count += 1
            event.error_message = str(e)
            event.status = "failed" if event.retry_count >= 3 else "new"
            return

        cls = result.classification
        meta = result.meta
        event.relevance_score = cls.relevance_score
        event.sentiment = cls.sentiment
        event.llm_summary = cls.summary
        event.llm_classified_at = datetime.now(UTC)
        event.status = "processed"

        session.add(
            LlmCall(
                event_id=event.id,
                provider=meta.provider,
                model=meta.model,
                prompt_tokens=meta.prompt_tokens,
                completion_tokens=meta.completion_tokens,
                cost_usd=meta.cost_usd,
                latency_ms=meta.latency_ms,
            )
        )
        log.info(
            "processor.classified",
            event_id=str(event.id),
            score=cls.relevance_score,
            cost_usd=meta.cost_usd,
        )
