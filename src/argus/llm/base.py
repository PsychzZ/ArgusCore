from typing import Protocol

from argus.common.models import RawEvent
from argus.llm.prompts import ClassifyResult


class LLMProvider(Protocol):
    name: str
    model: str

    async def classify(
        self, event: RawEvent, watchlist_note: str | None = None
    ) -> ClassifyResult: ...

    async def close(self) -> None: ...
