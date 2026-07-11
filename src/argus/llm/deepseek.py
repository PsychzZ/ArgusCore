import json
import time
from typing import Any

from argus.common.http import make_client, with_retry
from argus.common.logging import get_logger
from argus.common.models import RawEvent
from argus.llm.prompts import (
    SYSTEM_PROMPT,
    Classification,
    ClassifyResult,
    LlmCallMeta,
    build_user_prompt,
)

log = get_logger(__name__)


class DeepSeekProvider:
    name = "deepseek"
    API_URL = "https://api.deepseek.com/chat/completions"

    def __init__(
        self,
        api_key: str,
        model: str = "deepseek-chat",
        input_cost_per_m: float = 0.14,
        output_cost_per_m: float = 0.28,
    ) -> None:
        self.model = model
        self._client = make_client()
        self._client.headers["Authorization"] = f"Bearer {api_key}"
        self._input_cost_per_m = input_cost_per_m
        self._output_cost_per_m = output_cost_per_m

    async def classify(self, event: RawEvent, watchlist_note: str | None = None) -> ClassifyResult:
        user_prompt = build_user_prompt(event, watchlist_note)
        for attempt in range(2):
            payload = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                "response_format": {"type": "json_object"},
                "stream": False,
            }
            t0 = time.monotonic()
            data = await self._call(payload)
            latency_ms = int((time.monotonic() - t0) * 1000)
            usage = data.get("usage", {})
            content = data["choices"][0]["message"]["content"]
            from pydantic import ValidationError

            try:
                parsed = json.loads(content)
                cls = Classification.model_validate(parsed)
            except (json.JSONDecodeError, ValidationError) as e:
                log.warning(
                    "llm.invalid_json", event_id=str(event.id), attempt=attempt, error=str(e)
                )
                if attempt == 0:
                    user_prompt = (
                        "Previous response was not valid JSON. "
                        "Please respond with valid JSON only.\n\n" + user_prompt
                    )
                    continue
                raise ValueError(f"LLM returned invalid JSON twice: {e}") from e

            pt = usage.get("prompt_tokens", 0)
            ct = usage.get("completion_tokens", 0)
            cost = (pt / 1_000_000) * self._input_cost_per_m + (
                ct / 1_000_000
            ) * self._output_cost_per_m
            meta = LlmCallMeta(
                provider=self.name,
                model=self.model,
                prompt_tokens=pt,
                completion_tokens=ct,
                cost_usd=cost,
                latency_ms=latency_ms,
            )
            log.info(
                "llm.classified",
                event_id=str(event.id),
                prompt_tokens=pt,
                completion_tokens=ct,
                cost_usd=cost,
                latency_ms=latency_ms,
            )
            return ClassifyResult(classification=cls, meta=meta)
        raise RuntimeError("unreachable")

    @with_retry()
    async def _call(self, payload: dict[str, Any]) -> dict[str, Any]:
        res = await self._client.post(self.API_URL, json=payload)
        res.raise_for_status()
        data: dict[str, Any] = res.json()
        return data

    async def close(self) -> None:
        await self._client.aclose()
