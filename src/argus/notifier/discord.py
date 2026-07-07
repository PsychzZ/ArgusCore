import asyncio
from typing import Any

import httpx

from argus.common.logging import get_logger
from argus.common.models import RawEvent

log = get_logger(__name__)

COLOR_POSITIVE = 3066993  # green
COLOR_NEGATIVE = 15158332  # red
COLOR_NEUTRAL = 9807270  # grey

_SOURCE_LABEL = {"sec_form4": "SEC Form 4", "rss": "RSS"}


def _color_for(sentiment: str | None) -> int:
    if sentiment == "positive":
        return COLOR_POSITIVE
    if sentiment == "negative":
        return COLOR_NEGATIVE
    return COLOR_NEUTRAL


def _format_value(meta: dict[str, Any] | None) -> str:
    if not meta:
        return "n/a"
    value = meta.get("value_usd")
    if value is None:
        return "n/a"
    return f"${value / 1_000_000:.2f}M" if value >= 1_000_000 else f"${value / 1000:.0f}k"


def build_embed(event: RawEvent) -> dict[str, Any]:
    source_label = _SOURCE_LABEL.get(event.source, event.source)
    title_parts = []
    if event.poller_meta and event.poller_meta.get("transaction_type"):
        code = event.poller_meta["transaction_type"]
        if code == "P":
            action = "INSIDER BUY"
        elif code == "S":
            action = "INSIDER SALE"
        else:
            action = f"FORM 4 ({code})"
        title_parts.append(action)
    parts = [*title_parts, event.ticker, source_label] if event.ticker else [source_label]
    title = " | ".join(parts)

    fields = [
        {"name": "Source", "value": source_label, "inline": True},
        {"name": "Relevance", "value": f"{event.relevance_score}/100", "inline": True},
        {"name": "Sentiment", "value": event.sentiment or "neutral", "inline": True},
    ]
    if event.poller_meta and event.poller_meta.get("filer_name"):
        fields.append({"name": "Filer", "value": event.poller_meta["filer_name"], "inline": True})
        fields.append({"name": "Value", "value": _format_value(event.poller_meta), "inline": True})
    if event.llm_summary:
        fields.append({"name": "Summary", "value": event.llm_summary})

    return {
        "embeds": [
            {
                "title": title[:256],
                "url": event.url,
                "color": _color_for(event.sentiment),
                "fields": fields,
                "footer": {"text": "ArgusCore • DeepSeek"},
            }
        ]
    }


class DiscordClient:
    def __init__(
        self, webhook_url: str, max_attempts: int = 3, retry_backoff_base: float = 1.0
    ) -> None:
        self._url = webhook_url
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(15.0))
        self._max_attempts = max_attempts
        self._backoff_base = retry_backoff_base

    async def send_embed(self, payload: dict[str, Any]) -> bool:
        attempt = 0
        while True:
            attempt += 1
            try:
                res = await self._client.post(self._url, json=payload)
                if res.status_code == 429:
                    retry_after = float(res.headers.get("Retry-After", self._backoff_base))
                    if attempt >= self._max_attempts:
                        log.warning("discord.rate_limited_giving_up", attempts=attempt)
                        return False
                    log.warning("discord.rate_limited", retry_after=retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                res.raise_for_status()
                return True
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                retryable = status in {500, 502, 503, 504}
                if not retryable or attempt >= self._max_attempts:
                    log.warning("discord.http_error", status=status)
                    return False
                await asyncio.sleep(self._backoff_base * (2 ** (attempt - 1)))
            except Exception:
                log.exception("discord.send_failed")
                return False

    async def close(self) -> None:
        await self._client.aclose()
