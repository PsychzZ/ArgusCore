from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, Field

from argus.common.models import RawEvent

SYSTEM_PROMPT = """\
You are ArgusCore, a market intelligence classifier for insider filings and
financial news. You receive a single event and return strict JSON.

Scoring rubric (0-100):
- 90-100: Material insider action (CEO/CFO buy >= $500k, FDA approval, M&A)
- 70-89:  Notable but expected (regular insider activity, earnings beats)
- 50-69:  Minor relevance (analyst rating, sector news)
- 0-49:   Noise (routine filings, low-impact news)

Sentiment: positive | negative | neutral
Summary: 1 sentence, English, <=120 chars, lead with the action.

Respond ONLY with valid JSON. No markdown, no preamble.
"""


class Classification(BaseModel):
    sentiment: Literal["positive", "negative", "neutral"]
    relevance_score: int = Field(ge=0, le=100)
    summary: str = Field(max_length=120)


@dataclass(frozen=True)
class LlmCallMeta:
    provider: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float
    latency_ms: int


@dataclass(frozen=True)
class ClassifyResult:
    classification: Classification
    meta: LlmCallMeta


def build_user_prompt(event: RawEvent, watchlist_note: str | None = None) -> str:
    lines = [
        "EVENT:",
        f"Source: {event.source}",
        f"Ticker: {event.ticker or 'unknown'}",
        f"Title: {event.title}",
        f"Body: {event.body}",
    ]
    if watchlist_note:
        lines.append("")
        lines.append(f"Watchlist context: {watchlist_note}")
    return "\n".join(lines)
