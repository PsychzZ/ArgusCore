import pytest
from pydantic import ValidationError

from argus.common.models import RawEvent
from argus.llm.prompts import (
    SYSTEM_PROMPT,
    Classification,
    ClassifyResult,
    LlmCallMeta,
    build_user_prompt,
)


def test_classification_valid():
    c = Classification(sentiment="positive", relevance_score=85, summary="CEO bought shares.")
    assert c.relevance_score == 85


def test_classification_invalid_score():
    with pytest.raises(ValidationError):
        Classification(sentiment="positive", relevance_score=150, summary="x")


def test_classification_invalid_sentiment():
    with pytest.raises(ValidationError):
        Classification(sentiment="bullish", relevance_score=80, summary="x")


def test_classification_summary_too_long():
    with pytest.raises(ValidationError):
        Classification(sentiment="positive", relevance_score=80, summary="x" * 200)


def test_user_prompt_contains_event_fields():
    e = RawEvent(
        source="sec_form4", external_id="x", content_hash="h",
        ticker="NVDA", title="Form 4 - Huang Jensen",
        body="Transaction Code: P, Shares: 50000, Value: $2.4M",
        url="https://sec.gov/...",
    )
    prompt = build_user_prompt(e, watchlist_note="NVDA is on watchlist (Semiconductors).")
    assert "NVDA" in prompt
    assert "Form 4" in prompt
    assert "Semiconductors" in prompt


def test_system_prompt_is_stable():
    assert "ArgusCore" in SYSTEM_PROMPT
    assert "0-100" in SYSTEM_PROMPT


def test_llm_call_meta_dataclass():
    meta = LlmCallMeta(
        provider="deepseek", model="deepseek-chat",
        prompt_tokens=100, completion_tokens=30,
        cost_usd=0.0001, latency_ms=480,
    )
    assert meta.provider == "deepseek"
    assert meta.prompt_tokens == 100


def test_classify_result_dataclass():
    cls = Classification(sentiment="positive", relevance_score=92, summary="ok")
    meta = LlmCallMeta(provider="deepseek", model="deepseek-chat",
                       prompt_tokens=10, completion_tokens=5, cost_usd=0.0001, latency_ms=100)
    result = ClassifyResult(classification=cls, meta=meta)
    assert result.classification.relevance_score == 92
    assert result.meta.provider == "deepseek"
