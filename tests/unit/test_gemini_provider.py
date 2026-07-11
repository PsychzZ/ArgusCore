import httpx
import pytest
import respx

from argus.common.models import RawEvent
from argus.llm.gemini import GeminiProvider
from argus.llm.prompts import ClassifyResult, LlmCallMeta

GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
)


def make_event():
    return RawEvent(
        source="sec_form4",
        external_id="x",
        content_hash="h",
        ticker="NVDA",
        title="Form 4",
        body="Transaction Code: P",
        url="u",
    )


def gemini_response(content: str, prompt_tokens: int = 100, completion_tokens: int = 30):
    return {
        "candidates": [{"content": {"parts": [{"text": content}]}}],
        "usageMetadata": {
            "promptTokenCount": prompt_tokens,
            "candidatesTokenCount": completion_tokens,
            "totalTokenCount": prompt_tokens + completion_tokens,
        },
    }


@pytest.mark.asyncio
async def test_classify_success():
    with respx.mock:
        respx.post(GEMINI_URL).respond(
            200,
            json=gemini_response(
                '{"sentiment":"positive","relevance_score":92,"summary":"CEO bought shares"}'
            ),
        )
        provider = GeminiProvider(api_key="test-key", model="gemini-2.5-flash")
        result = await provider.classify(make_event(), watchlist_note="NVDA on watchlist.")
        assert isinstance(result, ClassifyResult)
        assert result.classification.sentiment == "positive"
        assert result.classification.relevance_score == 92
        assert isinstance(result.meta, LlmCallMeta)
        assert result.meta.provider == "gemini"
        assert result.meta.prompt_tokens == 100
        assert result.meta.completion_tokens == 30
        assert result.meta.cost_usd > 0
        await provider.close()


@pytest.mark.asyncio
async def test_classify_invalid_json_reprompt():
    with respx.mock as mock:
        route = mock.post(GEMINI_URL).mock(
            side_effect=[
                httpx.Response(200, json=gemini_response("not json")),
                httpx.Response(
                    200,
                    json=gemini_response(
                        '{"sentiment":"positive","relevance_score":80,"summary":"ok"}',
                        completion_tokens=20,
                    ),
                ),
            ]
        )
        provider = GeminiProvider(api_key="test-key", model="gemini-2.5-flash")
        result = await provider.classify(make_event())
        assert result.classification.relevance_score == 80
        assert route.call_count == 2
        await provider.close()


@pytest.mark.asyncio
async def test_classify_raises_after_max_reprompts():
    with respx.mock as mock:
        mock.post(GEMINI_URL).mock(return_value=httpx.Response(200, json=gemini_response("junk")))
        provider = GeminiProvider(api_key="test-key", model="gemini-2.5-flash")
        with pytest.raises(ValueError):
            await provider.classify(make_event())
        await provider.close()
