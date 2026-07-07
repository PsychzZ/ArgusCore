import httpx
import pytest
import respx

from argus.common.models import RawEvent
from argus.llm.deepseek import DeepSeekProvider
from argus.llm.prompts import ClassifyResult, LlmCallMeta


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


@pytest.mark.asyncio
async def test_classify_success():
    with respx.mock:
        respx.post("https://api.deepseek.com/chat/completions").respond(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"sentiment":"positive","relevance_score":92,'
                                '"summary":"CEO bought shares"}'
                            )
                        }
                    }
                ],
                "usage": {"prompt_tokens": 100, "completion_tokens": 30, "total_tokens": 130},
            },
        )
        provider = DeepSeekProvider(api_key="sk-test", model="deepseek-chat")
        result = await provider.classify(make_event(), watchlist_note="NVDA on watchlist.")
        assert isinstance(result, ClassifyResult)
        assert result.classification.sentiment == "positive"
        assert result.classification.relevance_score == 92
        assert isinstance(result.meta, LlmCallMeta)
        assert result.meta.prompt_tokens == 100
        assert result.meta.completion_tokens == 30
        assert result.meta.cost_usd > 0
        await provider.close()


@pytest.mark.asyncio
async def test_classify_invalid_json_reprompt():
    with respx.mock as mock:
        route1 = mock.post("https://api.deepseek.com/chat/completions").mock(
            side_effect=[
                httpx.Response(
                    200,
                    json={"choices": [{"message": {"content": "not json"}}], "usage": {}},
                ),
                httpx.Response(
                    200,
                    json={
                        "choices": [
                            {
                                "message": {
                                    "content": (
                                        '{"sentiment":"positive","relevance_score":80,'
                                        '"summary":"ok"}'
                                    )
                                }
                            }
                        ],
                        "usage": {
                            "prompt_tokens": 100,
                            "completion_tokens": 20,
                            "total_tokens": 120,
                        },
                    },
                ),
            ]
        )
        provider = DeepSeekProvider(api_key="sk-test", model="deepseek-chat")
        result = await provider.classify(make_event())
        assert result.classification.relevance_score == 80
        assert route1.call_count == 2
        await provider.close()


@pytest.mark.asyncio
async def test_classify_raises_after_max_reprompts():
    with respx.mock as mock:
        mock.post("https://api.deepseek.com/chat/completions").mock(
            return_value=httpx.Response(
                200,
                json={"choices": [{"message": {"content": "junk"}}], "usage": {}},
            )
        )
        provider = DeepSeekProvider(api_key="sk-test", model="deepseek-chat")
        with pytest.raises(ValueError):
            await provider.classify(make_event())
        await provider.close()
