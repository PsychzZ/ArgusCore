import pytest
import respx

from argus.notifier.discord import DiscordClient


@pytest.mark.asyncio
async def test_send_embed_success():
    with respx.mock:
        route = respx.post("https://discord.com/api/webhooks/1/abc").respond(204)
        client = DiscordClient(webhook_url="https://discord.com/api/webhooks/1/abc")
        ok = await client.send_embed({"embeds": [{"title": "t"}]})
        assert ok is True
        assert route.call_count == 1
        await client.close()


@pytest.mark.asyncio
async def test_send_embed_rate_limit_retries():
    import httpx

    with respx.mock as mock:
        route = mock.post("https://discord.com/api/webhooks/1/abc").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "0.01"}),
                httpx.Response(204),
            ]
        )
        client = DiscordClient(
            webhook_url="https://discord.com/api/webhooks/1/abc",
            retry_backoff_base=0.01,
        )
        ok = await client.send_embed({"embeds": [{"title": "t"}]})
        assert ok is True
        assert route.call_count == 2
        await client.close()


def _mk(title: str, score: int, ticker: str = "NVDA"):
    from argus.common.models import RawEvent

    return RawEvent(
        source="rss",
        external_id=title,
        content_hash="h",
        ticker=ticker,
        title=title,
        body="b",
        url=f"https://x/{title}",
        status="processed",
        relevance_score=score,
    )


def test_digest_embed_sorts_and_caps_at_ten():
    from argus.notifier.discord import build_digest_embed

    events = [_mk(f"story {i}", score=70 + i) for i in range(12)]
    embed = build_digest_embed(events)
    lines = embed["description"].splitlines()
    assert "story 11" in lines[0]  # highest score first
    assert sum(1 for line in lines if line.startswith("**")) == 10
    assert embed["footer"]["text"] == "+2 weitere"
    assert "Digest" in embed["title"]


def test_digest_embed_no_footer_when_ten_or_fewer():
    from argus.notifier.discord import build_digest_embed

    embed = build_digest_embed([_mk("only one", score=75)])
    assert "footer" not in embed
