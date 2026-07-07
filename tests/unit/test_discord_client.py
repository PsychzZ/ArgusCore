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
