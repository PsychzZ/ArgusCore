import httpx
import pytest


@pytest.mark.asyncio
async def test_with_retry_succeeds_after_503():
    from argus.common.http import with_retry

    call_count = 0

    @with_retry(max_attempts=3, retryable_status={503})
    async def fetch():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise httpx.HTTPStatusError(
                "503", request=httpx.Request("GET", "https://x"), response=httpx.Response(503)
            )
        return httpx.Response(200, text="ok")

    res = await fetch()
    assert res.status_code == 200
    assert call_count == 3


@pytest.mark.asyncio
async def test_with_retry_no_retry_on_404():
    from argus.common.http import with_retry

    call_count = 0

    @with_retry(max_attempts=3, retryable_status={503})
    async def fetch():
        nonlocal call_count
        call_count += 1
        raise httpx.HTTPStatusError(
            "404", request=httpx.Request("GET", "https://x"), response=httpx.Response(404)
        )

    with pytest.raises(httpx.HTTPStatusError):
        await fetch()
    assert call_count == 1


@pytest.mark.asyncio
async def test_with_retry_honors_retry_after_header(monkeypatch):
    import argus.common.http as http_mod
    from argus.common.http import with_retry

    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(http_mod.asyncio, "sleep", fake_sleep)
    call_count = 0

    @with_retry(max_attempts=2, backoff_base=100.0)
    async def fetch():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise httpx.HTTPStatusError(
                "429",
                request=httpx.Request("GET", "https://x"),
                response=httpx.Response(429, headers={"Retry-After": "7"}),
            )
        return "ok"

    assert await fetch() == "ok"
    assert sleeps == [7.0]


@pytest.mark.asyncio
async def test_with_retry_caps_excessive_retry_after(monkeypatch):
    import argus.common.http as http_mod
    from argus.common.http import with_retry

    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(http_mod.asyncio, "sleep", fake_sleep)
    call_count = 0

    @with_retry(max_attempts=2)
    async def fetch():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise httpx.HTTPStatusError(
                "429",
                request=httpx.Request("GET", "https://x"),
                response=httpx.Response(429, headers={"Retry-After": "3600"}),
            )
        return "ok"

    assert await fetch() == "ok"
    assert sleeps == [60.0]


@pytest.mark.asyncio
async def test_make_client_sets_user_agent_and_follows_redirects():
    from argus.common.http import make_client

    client = make_client(user_agent="ArgusCore/1.0 (test@example.com)")
    assert client.headers["User-Agent"] == "ArgusCore/1.0 (test@example.com)"
    assert client.follow_redirects is True
    await client.aclose()


@pytest.mark.asyncio
async def test_make_client_default_has_no_custom_user_agent():
    from argus.common.http import make_client

    client = make_client()
    assert "httpx" in client.headers["User-Agent"]
    await client.aclose()


@pytest.mark.asyncio
async def test_with_retry_max_attempts_exceeded():
    from argus.common.http import with_retry

    call_count = 0

    @with_retry(max_attempts=2, retryable_status={503}, backoff_base=0.01)
    async def fetch():
        nonlocal call_count
        call_count += 1
        raise httpx.HTTPStatusError(
            "503", request=httpx.Request("GET", "https://x"), response=httpx.Response(503)
        )

    with pytest.raises(httpx.HTTPStatusError):
        await fetch()
    assert call_count == 2
