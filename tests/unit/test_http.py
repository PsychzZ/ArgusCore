import httpx
import pytest
import respx


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
