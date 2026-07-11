import asyncio
import functools
import random
from collections.abc import Awaitable, Callable
from typing import ParamSpec, TypeVar

import httpx

from argus.common.logging import get_logger

log = get_logger(__name__)
P = ParamSpec("P")
T = TypeVar("T")

MAX_RETRY_AFTER_S = 60.0


def _parse_retry_after(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        # HTTP-date variant — rare from our APIs; fall back to backoff
        return None


def with_retry(
    max_attempts: int = 3,
    retryable_status: frozenset[int] = frozenset({429, 500, 502, 503, 504}),
    backoff_base: float = 1.0,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    def decorator(fn: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        @functools.wraps(fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            attempt = 0
            while True:
                attempt += 1
                try:
                    return await fn(*args, **kwargs)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code not in retryable_status or attempt >= max_attempts:
                        raise
                    delay = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                    retry_after = _parse_retry_after(e.response)
                    if retry_after is not None:
                        delay = min(retry_after, MAX_RETRY_AFTER_S)
                    log.warning(
                        "http_retry",
                        attempt=attempt,
                        status=e.response.status_code,
                        delay=delay,
                        error=str(e),
                    )
                    await asyncio.sleep(delay)

        return wrapper

    return decorator


def make_client(timeout: float = 30.0, user_agent: str | None = None) -> httpx.AsyncClient:
    headers = {"User-Agent": user_agent} if user_agent else None
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=10.0),
        transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        follow_redirects=True,
        headers=headers,
    )
