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


def with_retry(
    max_attempts: int = 3,
    retryable_status: set[int] = {429, 500, 502, 503, 504},
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
                    log.warning(
                        "http_retry", attempt=attempt, status=e.response.status_code,
                        delay=delay, error=str(e),
                    )
                    await asyncio.sleep(delay)
        return wrapper
    return decorator


def make_client(timeout: float = 30.0) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=10.0),
        transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
    )
