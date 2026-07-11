import functools
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from fastapi import FastAPI
from fastapi.responses import JSONResponse


class LastRunTracker:
    """Mutable holder for the last successful scheduler-tick timestamp."""

    def __init__(self) -> None:
        self.iso: str | None = None

    def mark(self) -> None:
        self.iso = datetime.now(UTC).isoformat()


def tracked[T](
    job: Callable[[], Awaitable[T]], tracker: LastRunTracker
) -> Callable[[], Awaitable[T]]:
    """Wrap a scheduler job so the tracker is marked after each successful run."""

    @functools.wraps(job)
    async def wrapper() -> T:
        result = await job()
        tracker.mark()
        return result

    return wrapper


def create_app(
    db_ping: Callable[[], Awaitable[bool]],
    scheduler_running: bool,
    last_run: LastRunTracker,
) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None)

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        checks = {
            "db": await db_ping(),
            "scheduler": scheduler_running,
            "last_run": last_run.iso,
        }
        healthy = checks["db"] and checks["scheduler"]
        return JSONResponse(checks, status_code=200 if healthy else 503)

    return app
