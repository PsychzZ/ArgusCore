from collections.abc import Awaitable, Callable
from fastapi import FastAPI
from fastapi.responses import JSONResponse


def create_app(
    db_ping: Callable[[], Awaitable[bool]],
    scheduler_running: bool,
    last_run_iso: str | None,
) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None)

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        checks = {
            "db": await db_ping(),
            "scheduler": scheduler_running,
            "last_run": last_run_iso,
        }
        healthy = checks["db"] and checks["scheduler"]
        return JSONResponse(checks, status_code=200 if healthy else 503)

    return app
