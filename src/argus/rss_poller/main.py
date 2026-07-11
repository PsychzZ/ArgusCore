import asyncio

import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import text

from argus.common.config import Settings, load_feeds, load_watchlist
from argus.common.db import create_engine, create_session_factory
from argus.common.health import create_app
from argus.common.http import make_client
from argus.common.logging import get_logger, setup_logging
from argus.rss_poller.pipeline import RssPipeline

log = get_logger(__name__)


async def run_all(pipelines: list[RssPipeline]) -> None:
    for p in pipelines:
        try:
            await p.run()
        except Exception:
            log.exception("rss.run_failed", feed=p._feed["name"])


async def ping(engine: object) -> bool:
    try:
        async with engine.connect() as conn:  # type: ignore[attr-defined]
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


async def main() -> None:
    settings = Settings()
    setup_logging(settings.log_level)
    watchlist = load_watchlist(settings.watchlist_path)
    feeds_cfg = load_feeds(settings.feeds_path)

    engine = create_engine(settings.database_url)
    sessions = create_session_factory(engine)
    # SEC feeds reject requests without a descriptive User-Agent
    client = make_client(user_agent=settings.sec_user_agent)

    pipelines = [
        RssPipeline(client=client, session_factory=sessions, watchlist=watchlist, feed=feed)
        for feed in feeds_cfg.get("feeds", [])
    ]

    interval = feeds_cfg.get("defaults", {}).get("poll_interval_seconds", 600)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_all, IntervalTrigger(seconds=interval), args=[pipelines])
    scheduler.start()
    log.info("rss.scheduler_started", feed_count=len(pipelines), interval_s=interval)

    app = create_app(db_ping=lambda: ping(engine), scheduler_running=True, last_run_iso=None)
    config = uvicorn.Config(app, host="0.0.0.0", port=settings.health_port, log_config=None)
    server = uvicorn.Server(config)

    try:
        await server.serve()
    finally:
        scheduler.shutdown(wait=False)
        await client.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
