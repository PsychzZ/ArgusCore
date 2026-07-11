import asyncio
from datetime import UTC, datetime, timedelta

import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text

from argus.common.config import Settings, load_watchlist
from argus.common.db import create_engine, create_session_factory
from argus.common.health import LastRunTracker, create_app, tracked
from argus.common.logging import get_logger, setup_logging
from argus.sec_poller.edgar import EdgarClient
from argus.sec_poller.pipeline import Form4Filter, SecPipeline

log = get_logger(__name__)


async def run_once(pipeline: SecPipeline) -> None:
    since = (datetime.now(UTC) - timedelta(days=2)).strftime("%Y-%m-%d")
    try:
        await pipeline.run(since=since)
    except Exception:
        log.exception("sec.run_failed")


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

    engine = create_engine(settings.database_url)
    sessions = create_session_factory(engine)
    client = EdgarClient(user_agent=settings.sec_user_agent)
    pipeline = SecPipeline(
        client=client,
        session_factory=sessions,
        form4_filter=Form4Filter(
            buy_min_usd=watchlist.thresholds["form4_buy_min_usd"],
            sell_min_usd=watchlist.thresholds["form4_sell_min_usd"],
        ),
    )

    last_run = LastRunTracker()
    scheduler = AsyncIOScheduler()
    job = tracked(lambda: run_once(pipeline), last_run)
    scheduler.add_job(job, CronTrigger(hour="*", minute=0))
    scheduler.start()
    log.info("sec.scheduler_started")

    app = create_app(
        db_ping=lambda: ping(engine),
        scheduler_running=True,
        last_run=last_run,
    )
    config = uvicorn.Config(app, host="0.0.0.0", port=settings.health_port, log_config=None)
    server = uvicorn.Server(config)

    try:
        await server.serve()
    finally:
        scheduler.shutdown(wait=False)
        await client.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
