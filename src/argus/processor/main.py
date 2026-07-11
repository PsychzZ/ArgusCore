import asyncio

import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from argus.common.config import Settings, load_watchlist
from argus.common.db import create_engine, create_session_factory
from argus.common.health import create_app
from argus.common.logging import get_logger, setup_logging
from argus.llm import create_provider
from argus.processor.classifier import Classifier
from argus.processor.filters import WatchlistFilter
from argus.processor.worker import ProcessorWorker

log = get_logger(__name__)


async def ping(engine: AsyncEngine) -> bool:
    try:
        async with engine.connect() as conn:
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

    llm = create_provider(settings)
    classifier = Classifier(filter_=WatchlistFilter(watchlist), llm=llm)
    worker = ProcessorWorker(sessions=sessions, classifier=classifier)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(worker.run_once, IntervalTrigger(seconds=30))
    scheduler.start()
    log.info("processor.scheduler_started")

    app = create_app(db_ping=lambda: ping(engine), scheduler_running=True, last_run_iso=None)
    config = uvicorn.Config(app, host="0.0.0.0", port=settings.health_port, log_config=None)
    server = uvicorn.Server(config)

    try:
        await server.serve()
    finally:
        scheduler.shutdown(wait=False)
        await llm.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
