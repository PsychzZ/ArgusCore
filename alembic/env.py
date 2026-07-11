import asyncio
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from argus.common.config import Settings
from argus.common.models import Base

# psycopg (async mode) is incompatible with the ProactorEventLoop that
# asyncio uses by default on Windows. Force the SelectorEventLoop on Windows
# before anything constructs an event loop. Harmless on other platforms.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

settings = Settings()
config.set_main_option("sqlalchemy.url", settings.database_url)
target_metadata = Base.metadata


# Arbitrary but fixed key so concurrent container starts serialize their
# "alembic upgrade head" runs instead of racing each other.
MIGRATION_LOCK_KEY = 0x41524755  # "ARGU"


def do_run_migrations(connection: Connection) -> None:
    connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": MIGRATION_LOCK_KEY})
    try:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
    finally:
        connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": MIGRATION_LOCK_KEY})


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


run_migrations_online()
