from typing import Any

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def create_engine(database_url: str, **kwargs: Any) -> AsyncEngine:
    """Create the shared async SQLAlchemy engine for ArgusCore services.

    Sensible connection-pool defaults are applied (5 base connections, 10
    overflow, pre-ping to recover stale connections, hourly recycle). Callers
    may override or extend via ``kwargs`` — anything accepted by
    :func:`sqlalchemy.ext.asyncio.create_async_engine` is forwarded.
    """
    return create_async_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600,
        **kwargs,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Build a session factory bound to ``engine``.

    ``expire_on_commit=False`` keeps loaded attributes readable after commit,
    which is what the pollers/processor/notifier need when emitting follow-up
    work based on just-committed state.
    """
    return async_sessionmaker(engine, expire_on_commit=False)
