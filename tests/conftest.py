import asyncio
import sys
from collections.abc import Mapping
from typing import Any

import pytest
from pytest_asyncio.plugin import LoopFactory
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]


def pytest_asyncio_loop_factories(
    config: pytest.Config,
    item: pytest.Item,
) -> Mapping[str, LoopFactory] | None:
    """Force a SelectorEventLoop on Windows for psycopg3 async compatibility.

    Psycopg 3's async mode cannot run on ``ProactorEventLoop`` (the Windows
    default). Registering a ``SelectorEventLoop`` factory through the modern
    ``pytest_asyncio_loop_factories`` hook lets the real-Postgres integration
    tests (testcontainers) drive async I/O through psycopg. POSIX keeps its
    default Selector loop, so we return ``None`` there to opt out.
    """
    if sys.platform == "win32":
        return {"selector": lambda: asyncio.SelectorEventLoop()}
    return None


@pytest.fixture(scope="session")
def testcontainer_postgres() -> Any:
    with PostgresContainer("postgres:16-alpine", driver="psycopg") as pg:
        yield pg.get_connection_url()
