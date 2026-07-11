# ArgusCore Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a 24/7 market-intelligence pipeline that ingests SEC Form 4 filings and RSS feeds, classifies them via LLM, and ships high-relevance alerts to Discord.

**Architecture:** Four Python services (sec-poller, rss-poller, processor, notifier) + Postgres, orchestrated by Docker Compose. Services communicate via shared DB (status column on `raw_events` acts as a queue). APScheduler runs in each service as in-process scheduler. LLM via DeepSeek API.

**Tech Stack:** Python 3.12, httpx, APScheduler, SQLAlchemy 2.0 (async) + Alembic, psycopg3, Pydantic v2, structlog, feedparser, pytest + testcontainers, ruff, mypy, uv.

**Spec reference:** `docs/superpowers/specs/2026-07-05-arguscore-design.md`

---

## Phases Overview

1. **Foundation** (Tasks 1–9): Project bootstrap, common modules (config, DB, models, HTTP, logging, health), Alembic migration, example configs
2. **SEC Poller** (Tasks 10–13): EDGAR XML parser, API client, pipeline, scheduler entrypoint
3. **RSS Poller** (Tasks 14–16): feedparser wrapper, pipeline, scheduler entrypoint
4. **Processor** (Tasks 17–22): Watchlist filter, LLM provider, classifier, worker, entrypoint
5. **Notifier** (Tasks 23–26): Discord embed builder, webhook client, worker, entrypoint
6. **Docker, CI, Polish** (Tasks 27–32): Dockerfile, Compose, pre-commit, GitHub Actions, README, E2E test

---

## Phase 1: Foundation

### Task 1: Project bootstrap

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `src/argus/__init__.py`
- Create: `src/argus/common/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/unit/__init__.py`

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[project]
name = "argus"
version = "0.1.0"
description = "Automated 24/7 data pipeline filtering alternative market data and whale activity into high-signal alerts."
requires-python = ">=3.12"
dependencies = [
    "httpx[http2]>=0.27",
    "apscheduler>=3.10",
    "sqlalchemy[asyncio]>=2.0",
    "alembic>=1.13",
    "psycopg[binary]>=3.1",
    "pydantic>=2.6",
    "pydantic-settings>=2.2",
    "pyyaml>=6.0",
    "structlog>=24.1",
    "feedparser>=6.0",
    "lxml>=5.1",
    "fastapi>=0.110",
    "uvicorn[standard]>=0.29",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "respx>=0.21",
    "testcontainers[postgres]>=4.0",
    "ruff>=0.4",
    "mypy>=1.9",
    "pre-commit>=3.7",
    "types-pyyaml>=6.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/argus"]

[tool.ruff]
target-version = "py312"
line-length = 100
src = ["src", "tests"]

[tool.ruff.lint]
select = ["E", "F", "I", "N", "W", "B", "UP", "RUF", "TID"]

[tool.ruff.lint.isort]
known-first-party = ["argus"]

[tool.mypy]
python_version = "3.12"
strict = true
plugins = ["pydantic.mypy"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 2: Write `.gitignore`**

```
__pycache__/
*.py[cod]
*.egg-info/
.venv/
.env
*.log
*.sqlite
.pytest_cache/
.mypy_cache/
.ruff_cache/
dist/
build/
node_modules/
.idea/
.vscode/
*.swp
```

- [ ] **Step 3: Create empty `__init__.py` files**

`src/argus/__init__.py`:
```python
__version__ = "0.1.0"
```

`src/argus/common/__init__.py`, `tests/__init__.py`, `tests/unit/__init__.py`: empty files.

- [ ] **Step 4: Install dependencies via uv**

Run: `uv sync --all-extras`
Expected: `.venv/` is created, no errors.

- [ ] **Step 5: Verify ruff and mypy work**

Run: `uv run ruff check . && uv run mypy src`
Expected: clean output (no errors).

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml .gitignore src/ tests/
git commit -m "feat: project bootstrap with uv, ruff, mypy config"
```

---

### Task 2: Common — Config module (Pydantic Settings + YAML loader)

**Files:**
- Create: `src/argus/common/config.py`
- Create: `tests/unit/test_config.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_config.py`:
```python
import pytest
from pydantic import ValidationError


def test_settings_loads_from_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "dbhost")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/1/abc")
    monkeypatch.setenv("SEC_USER_AGENT", "Test/1.0 (test@example.com)")
    from argus.common.config import Settings
    s = Settings()
    assert s.postgres_host == "dbhost"
    assert s.database_url == "postgresql+psycopg://argus:secret@dbhost:5432/argus"


def test_settings_requires_api_key(monkeypatch):
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    from argus.common.config import Settings
    with pytest.raises(ValidationError):
        Settings()


def test_watchlist_loads_yaml(tmp_path):
    yaml_file = tmp_path / "watchlist.yaml"
    yaml_file.write_text("""
watchlist:
  - ticker: NVDA
    sector: Semiconductors
keywords:
  - "insider buy"
thresholds:
  min_relevance_score: 70
  form4_buy_min_usd: 100000
  form4_sell_min_usd: 1000000
""")
    from argus.common.config import load_watchlist
    wl = load_watchlist(yaml_file)
    assert wl["watchlist"][0]["ticker"] == "NVDA"
    assert "insider buy" in wl["keywords"]
    assert wl["thresholds"]["form4_buy_min_usd"] == 100000
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL with `ImportError` (module not created).

- [ ] **Step 3: Implement `config.py`**

`src/argus/common/config.py`:
```python
from pathlib import Path
import yaml
from pydantic import BaseModel, Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Postgres
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "argus"
    postgres_user: str = "argus"
    postgres_password: str

    # LLM
    llm_provider: str = "deepseek"
    deepseek_api_key: str
    deepseek_model: str = "deepseek-chat"

    # Discord
    discord_webhook_url: str

    # SEC
    sec_user_agent: str

    # Operational
    log_level: str = "INFO"
    health_port: int = 8080

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


class WatchlistConfig(BaseModel):
    watchlist: list[dict[str, str]] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    thresholds: dict[str, int] = Field(default_factory=lambda: {
        "min_relevance_score": 70,
        "form4_buy_min_usd": 100_000,
        "form4_sell_min_usd": 1_000_000,
    })


def load_watchlist(path: str | Path) -> WatchlistConfig:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return WatchlistConfig.model_validate(data)


def load_feeds(path: str | Path) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/common/config.py tests/unit/test_config.py
git commit -m "feat(common): pydantic settings + yaml loaders"
```

---

### Task 3: Common — Structlog setup

**Files:**
- Create: `src/argus/common/logging.py`
- Create: `tests/unit/test_logging.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_logging.py`:
```python
import io
import json


def test_log_emits_json(capsys):
    from argus.common.logging import setup_logging, get_logger
    setup_logging("INFO")
    log = get_logger("test")
    log.info("hello", ticker="NVDA", score=92)
    out = capsys.readouterr().out.strip()
    parsed = json.loads(out)
    assert parsed["event"] == "hello"
    assert parsed["ticker"] == "NVDA"
    assert parsed["score"] == 92
    assert parsed["level"] == "info"


def test_log_respects_level(capsys):
    from argus.common.logging import setup_logging, get_logger
    setup_logging("WARNING")
    log = get_logger("test")
    log.info("should-not-appear")
    log.warning("should-appear")
    out = capsys.readouterr().out.strip()
    assert "should-not-appear" not in out
    assert "should-appear" in out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_logging.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `logging.py`**

`src/argus/common/logging.py`:
```python
import logging
import sys
import structlog

_configured = False


def setup_logging(level: str = "INFO") -> None:
    global _configured
    if _configured:
        return
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper())
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _configured = True


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_logging.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/common/logging.py tests/unit/test_logging.py
git commit -m "feat(common): structured JSON logging via structlog"
```

---

### Task 4: Common — SQLAlchemy models

**Files:**
- Create: `src/argus/common/models.py`
- Create: `tests/unit/test_models.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_models.py`:
```python
from datetime import datetime, UTC
from argus.common.models import RawEvent, LlmCall, Notification, PollingState


def test_raw_event_default_status():
    e = RawEvent(
        source="sec_form4",
        external_id="0001209191-24-000123",
        content_hash="abc",
        title="Form 4",
        body="...",
        url="https://sec.gov/...",
    )
    assert e.status == "new"
    assert e.retry_count == 0


def test_raw_event_status_setter():
    e = RawEvent(
        source="rss", external_id="guid-1", content_hash="x",
        title="t", body="b", url="u",
    )
    e.status = "processed"
    e.relevance_score = 85
    assert e.status == "processed"
    assert e.relevance_score == 85


def test_llm_call_relationship():
    call = LlmCall(
        event_id="00000000-0000-0000-0000-000000000000",
        provider="deepseek", model="deepseek-chat",
        prompt_tokens=100, completion_tokens=50,
        cost_usd=0.0001, latency_ms=480,
    )
    assert call.provider == "deepseek"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_models.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `models.py`**

`src/argus/common/models.py`:
```python
import uuid
from datetime import datetime, UTC
from sqlalchemy import (
    String, Text, Integer, BigInteger, ForeignKey, DateTime, Numeric, Index,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class RawEvent(Base):
    __tablename__ = "raw_events"
    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_source_external"),
        Index("idx_events_status", "status", "fetched_at"),
        Index("idx_events_score", "relevance_score", postgresql_where=text("status = 'processed'")),
        Index("idx_events_hash", "content_hash"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    external_id: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    ticker: Mapped[str | None] = mapped_column(String(16))
    title: Mapped[str] = mapped_column(Text, nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    status: Mapped[str] = mapped_column(String(16), default="new")
    error_message: Mapped[str | None] = mapped_column(Text)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)

    relevance_score: Mapped[int | None] = mapped_column(Integer)
    sentiment: Mapped[str | None] = mapped_column(String(16))
    llm_summary: Mapped[str | None] = mapped_column(Text)
    llm_classified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    poller_meta: Mapped[dict | None] = mapped_column(JSONB)

    llm_calls: Mapped[list["LlmCall"]] = relationship(back_populates="event")
    notifications: Mapped[list["Notification"]] = relationship(back_populates="event")


class LlmCall(Base):
    __tablename__ = "llm_calls"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("raw_events.id", ondelete="CASCADE")
    )
    provider: Mapped[str] = mapped_column(String(32))
    model: Mapped[str] = mapped_column(String(64))
    prompt_tokens: Mapped[int | None] = mapped_column(Integer)
    completion_tokens: Mapped[int | None] = mapped_column(Integer)
    cost_usd: Mapped[float | None] = mapped_column(Numeric(10, 6))
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    event: Mapped["RawEvent"] = relationship(back_populates="llm_calls")


class Notification(Base):
    __tablename__ = "notifications"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("raw_events.id")
    )
    channel: Mapped[str] = mapped_column(String(16))
    webhook_id: Mapped[str | None] = mapped_column(String(64))
    delivered: Mapped[bool] = mapped_column(default=False)
    discord_msg_id: Mapped[str | None] = mapped_column(String(64))
    error: Mapped[str | None] = mapped_column(Text)
    sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    event: Mapped["RawEvent"] = relationship(back_populates="notifications")


class PollingState(Base):
    __tablename__ = "polling_state"
    source: Mapped[str] = mapped_column(String(128), primary_key=True)
    cursor: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


# Required import at bottom to make `text` available in Index expression
from sqlalchemy import text  # noqa: E402
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_models.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/common/models.py tests/unit/test_models.py
git commit -m "feat(common): SQLAlchemy ORM models"
```

---

### Task 5: Common — Async DB engine + session factory

**Files:**
- Create: `src/argus/common/db.py`
- Create: `tests/integration/__init__.py`
- Create: `tests/integration/test_db.py`

- [ ] **Step 1: Write the failing test**

`tests/integration/test_db.py`:
```python
import pytest
from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent


@pytest.fixture
async def db_url(testcontainer_postgres):
    return testcontainer_postgres


async def test_can_insert_and_query(db_url):
    engine = create_engine(db_url)
    session_factory = create_session_factory(engine)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with session_factory() as session:
        e = RawEvent(
            source="sec_form4", external_id="ext-1",
            content_hash="h", title="t", body="b", url="u",
        )
        session.add(e)
        await session.commit()
    async with session_factory() as session:
        from sqlalchemy import select
        result = await session.execute(select(RawEvent).where(RawEvent.external_id == "ext-1"))
        assert result.scalar_one().source == "sec_form4"
    await engine.dispose()
```

- [ ] **Step 2: Add `conftest.py` with testcontainers fixture**

`tests/conftest.py`:
```python
import pytest
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def testcontainer_postgres():
    with PostgresContainer("postgres:16-alpine", driver="psycopg") as pg:
        yield pg.get_connection_url()
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_db.py -v`
Expected: FAIL with `ImportError` (db.py not created).

- [ ] **Step 4: Implement `db.py`**

`src/argus/common/db.py`:
```python
from sqlalchemy.ext.asyncio import (
    AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine,
)


def create_engine(database_url: str, **kwargs) -> AsyncEngine:
    return create_async_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600,
        **kwargs,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_db.py -v`
Expected: 1 passed (testcontainers spins up a Postgres container).

- [ ] **Step 6: Commit**

```bash
git add src/argus/common/db.py tests/conftest.py tests/integration/
git commit -m "feat(common): async db engine + session factory"
```

---

### Task 6: Alembic migration setup + initial migration

**Files:**
- Create: `alembic.ini`
- Create: `alembic/env.py`
- Create: `alembic/script.py.mako`
- Create: `alembic/versions/0001_initial.py`

- [ ] **Step 1: Write `alembic.ini`**

```ini
[alembic]
script_location = alembic
prepend_sys_path = src
version_path_separator = os

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console
qualname =

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S
```

- [ ] **Step 2: Write `alembic/env.py`**

```python
import asyncio
import os
from logging.config import fileConfig
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config
from alembic import context
from argus.common.models import Base
from argus.common.config import Settings

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

settings = Settings()
config.set_main_option("sqlalchemy.url", settings.database_url)
target_metadata = Base.metadata


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


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
```

- [ ] **Step 3: Write `alembic/script.py.mako`**

```mako
"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

revision: str = ${repr(up_revision)}
down_revision: Union[str, None] = ${repr(down_revision)}
branch_labels: Union[str, Sequence[str], None] = ${repr(branch_labels)}
depends_on: Union[str, Sequence[str], None] = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
```

- [ ] **Step 4: Write the initial migration**

`alembic/versions/0001_initial.py`:
```python
"""initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2026-07-05

"""
from typing import Sequence, Union
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "raw_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("external_id", sa.Text, nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("ticker", sa.String(16)),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("body", sa.Text, nullable=False),
        sa.Column("url", sa.Text, nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("status", sa.String(16), server_default="new", nullable=False),
        sa.Column("error_message", sa.Text),
        sa.Column("retry_count", sa.Integer, server_default="0", nullable=False),
        sa.Column("relevance_score", sa.Integer),
        sa.Column("sentiment", sa.String(16)),
        sa.Column("llm_summary", sa.Text),
        sa.Column("llm_classified_at", sa.DateTime(timezone=True)),
        sa.Column("sent_at", sa.DateTime(timezone=True)),
        sa.Column("poller_meta", postgresql.JSONB),
        sa.UniqueConstraint("source", "external_id", name="uq_source_external"),
    )
    op.create_index("idx_events_status", "raw_events", ["status", "fetched_at"])
    op.create_index(
        "idx_events_score", "raw_events", ["relevance_score"],
        postgresql_where=sa.text("status = 'processed'"),
    )
    op.create_index("idx_events_hash", "raw_events", ["content_hash"])

    op.create_table(
        "llm_calls",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("event_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("raw_events.id", ondelete="CASCADE"), nullable=False),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("model", sa.String(64), nullable=False),
        sa.Column("prompt_tokens", sa.Integer),
        sa.Column("completion_tokens", sa.Integer),
        sa.Column("cost_usd", sa.Numeric(10, 6)),
        sa.Column("latency_ms", sa.Integer),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("idx_llm_event", "llm_calls", ["event_id"])

    op.create_table(
        "notifications",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("event_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("raw_events.id"), nullable=False),
        sa.Column("channel", sa.String(16), nullable=False),
        sa.Column("webhook_id", sa.String(64)),
        sa.Column("delivered", sa.Boolean, server_default="false", nullable=False),
        sa.Column("discord_msg_id", sa.String(64)),
        sa.Column("error", sa.Text),
        sa.Column("sent_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )

    op.create_table(
        "polling_state",
        sa.Column("source", sa.String(128), primary_key=True),
        sa.Column("cursor", sa.Text, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("polling_state")
    op.drop_table("notifications")
    op.drop_table("llm_calls")
    op.drop_index("idx_events_hash", table_name="raw_events")
    op.drop_index("idx_events_score", table_name="raw_events")
    op.drop_index("idx_events_status", table_name="raw_events")
    op.drop_table("raw_events")
```

- [ ] **Step 5: Test migration against a fresh container**

Run: `uv run pytest tests/integration/test_db.py -v`
Expected: still 1 passed (uses `Base.metadata.create_all`, not Alembic — but Alembic env.py imports must work).

Then test Alembic directly:
```bash
export POSTGRES_PASSWORD=test; export DEEPSEEK_API_KEY=test; export DISCORD_WEBHOOK_URL=test; export SEC_USER_AGENT="test/1.0"
docker run -d --name pg-test -e POSTGRES_PASSWORD=test -e POSTGRES_USER=argus -e POSTGRES_DB=argus -p 5433:5432 postgres:16-alpine
export POSTGRES_HOST=localhost; export POSTGRES_PORT=5433
uv run alembic upgrade head
uv run alembic downgrade base
uv run alembic upgrade head
docker stop pg-test && docker rm pg-test
```
Expected: migration runs both directions cleanly, second `upgrade head` is idempotent.

- [ ] **Step 6: Commit**

```bash
git add alembic.ini alembic/
git commit -m "feat(db): alembic setup + initial migration"
```

---

### Task 7: Common — HTTP client with retry

**Files:**
- Create: `src/argus/common/http.py`
- Create: `tests/unit/test_http.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_http.py`:
```python
import httpx
import pytest
import respx


@pytest.mark.asyncio
async def test_with_retry_succeeds_after_503():
    from argus.common.http import with_retry
    call_count = 0

    @with_retry(max_attempts=3, retryable_status={503})
    async def fetch():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise httpx.HTTPStatusError(
                "503", request=httpx.Request("GET", "https://x"), response=httpx.Response(503)
            )
        return httpx.Response(200, text="ok")

    res = await fetch()
    assert res.status_code == 200
    assert call_count == 3


@pytest.mark.asyncio
async def test_with_retry_no_retry_on_404():
    from argus.common.http import with_retry
    call_count = 0

    @with_retry(max_attempts=3, retryable_status={503})
    async def fetch():
        nonlocal call_count
        call_count += 1
        raise httpx.HTTPStatusError(
            "404", request=httpx.Request("GET", "https://x"), response=httpx.Response(404)
        )

    with pytest.raises(httpx.HTTPStatusError):
        await fetch()
    assert call_count == 1


@pytest.mark.asyncio
async def test_with_retry_max_attempts_exceeded():
    from argus.common.http import with_retry
    call_count = 0

    @with_retry(max_attempts=2, retryable_status={503}, backoff_base=0.01)
    async def fetch():
        nonlocal call_count
        call_count += 1
        raise httpx.HTTPStatusError(
            "503", request=httpx.Request("GET", "https://x"), response=httpx.Response(503)
        )

    with pytest.raises(httpx.HTTPStatusError):
        await fetch()
    assert call_count == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_http.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `http.py`**

`src/argus/common/http.py`:
```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_http.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/common/http.py tests/unit/test_http.py
git commit -m "feat(common): httpx client + retry decorator"
```

---

### Task 8: Common — Healthcheck endpoint

**Files:**
- Create: `src/argus/common/health.py`
- Create: `tests/unit/test_health.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_health.py`:
```python
import pytest
from fastapi.testclient import TestClient


def test_healthz_healthy_when_db_ok(monkeypatch):
    from argus.common import health

    async def ok():
        return True

    app = health.create_app(db_ping=ok, scheduler_running=True, last_run_iso="2026-07-05T13:00:00Z")
    client = TestClient(app)
    res = client.get("/healthz")
    assert res.status_code == 200
    body = res.json()
    assert body["db"] is True
    assert body["scheduler"] is True
    assert body["last_run"] == "2026-07-05T13:00:00Z"


def test_healthz_unhealthy_when_db_down(monkeypatch):
    from argus.common import health

    async def bad():
        return False

    app = health.create_app(db_ping=bad, scheduler_running=True, last_run_iso=None)
    client = TestClient(app)
    res = client.get("/healthz")
    assert res.status_code == 503
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_health.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `health.py`**

`src/argus/common/health.py`:
```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_health.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/common/health.py tests/unit/test_health.py
git commit -m "feat(common): healthz endpoint"
```

---

### Task 9: Example config files

**Files:**
- Create: `.env.example`
- Create: `watchlist.example.yaml`
- Create: `feeds.example.yaml`

- [ ] **Step 1: Write `.env.example`**

```bash
# Postgres
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=argus
POSTGRES_USER=argus
POSTGRES_PASSWORD=__CHANGE_ME__

# LLM
LLM_PROVIDER=deepseek
DEEPSEEK_API_KEY=__CHANGE_ME__
DEEPSEEK_MODEL=deepseek-chat

# Discord
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/__CHANGE_ME__

# SEC (SEC requires User-Agent with email)
SEC_USER_AGENT=ArgusCore/1.0 (your-email@example.com)

# Operational
LOG_LEVEL=INFO
HEALTH_PORT=8080

# Config files
WATCHLIST_PATH=/app/watchlist.yaml
FEEDS_PATH=/app/feeds.yaml
```

- [ ] **Step 2: Write `watchlist.example.yaml`**

```yaml
watchlist:
  - ticker: NVDA
    sector: Semiconductors
  - ticker: AMD
    sector: Semiconductors
  - ticker: TSLA
    sector: Automotive
  - ticker: PLTR
    sector: AI
  - ticker: MRNA
    sector: Biotech

keywords:
  - "insider buy"
  - "FDA approval"
  - "Phase 3"
  - "share buyback program"

thresholds:
  min_relevance_score: 70
  form4_buy_min_usd: 100000
  form4_sell_min_usd: 1000000
```

- [ ] **Step 3: Write `feeds.example.yaml`**

```yaml
feeds:
  - name: DGAP Ad-hoc
    url: https://www.dgap.de/feed/adhoc.xml
    category: corporate_actions

  - name: TechCrunch AI
    url: https://techcrunch.com/category/artificial-intelligence/feed/
    category: tech_news

  - name: SEC Press Releases
    url: https://www.sec.gov/rss/press.xml
    category: regulatory

defaults:
  poll_interval_seconds: 600
  max_items_per_run: 50
```

- [ ] **Step 4: Verify configs load**

Run:
```bash
cp .env.example .env && cp watchlist.example.yaml watchlist.yaml && cp feeds.example.yaml feeds.yaml
# Set dummy values in .env for verification
uv run python -c "from argus.common.config import Settings, load_watchlist, load_feeds; import os; os.environ.setdefault('POSTGRES_PASSWORD','x'); os.environ.setdefault('DEEPSEEK_API_KEY','x'); os.environ.setdefault('DISCORD_WEBHOOK_URL','x'); os.environ.setdefault('SEC_USER_AGENT','x'); s=Settings(); wl=load_watchlist('watchlist.yaml'); f=load_feeds('feeds.yaml'); print('OK', len(wl.watchlist), 'tickers,', len(f['feeds']), 'feeds')"
```
Expected: `OK 5 tickers, 3 feeds`

- [ ] **Step 5: Add real config files to .gitignore**

Append to `.gitignore`:
```
.env
watchlist.yaml
feeds.yaml
```

- [ ] **Step 6: Commit**

```bash
git add .env.example watchlist.example.yaml feeds.example.yaml .gitignore
git commit -m "feat(config): example env, watchlist, feeds"
```

---

## Phase 2: SEC Poller

### Task 10: SEC EDGAR Form 4 XML fixture + parser

**Files:**
- Create: `tests/fixtures/__init__.py`
- Create: `tests/fixtures/sec/__init__.py`
- Create: `tests/fixtures/sec/form4_sample.xml`
- Create: `src/argus/sec_poller/__init__.py`
- Create: `src/argus/sec_poller/edgar.py`
- Create: `tests/unit/test_edgar_parser.py`

- [ ] **Step 1: Create the XML fixture**

`tests/fixtures/sec/form4_sample.xml` (truncated, real Form 4 structure):
```xml
<?xml version="1.0"?>
<RPT>
  <SECForm4>
    <issuer>
      <issuerCik>0001045810</issuerCik>
      <issuerName>NVIDIA CORP</issuerName>
      <issuerTradingSymbol>NVDA</issuerTradingSymbol>
    </issuer>
    <reportingOwner>
      <reportingOwnerId>
        <rptOwnerCik>0001237446</rptOwnerCik>
        <rptOwnerName>HUANG JENSEN</rptOwnerName>
      </reportingOwnerId>
      <reportingOwnerRelationship>
        <isDirector>false</isDirector>
        <isOfficer>true</isOfficer>
        <officerTitle>CEO</officerTitle>
      </reportingOwnerRelationship>
    </reportingOwner>
    <nonDerivativeTable>
      <nonDerivativeTransaction>
        <transactionDate>
          <value>2026-06-15</value>
        </transactionDate>
        <transactionCoding>
          <transactionFormType>4</transactionFormType>
          <transactionCode>P</transactionCode>
          <equitySwapInvolved>false</equitySwapInvolved>
        </transactionCoding>
        <transactionAmounts>
          <transactionShares>
            <value>50000</value>
          </transactionShares>
          <transactionPricePerShare>
            <value>48.20</value>
          </transactionPricePerShare>
          <transactionAcquiredDisposedCode>
            <value>A</value>
          </transactionAcquiredDisposedCode>
        </transactionAmounts>
        <postTransactionTransactionAmounts>
          <sharesOwnedFollowingTransaction>
            <value>1234567</value>
          </sharesOwnedFollowingTransaction>
        </postTransactionTransactionAmounts>
      </nonDerivativeTransaction>
    </nonDerivativeTable>
  </SECForm4>
</RPT>
```

- [ ] **Step 2: Write the failing test**

`tests/unit/test_edgar_parser.py`:
```python
from pathlib import Path
from argus.sec_poller.edgar import parse_form4, Form4Data

FIXTURE = Path(__file__).parent.parent / "fixtures" / "sec" / "form4_sample.xml"


def test_parse_form4_extracts_fields():
    xml_bytes = FIXTURE.read_bytes()
    data = parse_form4(xml_bytes)
    assert isinstance(data, Form4Data)
    assert data.ticker == "NVDA"
    assert data.filer_name == "HUANG JENSEN"
    assert data.filer_role == "CEO"
    assert data.transaction_code == "P"
    assert data.shares == 50000
    assert data.price_per_share == 48.20
    assert data.value_usd == 2_410_000.0


def test_parse_form4_handles_sale():
    # Modify fixture in-memory: replace P -> S, A -> D
    xml = FIXTURE.read_text()
    xml = xml.replace("<transactionCode>P</transactionCode>", "<transactionCode>S</transactionCode>")
    data = parse_form4(xml.encode())
    assert data.transaction_code == "S"


def test_parse_form4_raises_on_invalid_xml():
    import pytest
    with pytest.raises(ValueError):
        parse_form4(b"not xml")
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_edgar_parser.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 4: Implement parser**

`src/argus/sec_poller/edgar.py` (initial version, just parser):
```python
from dataclasses import dataclass
from lxml import etree


@dataclass(frozen=True)
class Form4Data:
    ticker: str
    filer_name: str
    filer_role: str
    transaction_code: str
    shares: int
    price_per_share: float
    value_usd: float


def parse_form4(xml_bytes: bytes) -> Form4Data:
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError as e:
        raise ValueError(f"invalid XML: {e}") from e

    def _find(path: str) -> str | None:
        node = root.find(f".//{path}")
        return node.text if node is not None and node.text else None

    ticker = _find("issuerTradingSymbol")
    filer_name = _find("rptOwnerName")
    filer_role = _find("officerTitle") or _find("isDirector")
    transaction_code = _find("transactionCode")
    shares_str = _find("transactionShares/value")
    price_str = _find("transactionPricePerShare/value")

    if not all([ticker, filer_name, transaction_code, shares_str, price_str]):
        raise ValueError("missing required fields in Form 4 XML")

    shares = int(shares_str)  # type: ignore[arg-type]
    price = float(price_str)  # type: ignore[arg-type]
    return Form4Data(
        ticker=ticker or "",
        filer_name=filer_name or "",
        filer_role=filer_role or "",
        transaction_code=transaction_code or "",
        shares=shares,
        price_per_share=price,
        value_usd=shares * price,
    )
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_edgar_parser.py -v`
Expected: 3 passed.

- [ ] **Step 6: Commit**

```bash
git add tests/fixtures/ src/argus/sec_poller/__init__.py src/argus/sec_poller/edgar.py tests/unit/test_edgar_parser.py
git commit -m "feat(sec): Form 4 XML parser"
```

---

### Task 11: SEC EDGAR API client

**Files:**
- Modify: `src/argus/sec_poller/edgar.py` (append client)
- Create: `tests/unit/test_edgar_client.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_edgar_client.py`:
```python
import pytest
import respx
import httpx
from argus.sec_poller.edgar import EdgarClient


@pytest.mark.asyncio
async def test_client_fetches_filings():
    with respx.mock:
        respx.get(
            "https://efts.sec.gov/LATEST/search-index",
            params={"q": '"form type":"4"', "dateRange": "custom", "startDt": "2026-07-04"},
        ).respond(200, json={
            "hits": {"hits": [
                {"_id": "abc", "_source": {"_id": "https://www.sec.gov/Archives/edgar/data/123/abc.xml"}},
                {"_id": "def", "_source": {"_id": "https://www.sec.gov/Archives/edgar/data/456/def.xml"}},
            ]}
        })
        respx.get("https://www.sec.gov/Archives/edgar/data/123/abc.xml").respond(200, content=b"<xml/>")
        respx.get("https://www.sec.gov/Archives/edgar/data/456/def.xml").respond(200, content=b"<xml/>")

        client = EdgarClient(user_agent="Test/1.0 (test@example.com)")
        urls = await client.list_recent_form4_urls(since="2026-07-04")
        assert len(urls) == 2

        xml = await client.fetch_filing_xml(urls[0])
        assert xml == b"<xml/>"
        await client.close()


@pytest.mark.asyncio
async def test_client_sends_user_agent():
    with respx.mock as mock:
        route = mock.get("https://efts.sec.gov/LATEST/search-index").respond(200, json={"hits": {"hits": []}})
        client = EdgarClient(user_agent="MyAgent/1.0 (test@example.com)")
        await client.list_recent_form4_urls(since="2026-07-04")
        request = mock["https://efts.sec.gov/LATEST/search-index"].calls.last.request
        assert request.headers["user-agent"] == "MyAgent/1.0 (test@example.com)"
        await client.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_edgar_client.py -v`
Expected: FAIL with `ImportError` on `EdgarClient`.

- [ ] **Step 3: Append client to `edgar.py`**

Append to `src/argus/sec_poller/edgar.py`:
```python
import httpx
from argus.common.http import make_client, with_retry
from argus.common.logging import get_logger

log = get_logger(__name__)


class EdgarClient:
    SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"

    def __init__(self, user_agent: str) -> None:
        self._user_agent = user_agent
        self._client = make_client()
        self._client.headers["User-Agent"] = user_agent

    @with_retry()
    async def list_recent_form4_urls(self, since: str) -> list[str]:
        res = await self._client.get(
            self.SEARCH_URL,
            params={"q": '"form type":"4"', "dateRange": "custom", "startDt": since},
        )
        res.raise_for_status()
        data = res.json()
        return [hit["_source"]["_id"] for hit in data.get("hits", {}).get("hits", [])]

    @with_retry()
    async def fetch_filing_xml(self, url: str) -> bytes:
        res = await self._client.get(url)
        res.raise_for_status()
        return res.content

    async def close(self) -> None:
        await self._client.aclose()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_edgar_client.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/sec_poller/edgar.py tests/unit/test_edgar_client.py
git commit -m "feat(sec): EDGAR API client"
```

---

### Task 12: SEC pipeline (fetch → filter → insert)

**Files:**
- Create: `src/argus/sec_poller/pipeline.py`
- Create: `tests/integration/test_sec_pipeline.py`

- [ ] **Step 1: Write the failing test**

`tests/integration/test_sec_pipeline.py`:
```python
import pytest
from pathlib import Path
from unittest.mock import AsyncMock
from sqlalchemy import select
from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent, PollingState
from argus.sec_poller.pipeline import SecPipeline, Form4Filter
from argus.sec_poller.edgar import Form4Data

FIXTURE = Path(__file__).parent.parent / "fixtures" / "sec" / "form4_sample.xml"


@pytest.fixture
async def db(testcontainer_postgres):
    engine = create_engine(testcontainer_postgres)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = create_session_factory(engine)
    yield engine, sessions
    await engine.dispose()


async def test_pipeline_inserts_filtered_event(db):
    engine, sessions = db
    client = AsyncMock()
    client.list_recent_form4_urls.return_value = ["https://www.sec.gov/Archives/edgar/data/123/abc/index.xml"]
    client.fetch_filing_xml.return_value = FIXTURE.read_bytes()

    pipeline = SecPipeline(
        client=client,
        session_factory=sessions,
        form4_filter=Form4Filter(buy_min_usd=100_000, sell_min_usd=1_000_000),
        source_cursor_key="sec_form4",
    )
    inserted = await pipeline.run(since="2026-07-04")
    assert inserted == 1

    async with sessions() as session:
        result = await session.execute(select(RawEvent))
        events = result.scalars().all()
        assert len(events) == 1
        e = events[0]
        assert e.source == "sec_form4"
        assert e.ticker == "NVDA"
        assert e.status == "new"
        assert e.poller_meta["transaction_type"] == "P"
        assert e.poller_meta["value_usd"] == 2_410_000.0


async def test_pipeline_skips_below_threshold(db):
    engine, sessions = db
    client = AsyncMock()
    client.list_recent_form4_urls.return_value = ["https://www.sec.gov/x"]
    # Make a small purchase: modify fixture in-memory
    xml = FIXTURE.read_text()
    xml = xml.replace("<value>50000</value>", "<value>100</value>")
    client.fetch_filing_xml.return_value = xml.encode()

    pipeline = SecPipeline(
        client=client, session_factory=sessions,
        form4_filter=Form4Filter(buy_min_usd=100_000, sell_min_usd=1_000_000),
        source_cursor_key="sec_form4",
    )
    inserted = await pipeline.run(since="2026-07-04")
    assert inserted == 0


async def test_pipeline_writes_cursor(db):
    engine, sessions = db
    client = AsyncMock()
    client.list_recent_form4_urls.return_value = []
    pipeline = SecPipeline(
        client=client, session_factory=sessions,
        form4_filter=Form4Filter(buy_min_usd=100_000, sell_min_usd=1_000_000),
        source_cursor_key="sec_form4",
    )
    await pipeline.run(since="2026-07-04")
    async with sessions() as session:
        result = await session.execute(select(PollingState).where(PollingState.source == "sec_form4"))
        state = result.scalar_one_or_none()
        assert state is not None
        assert state.cursor == "2026-07-04"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_sec_pipeline.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `pipeline.py`**

`src/argus/sec_poller/pipeline.py`:
```python
import hashlib
from datetime import datetime, UTC
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker
from argus.common.logging import get_logger
from argus.common.models import RawEvent, PollingState
from argus.sec_poller.edgar import EdgarClient, parse_form4

log = get_logger(__name__)


class Form4Filter:
    def __init__(self, buy_min_usd: float, sell_min_usd: float) -> None:
        self.buy_min_usd = buy_min_usd
        self.sell_min_usd = sell_min_usd

    def keep(self, transaction_code: str, value_usd: float) -> bool:
        if transaction_code == "P":
            return value_usd >= self.buy_min_usd
        if transaction_code == "S":
            return value_usd >= self.sell_min_usd
        return False


class SecPipeline:
    def __init__(
        self,
        client: EdgarClient,
        session_factory: async_sessionmaker,
        form4_filter: Form4Filter,
        source_cursor_key: str = "sec_form4",
    ) -> None:
        self._client = client
        self._sessions = session_factory
        self._filter = form4_filter
        self._cursor_key = source_cursor_key

    async def run(self, since: str) -> int:
        urls = await self._client.list_recent_form4_urls(since=since)
        inserted = 0
        async with self._sessions() as session:
            for url in urls:
                try:
                    xml = await self._client.fetch_filing_xml(url)
                    data = parse_form4(xml)
                except Exception as e:
                    log.warning("sec.parse_failed", url=url, error=str(e))
                    continue

                if not self._filter.keep(data.transaction_code, data.value_usd):
                    log.info("sec.filtered_out", ticker=data.ticker, code=data.transaction_code)
                    continue

                external_id = url.rsplit("/", 1)[-1]
                content_hash = hashlib.sha256(
                    f"{external_id}:{data.transaction_code}:{data.shares}".encode()
                ).hexdigest()

                existing = await session.execute(
                    select(RawEvent).where(
                        RawEvent.source == "sec_form4",
                        RawEvent.external_id == external_id,
                    )
                )
                if existing.scalar_one_or_none() is not None:
                    continue

                event = RawEvent(
                    source="sec_form4",
                    external_id=external_id,
                    content_hash=content_hash,
                    ticker=data.ticker,
                    title=f"Form 4 - {data.filer_name} ({data.filer_role})",
                    body=(
                        f"Transaction Code: {data.transaction_code}\n"
                        f"Shares: {data.shares}\n"
                        f"Price: ${data.price_per_share}\n"
                        f"Total Value: ${data.value_usd:,.0f}\n"
                        f"Filer Role: {data.filer_role}"
                    ),
                    url=url,
                    poller_meta={
                        "filer_name": data.filer_name,
                        "transaction_type": data.transaction_code,
                        "value_usd": data.value_usd,
                    },
                )
                session.add(event)
                inserted += 1

            await self._upsert_cursor(session, since)
            await session.commit()

        log.info("sec.run_complete", since=since, urls_fetched=len(urls), inserted=inserted)
        return inserted

    async def _upsert_cursor(self, session, since: str) -> None:
        existing = await session.get(PollingState, self._cursor_key)
        if existing is None:
            session.add(PollingState(source=self._cursor_key, cursor=since))
        else:
            existing.cursor = since
            existing.updated_at = datetime.now(UTC)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_sec_pipeline.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/sec_poller/pipeline.py tests/integration/test_sec_pipeline.py
git commit -m "feat(sec): fetch → filter → insert pipeline"
```

---

### Task 13: SEC poller main entrypoint (APScheduler)

**Files:**
- Create: `src/argus/sec_poller/main.py`

- [ ] **Step 1: Write `main.py`**

`src/argus/sec_poller/main.py`:
```python
import asyncio
from datetime import datetime, timedelta, UTC
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import uvicorn
from argus.common.config import Settings, load_watchlist
from argus.common.db import create_engine, create_session_factory
from argus.common.logging import setup_logging, get_logger
from argus.common.health import create_app
from argus.sec_poller.edgar import EdgarClient
from argus.sec_poller.pipeline import SecPipeline, Form4Filter

log = get_logger(__name__)


async def run_once(pipeline: SecPipeline) -> None:
    since = (datetime.now(UTC) - timedelta(days=2)).strftime("%Y-%m-%d")
    try:
        await pipeline.run(since=since)
    except Exception:
        log.exception("sec.run_failed")


async def ping(engine) -> bool:
    try:
        async with engine.connect() as conn:
            await conn.execute("SELECT 1")
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
        client=client, session_factory=sessions,
        form4_filter=Form4Filter(
            buy_min_usd=watchlist.thresholds["form4_buy_min_usd"],
            sell_min_usd=watchlist.thresholds["form4_sell_min_usd"],
        ),
    )

    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_once, CronTrigger(hour="*", minute=0), args=[pipeline])
    scheduler.start()
    log.info("sec.scheduler_started")

    app = create_app(
        db_ping=lambda: ping(engine),
        scheduler_running=True,
        last_run_iso=None,
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
```

- [ ] **Step 2: Add `watchlist_path` to Settings**

Modify `src/argus/common/config.py`, add to `Settings`:
```python
    watchlist_path: str = "watchlist.yaml"
    feeds_path: str = "feeds.yaml"
```

- [ ] **Step 3: Smoke-test that module imports cleanly**

Run: `uv run python -c "from argus.sec_poller.main import main; print('OK')"`
Expected: `OK` (no import errors).

- [ ] **Step 4: Commit**

```bash
git add src/argus/sec_poller/main.py src/argus/common/config.py
git commit -m "feat(sec): scheduler entrypoint with health endpoint"
```

---

## Phase 3: RSS Poller

### Task 14: RSS parser (feedparser wrapper)

**Files:**
- Create: `tests/fixtures/rss/__init__.py`
- Create: `tests/fixtures/rss/rss20.xml`
- Create: `tests/fixtures/rss/atom.xml`
- Create: `src/argus/rss_poller/__init__.py`
- Create: `src/argus/rss_poller/parser.py`
- Create: `tests/unit/test_rss_parser.py`

- [ ] **Step 1: Create RSS fixture**

`tests/fixtures/rss/rss20.xml`:
```xml
<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <title>TechCrunch AI</title>
    <item>
      <title>Nvidia announces new AI chip NVDA-X</title>
      <link>https://techcrunch.com/2026/07/04/nvidia-x</link>
      <guid>tc-001</guid>
      <pubDate>Fri, 04 Jul 2026 10:00:00 GMT</pubDate>
      <description>Nvidia today announced its next-gen chip.</description>
    </item>
    <item>
      <title>Random sector news</title>
      <link>https://techcrunch.com/2026/07/04/random</link>
      <guid>tc-002</guid>
      <pubDate>Fri, 04 Jul 2026 09:00:00 GMT</pubDate>
      <description>Nothing relevant here.</description>
    </item>
  </channel>
</rss>
```

`tests/fixtures/rss/atom.xml`:
```xml
<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Example Atom</title>
  <entry>
    <title>Biotech Phase 3 results: MRNA</title>
    <id>tag:example.com,2026:1</id>
    <updated>2026-07-04T18:00:00Z</updated>
    <link href="https://example.com/1"/>
    <summary>Phase 3 trial successful.</summary>
  </entry>
</feed>
```

- [ ] **Step 2: Write the failing test**

`tests/unit/test_rss_parser.py`:
```python
from pathlib import Path
from argus.rss_poller.parser import parse_feed, FeedItem

FIXTURES = Path(__file__).parent.parent / "fixtures" / "rss"


def test_parse_rss20():
    items = parse_feed((FIXTURES / "rss20.xml").read_bytes(), base_url="https://tc.com")
    assert len(items) == 2
    first = items[0]
    assert isinstance(first, FeedItem)
    assert first.title == "Nvidia announces new AI chip NVDA-X"
    assert first.guid == "tc-001"
    assert first.url == "https://techcrunch.com/2026/07/04/nvidia-x"
    assert first.published is not None


def test_parse_atom():
    items = parse_feed((FIXTURES / "atom.xml").read_bytes(), base_url="https://example.com")
    assert len(items) == 1
    assert items[0].guid == "tag:example.com,2026:1"
    assert items[0].title == "Biotech Phase 3 results: MRNA"


def test_parse_empty_feed():
    items = parse_feed(b"<rss version='2.0'><channel></channel></rss>", base_url="x")
    assert items == []


def test_parse_invalid_returns_empty():
    items = parse_feed(b"not xml at all", base_url="x")
    assert items == []
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_rss_parser.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 4: Implement parser**

`src/argus/rss_poller/parser.py`:
```python
import time
from dataclasses import dataclass
from datetime import datetime, UTC
import feedparser
from argus.common.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class FeedItem:
    title: str
    body: str
    url: str
    guid: str
    published: datetime | None


def _parse_date(entry) -> datetime | None:
    for field in ("published_parsed", "updated_parsed"):
        v = entry.get(field)
        if v:
            return datetime.fromtimestamp(time.mktime(v), tz=UTC)
    return None


def parse_feed(xml_bytes: bytes, base_url: str = "") -> list[FeedItem]:
    parsed = feedparser.parse(xml_bytes)
    if parsed.bozo and not parsed.entries:
        log.warning("rss.parse_failed", base_url=base_url, error=str(parsed.bozo_exception))
        return []
    items: list[FeedItem] = []
    for entry in parsed.entries:
        items.append(FeedItem(
            title=entry.get("title", ""),
            body=entry.get("description") or entry.get("summary", ""),
            url=entry.get("link", ""),
            guid=entry.get("id") or entry.get("guid") or entry.get("link", ""),
            published=_parse_date(entry),
        ))
    return items
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_rss_parser.py -v`
Expected: 4 passed.

- [ ] **Step 6: Commit**

```bash
git add tests/fixtures/rss/ src/argus/rss_poller/ tests/unit/test_rss_parser.py
git commit -m "feat(rss): feedparser wrapper"
```

---

### Task 15: RSS pipeline

**Files:**
- Create: `src/argus/rss_poller/pipeline.py`
- Create: `tests/integration/test_rss_pipeline.py`

- [ ] **Step 1: Write the failing test**

`tests/integration/test_rss_pipeline.py`:
```python
import pytest
from pathlib import Path
from unittest.mock import AsyncMock
import httpx
from sqlalchemy import select
from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent
from argus.common.config import WatchlistConfig
from argus.rss_poller.pipeline import RssPipeline

FIXTURE = Path(__file__).parent.parent / "fixtures" / "rss" / "rss20.xml"


@pytest.fixture
async def db(testcontainer_postgres):
    engine = create_engine(testcontainer_postgres)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = create_session_factory(engine)
    yield sessions
    await engine.dispose()


@pytest.fixture
def watchlist():
    return WatchlistConfig.model_validate({
        "watchlist": [{"ticker": "NVDA", "sector": "Semiconductors"}],
        "keywords": ["Phase 3"],
        "thresholds": {},
    })


async def test_inserts_only_matching_items(db, watchlist):
    client = AsyncMock()
    client.fetch.return_value = FIXTURE.read_bytes()

    pipeline = RssPipeline(
        client=client, session_factory=db, watchlist=watchlist,
        feed={"name": "TC", "url": "https://tc.com/feed", "category": "tech"},
    )
    inserted = await pipeline.run()
    # Item 1 has "NVDA" ticker; item 2 has nothing → 1 inserted
    assert inserted == 1
    async with db() as session:
        events = (await session.execute(select(RawEvent))).scalars().all()
        assert len(events) == 1
        assert events[0].title.startswith("Nvidia")


async def test_dedup_by_guid(db, watchlist):
    client = AsyncMock()
    client.fetch.return_value = FIXTURE.read_bytes()
    pipeline = RssPipeline(client=client, session_factory=db, watchlist=watchlist,
                            feed={"name": "TC", "url": "https://tc.com/feed", "category": "tech"})
    await pipeline.run()
    inserted_again = await pipeline.run()
    assert inserted_again == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_rss_pipeline.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `pipeline.py`**

`src/argus/rss_poller/pipeline.py`:
```python
import hashlib
import re
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker
import httpx
from argus.common.config import WatchlistConfig
from argus.common.http import with_retry
from argus.common.logging import get_logger
from argus.common.models import RawEvent, PollingState
from argus.rss_poller.parser import parse_feed

log = get_logger(__name__)
TICKER_RE = re.compile(r"\b[A-Z]{1,5}\b")


class RssPipeline:
    def __init__(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker,
        watchlist: WatchlistConfig,
        feed: dict,
    ) -> None:
        self._client = client
        self._sessions = session_factory
        self._watchlist = watchlist
        self._feed = feed
        self._cursor_key = f"rss:{feed['url']}"
        self._ticker_set = {w["ticker"] for w in watchlist.watchlist}

    @with_retry()
    async def fetch(self) -> bytes:
        res = await self._client.get(self._feed["url"])
        res.raise_for_status()
        return res.content

    async def run(self) -> int:
        try:
            xml = await self.fetch()
        except Exception as e:
            log.warning("rss.fetch_failed", url=self._feed["url"], error=str(e))
            return 0

        items = parse_feed(xml, base_url=self._feed["url"])
        inserted = 0
        async with self._sessions() as session:
            for item in items:
                if not self._is_relevant(item.title, item.body):
                    continue
                existing = await session.execute(
                    select(RawEvent).where(
                        RawEvent.source == "rss",
                        RawEvent.external_id == item.guid,
                    )
                )
                if existing.scalar_one_or_none() is not None:
                    continue
                ticker = self._extract_ticker(item.title)
                content_hash = hashlib.sha256(item.guid.encode()).hexdigest()
                session.add(RawEvent(
                    source="rss",
                    external_id=item.guid,
                    content_hash=content_hash,
                    ticker=ticker,
                    title=item.title,
                    body=item.body,
                    url=item.url,
                ))
                inserted += 1

            cursor_value = items[0].guid if items else ""
            existing_state = await session.get(PollingState, self._cursor_key)
            if existing_state is None:
                session.add(PollingState(source=self._cursor_key, cursor=cursor_value))
            else:
                existing_state.cursor = cursor_value
            await session.commit()

        log.info("rss.run_complete", feed=self._feed["name"], inserted=inserted)
        return inserted

    def _is_relevant(self, title: str, body: str) -> bool:
        if self._extract_ticker(title) in self._ticker_set:
            return True
        text = f"{title} {body}".lower()
        return any(kw.lower() in text for kw in self._keywords_lower())

    def _keywords_lower(self) -> set[str]:
        return {kw.lower() for kw in self._watchlist.keywords}

    def _extract_ticker(self, text: str) -> str | None:
        for m in TICKER_RE.findall(text):
            if m in self._ticker_set:
                return m
        return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_rss_pipeline.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/rss_poller/pipeline.py tests/integration/test_rss_pipeline.py
git commit -m "feat(rss): fetch → keyword/ticker filter → insert pipeline"
```

---

### Task 16: RSS poller main entrypoint

**Files:**
- Create: `src/argus/rss_poller/main.py`

- [ ] **Step 1: Write `main.py`**

`src/argus/rss_poller/main.py`:
```python
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import uvicorn
import httpx
from argus.common.config import Settings, load_watchlist, load_feeds
from argus.common.db import create_engine, create_session_factory
from argus.common.http import make_client
from argus.common.logging import setup_logging, get_logger
from argus.common.health import create_app
from argus.rss_poller.pipeline import RssPipeline

log = get_logger(__name__)


async def run_all(pipelines: list[RssPipeline]) -> None:
    for p in pipelines:
        try:
            await p.run()
        except Exception:
            log.exception("rss.run_failed", feed=p._feed["name"])


async def ping(engine) -> bool:
    try:
        async with engine.connect() as conn:
            await conn.execute("SELECT 1")
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
    client = make_client()

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
```

- [ ] **Step 2: Smoke-test import**

Run: `uv run python -c "from argus.rss_poller.main import main; print('OK')"`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add src/argus/rss_poller/main.py
git commit -m "feat(rss): scheduler entrypoint"
```

---

## Phase 4: Processor

### Task 17: Watchlist pre-filter

**Files:**
- Create: `src/argus/processor/__init__.py`
- Create: `src/argus/processor/filters.py`
- Create: `tests/unit/test_filters.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_filters.py`:
```python
from argus.common.config import WatchlistConfig
from argus.common.models import RawEvent
from argus.processor.filters import WatchlistFilter, PreFilterResult


def make_event(ticker=None, title="t", body="b"):
    return RawEvent(
        source="rss", external_id="x", content_hash="h",
        ticker=ticker, title=title, body=body, url="u",
    )


def make_wl():
    return WatchlistConfig.model_validate({
        "watchlist": [
            {"ticker": "NVDA", "sector": "Semiconductors"},
            {"ticker": "MRNA", "sector": "Biotech"},
        ],
        "keywords": ["insider buy", "Phase 3"],
        "thresholds": {},
    })


def test_ticker_match_boost():
    f = WatchlistFilter(make_wl())
    result = f.classify(make_event(ticker="NVDA"))
    assert result.score == 80
    assert result.action == "classify"


def test_keyword_match_only():
    f = WatchlistFilter(make_wl())
    result = f.classify(make_event(ticker="UNKNOWN", title="Company X Phase 3 results"))
    assert result.score == 50
    assert result.action == "classify"


def test_no_match_skip():
    f = WatchlistFilter(make_wl())
    result = f.classify(make_event(ticker="UNKN", title="random", body="boring"))
    assert result.action == "skip"


def test_ticker_not_in_list_but_keyword_present():
    f = WatchlistFilter(make_wl())
    result = f.classify(make_event(ticker="UNKNOWN", title=" insider buy reported"))
    assert result.action == "classify"
    assert result.score == 50
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_filters.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `filters.py`**

`src/argus/processor/filters.py`:
```python
from dataclasses import dataclass
from enum import Enum
from argus.common.config import WatchlistConfig
from argus.common.models import RawEvent


class Action(str, Enum):
    CLASSIFY = "classify"
    SKIP = "skip"


@dataclass(frozen=True)
class PreFilterResult:
    action: Action
    score: int = 0


class WatchlistFilter:
    def __init__(self, watchlist: WatchlistConfig) -> None:
        self._tickers = {w["ticker"] for w in watchlist.watchlist}
        self._keywords = {k.lower() for k in watchlist.keywords}

    def classify(self, event: RawEvent) -> PreFilterResult:
        if event.ticker and event.ticker in self._tickers:
            return PreFilterResult(action=Action.CLASSIFY, score=80)
        text = f"{event.title} {event.body}".lower()
        if any(kw in text for kw in self._keywords):
            return PreFilterResult(action=Action.CLASSIFY, score=50)
        return PreFilterResult(action=Action.SKIP)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_filters.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/processor/ tests/unit/test_filters.py
git commit -m "feat(processor): watchlist pre-filter"
```

---

### Task 18: LLM — Provider protocol + prompts

**Files:**
- Create: `src/argus/llm/__init__.py`
- Create: `src/argus/llm/base.py`
- Create: `src/argus/llm/prompts.py`
- Create: `tests/unit/test_prompts.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_prompts.py`:
```python
import pytest
from pydantic import ValidationError
from argus.llm.prompts import Classification, build_user_prompt, SYSTEM_PROMPT
from argus.common.models import RawEvent


def test_classification_valid():
    c = Classification(sentiment="positive", relevance_score=85, summary="CEO bought shares.")
    assert c.relevance_score == 85


def test_classification_invalid_score():
    with pytest.raises(ValidationError):
        Classification(sentiment="positive", relevance_score=150, summary="x")


def test_classification_invalid_sentiment():
    with pytest.raises(ValidationError):
        Classification(sentiment="bullish", relevance_score=80, summary="x")


def test_classification_summary_too_long():
    with pytest.raises(ValidationError):
        Classification(sentiment="positive", relevance_score=80, summary="x" * 200)


def test_user_prompt_contains_event_fields():
    e = RawEvent(
        source="sec_form4", external_id="x", content_hash="h",
        ticker="NVDA", title="Form 4 - Huang Jensen",
        body="Transaction Code: P, Shares: 50000, Value: $2.4M",
        url="https://sec.gov/...",
    )
    prompt = build_user_prompt(e, watchlist_note="NVDA is on watchlist (Semiconductors).")
    assert "NVDA" in prompt
    assert "Form 4" in prompt
    assert "Semiconductors" in prompt


def test_system_prompt_is_stable():
    assert "ArgusCore" in SYSTEM_PROMPT
    assert "0-100" in SYSTEM_PROMPT
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_prompts.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `prompts.py` and `base.py`**

`src/argus/llm/prompts.py`:
```python
from dataclasses import dataclass
from typing import Literal
from pydantic import BaseModel, Field
from argus.common.models import RawEvent

SYSTEM_PROMPT = """You are ArgusCore, a market intelligence classifier for insider filings and financial news. You receive a single event and return strict JSON.

Scoring rubric (0-100):
- 90-100: Material insider action (CEO/CFO buy >= $500k, FDA approval, M&A)
- 70-89:  Notable but expected (regular insider activity, earnings beats)
- 50-69:  Minor relevance (analyst rating, sector news)
- 0-49:   Noise (routine filings, low-impact news)

Sentiment: positive | negative | neutral
Summary: 1 sentence, English, <=120 chars, lead with the action.

Respond ONLY with valid JSON. No markdown, no preamble.
"""


class Classification(BaseModel):
    sentiment: Literal["positive", "negative", "neutral"]
    relevance_score: int = Field(ge=0, le=100)
    summary: str = Field(max_length=120)


@dataclass(frozen=True)
class LlmCallMeta:
    provider: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float
    latency_ms: int


@dataclass(frozen=True)
class ClassifyResult:
    classification: Classification
    meta: LlmCallMeta


def build_user_prompt(event: RawEvent, watchlist_note: str | None = None) -> str:
    lines = [
        "EVENT:",
        f"Source: {event.source}",
        f"Ticker: {event.ticker or 'unknown'}",
        f"Title: {event.title}",
        f"Body: {event.body}",
    ]
    if watchlist_note:
        lines.append("")
        lines.append(f"Watchlist context: {watchlist_note}")
    return "\n".join(lines)
```

`src/argus/llm/base.py`:
```python
from typing import Protocol
from argus.common.models import RawEvent
from argus.llm.prompts import ClassifyResult


class LLMProvider(Protocol):
    name: str
    model: str

    async def classify(self, event: RawEvent, watchlist_note: str | None = None) -> ClassifyResult: ...

    async def close(self) -> None: ...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_prompts.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/llm/ tests/unit/test_prompts.py
git commit -m "feat(llm): provider protocol + prompt templates"
```

---

### Task 19: LLM — DeepSeek provider

**Files:**
- Create: `src/argus/llm/deepseek.py`
- Create: `tests/unit/test_deepseek_provider.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_deepseek_provider.py`:
```python
import pytest
import respx
import httpx
from argus.llm.deepseek import DeepSeekProvider
from argus.llm.prompts import Classification, ClassifyResult, LlmCallMeta
from argus.common.models import RawEvent


def make_event():
    return RawEvent(
        source="sec_form4", external_id="x", content_hash="h",
        ticker="NVDA", title="Form 4", body="Transaction Code: P", url="u",
    )


@pytest.mark.asyncio
async def test_classify_success():
    with respx.mock:
        respx.post("https://api.deepseek.com/chat/completions").respond(200, json={
            "choices": [{
                "message": {
                    "content": '{"sentiment":"positive","relevance_score":92,"summary":"CEO bought shares"}'
                }
            }],
            "usage": {"prompt_tokens": 100, "completion_tokens": 30, "total_tokens": 130},
        })
        provider = DeepSeekProvider(api_key="sk-test", model="deepseek-chat")
        result = await provider.classify(make_event(), watchlist_note="NVDA on watchlist.")
        assert isinstance(result, ClassifyResult)
        assert result.classification.sentiment == "positive"
        assert result.classification.relevance_score == 92
        assert isinstance(result.meta, LlmCallMeta)
        assert result.meta.prompt_tokens == 100
        assert result.meta.completion_tokens == 30
        assert result.meta.cost_usd > 0
        await provider.close()


@pytest.mark.asyncio
async def test_classify_invalid_json_reprompt():
    with respx.mock as mock:
        route1 = mock.post("https://api.deepseek.com/chat/completions").mock(
            side_effect=[
                httpx.Response(200, json={"choices":[{"message":{"content":"not json"}}],"usage":{}}),
                httpx.Response(200, json={
                    "choices":[{"message":{"content":'{"sentiment":"positive","relevance_score":80,"summary":"ok"}'}}],
                    "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120},
                }),
            ]
        )
        provider = DeepSeekProvider(api_key="sk-test", model="deepseek-chat")
        result = await provider.classify(make_event())
        assert result.classification.relevance_score == 80
        assert route1.call_count == 2
        await provider.close()


@pytest.mark.asyncio
async def test_classify_raises_after_max_reprompts():
    with respx.mock as mock:
        mock.post("https://api.deepseek.com/chat/completions").mock(
            return_value=httpx.Response(200, json={"choices":[{"message":{"content":"junk"}}],"usage":{}})
        )
        provider = DeepSeekProvider(api_key="sk-test", model="deepseek-chat")
        with pytest.raises(ValueError):
            await provider.classify(make_event())
        await provider.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_deepseek_provider.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `deepseek.py`**

`src/argus/llm/deepseek.py`:
```python
import json
import time
from argus.common.http import make_client, with_retry
from argus.common.logging import get_logger
from argus.common.models import RawEvent
from argus.llm.prompts import (
    Classification, ClassifyResult, LlmCallMeta, SYSTEM_PROMPT, build_user_prompt,
)

log = get_logger(__name__)


class DeepSeekProvider:
    name = "deepseek"
    API_URL = "https://api.deepseek.com/chat/completions"

    def __init__(self, api_key: str, model: str = "deepseek-chat",
                 input_cost_per_m: float = 0.14, output_cost_per_m: float = 0.28) -> None:
        self.model = model
        self._client = make_client()
        self._client.headers["Authorization"] = f"Bearer {api_key}"
        self._input_cost_per_m = input_cost_per_m
        self._output_cost_per_m = output_cost_per_m

    async def classify(self, event: RawEvent, watchlist_note: str | None = None) -> ClassifyResult:
        user_prompt = build_user_prompt(event, watchlist_note)
        for attempt in range(2):
            payload = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                "response_format": {"type": "json_object"},
                "stream": False,
            }
            t0 = time.monotonic()
            data = await self._call(payload)
            latency_ms = int((time.monotonic() - t0) * 1000)
            usage = data.get("usage", {})
            content = data["choices"][0]["message"]["content"]
            try:
                parsed = json.loads(content)
                cls = Classification.model_validate(parsed)
            except (json.JSONDecodeError, ValueError) as e:
                log.warning("llm.invalid_json", event_id=str(event.id), attempt=attempt, error=str(e))
                if attempt == 0:
                    user_prompt = (
                        "Previous response was not valid JSON. "
                        "Please respond with valid JSON only.\n\n" + user_prompt
                    )
                    continue
                raise ValueError(f"LLM returned invalid JSON twice: {e}") from e

            pt = usage.get("prompt_tokens", 0)
            ct = usage.get("completion_tokens", 0)
            cost = (pt / 1_000_000) * self._input_cost_per_m + (ct / 1_000_000) * self._output_cost_per_m
            meta = LlmCallMeta(
                provider=self.name, model=self.model,
                prompt_tokens=pt, completion_tokens=ct,
                cost_usd=cost, latency_ms=latency_ms,
            )
            log.info(
                "llm.classified", event_id=str(event.id),
                prompt_tokens=pt, completion_tokens=ct, cost_usd=cost, latency_ms=latency_ms,
            )
            return ClassifyResult(classification=cls, meta=meta)
        raise RuntimeError("unreachable")

    @with_retry()
    async def _call(self, payload: dict) -> dict:
        res = await self._client.post(self.API_URL, json=payload)
        res.raise_for_status()
        return res.json()

    async def close(self) -> None:
        await self._client.aclose()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_deepseek_provider.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/llm/deepseek.py tests/unit/test_deepseek_provider.py
git commit -m "feat(llm): DeepSeek provider with reprompt-on-invalid-json"
```

---

### Task 20: Processor — Classifier (orchestrates filter + LLM)

**Files:**
- Create: `src/argus/processor/classifier.py`
- Create: `tests/integration/test_classifier.py`

- [ ] **Step 1: Write the failing test**

`tests/integration/test_classifier.py`:
```python
import pytest
from unittest.mock import AsyncMock
from sqlalchemy import select
from argus.common.config import WatchlistConfig
from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent, LlmCall
from argus.processor.classifier import Classifier
from argus.processor.filters import WatchlistFilter
from argus.llm.prompts import Classification, ClassifyResult, LlmCallMeta


@pytest.fixture
async def db(testcontainer_postgres):
    engine = create_engine(testcontainer_postgres)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = create_session_factory(engine)
    yield sessions
    await engine.dispose()


async def test_skipped_event_marked_skipped(db):
    llm = AsyncMock()
    wl = WatchlistConfig.model_validate({"watchlist": [{"ticker": "NVDA", "sector": "X"}], "keywords": [], "thresholds": {}})
    classifier = Classifier(filter_=WatchlistFilter(wl), llm=llm)
    e = RawEvent(source="rss", external_id="x", content_hash="h",
                 title="random", body="boring", url="u")
    async with db() as session:
        session.add(e)
        await session.flush()
        await classifier.classify(session, e)
        await session.commit()
    assert e.status == "skipped"
    llm.classify.assert_not_called()


async def test_classified_event_updates_fields_and_writes_llm_call(db):
    llm = AsyncMock()
    llm.classify.return_value = ClassifyResult(
        classification=Classification(sentiment="positive", relevance_score=92, summary="CEO bought shares."),
        meta=LlmCallMeta(provider="deepseek", model="deepseek-chat",
                         prompt_tokens=100, completion_tokens=30, cost_usd=0.0001, latency_ms=480),
    )
    wl = WatchlistConfig.model_validate({"watchlist": [{"ticker": "NVDA", "sector": "X"}], "keywords": [], "thresholds": {}})
    classifier = Classifier(filter_=WatchlistFilter(wl), llm=llm)
    e = RawEvent(source="sec_form4", external_id="x", content_hash="h",
                 ticker="NVDA", title="t", body="b", url="u")
    async with db() as session:
        session.add(e)
        await session.flush()
        await classifier.classify(session, e)
        await session.commit()

    assert e.status == "processed"
    assert e.relevance_score == 92
    assert e.sentiment == "positive"
    assert e.llm_summary == "CEO bought shares."
    assert e.llm_classified_at is not None

    async with db() as session:
        calls = (await session.execute(select(LlmCall))).scalars().all()
        assert len(calls) == 1
        assert calls[0].provider == "deepseek"
        assert calls[0].prompt_tokens == 100
        assert calls[0].cost_usd == pytest.approx(0.0001)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_classifier.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `classifier.py`**

`src/argus/processor/classifier.py`:
```python
from datetime import datetime, UTC
from sqlalchemy.ext.asyncio import AsyncSession
from argus.common.logging import get_logger
from argus.common.models import RawEvent, LlmCall
from argus.llm.base import LLMProvider
from argus.processor.filters import WatchlistFilter, Action

log = get_logger(__name__)


class Classifier:
    def __init__(self, filter_: WatchlistFilter, llm: LLMProvider) -> None:
        self._filter = filter_
        self._llm = llm

    async def classify(self, session: AsyncSession, event: RawEvent) -> None:
        pre = self._filter.classify(event)
        if pre.action == Action.SKIP:
            event.status = "skipped"
            log.info("processor.skipped", event_id=str(event.id), ticker=event.ticker)
            return
        try:
            result = await self._llm.classify(event)
        except Exception:
            log.exception("processor.classify_failed", event_id=str(event.id))
            event.status = "failed"
            event.retry_count += 1
            return

        cls = result.classification
        meta = result.meta
        event.relevance_score = cls.relevance_score
        event.sentiment = cls.sentiment
        event.llm_summary = cls.summary
        event.llm_classified_at = datetime.now(UTC)
        event.status = "processed"

        session.add(LlmCall(
            event_id=event.id,
            provider=meta.provider,
            model=meta.model,
            prompt_tokens=meta.prompt_tokens,
            completion_tokens=meta.completion_tokens,
            cost_usd=meta.cost_usd,
            latency_ms=meta.latency_ms,
        ))
        log.info(
            "processor.classified", event_id=str(event.id), score=cls.relevance_score,
            cost_usd=meta.cost_usd,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_classifier.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/processor/classifier.py tests/integration/test_classifier.py
git commit -m "feat(processor): classifier orchestrator with llm_calls persistence"
```

---

### Task 21: Processor worker (SELECT FOR UPDATE SKIP LOCKED)

**Files:**
- Create: `src/argus/processor/worker.py`
- Create: `tests/integration/test_processor_worker.py`

- [ ] **Step 1: Write the failing test**

`tests/integration/test_processor_worker.py`:
```python
import pytest
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy import select
from argus.common.config import Settings
from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent
from argus.common.config import WatchlistConfig
from argus.processor.classifier import Classifier
from argus.processor.filters import WatchlistFilter
from argus.processor.worker import ProcessorWorker
from argus.llm.prompts import Classification, ClassifyResult, LlmCallMeta


@pytest.fixture
async def db(testcontainer_postgres):
    engine = create_engine(testcontainer_postgres)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = create_session_factory(engine)
    yield engine, sessions
    await engine.dispose()


async def test_worker_classifies_new_events(db):
    engine, sessions = db
    async with sessions() as session:
        for i in range(3):
            session.add(RawEvent(
                source="rss", external_id=f"e{i}", content_hash=f"h{i}",
                ticker="NVDA" if i < 2 else None, title="t", body="b", url="u",
            ))
        await session.commit()

    llm = AsyncMock()
    llm.classify.return_value = ClassifyResult(
        classification=Classification(sentiment="positive", relevance_score=85, summary="ok"),
        meta=LlmCallMeta(provider="deepseek", model="deepseek-chat",
                         prompt_tokens=100, completion_tokens=20, cost_usd=0.0001, latency_ms=200),
    )
    wl = WatchlistConfig.model_validate({"watchlist": [{"ticker": "NVDA", "sector": "X"}], "keywords": [], "thresholds": {}})
    classifier = Classifier(filter_=WatchlistFilter(wl), llm=llm)
    worker = ProcessorWorker(sessions=sessions, classifier=classifier, batch_size=10)

    processed = await worker.run_once()
    assert processed == 2  # 2 with NVDA ticker get classified; 1 without gets skipped via classify() call

    async with sessions() as session:
        events = (await session.execute(select(RawEvent))).scalars().all()
        statuses = {e.external_id: e.status for e in events}
        # All 3 get processed by worker (it pulls 'new' events); classify() handles skip
        assert all(s in ("processed", "skipped") for s in statuses.values())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_processor_worker.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `worker.py`**

`src/argus/processor/worker.py`:
```python
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker
from argus.common.logging import get_logger
from argus.common.models import RawEvent
from argus.processor.classifier import Classifier

log = get_logger(__name__)


class ProcessorWorker:
    def __init__(self, sessions: async_sessionmaker, classifier: Classifier,
                 batch_size: int = 20) -> None:
        self._sessions = sessions
        self._classifier = classifier
        self._batch_size = batch_size

    async def run_once(self) -> int:
        processed = 0
        async with self._sessions() as session:
            result = await session.execute(
                select(RawEvent)
                .where(RawEvent.status == "new")
                .order_by(RawEvent.fetched_at)
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            events = list(result.scalars().all())
            for event in events:
                try:
                    await self._classifier.classify(session, event)
                    processed += 1
                except Exception:
                    log.exception("worker.event_failed", event_id=str(event.id))
                    event.retry_count += 1
                    if event.retry_count >= 3:
                        event.status = "failed"
                    else:
                        event.status = "new"
            await session.commit()
        log.info("processor.batch_complete", processed=processed)
        return processed
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_processor_worker.py -v`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/processor/worker.py tests/integration/test_processor_worker.py
git commit -m "feat(processor): worker with SELECT FOR UPDATE SKIP LOCKED"
```

---

### Task 22: Processor main entrypoint

**Files:**
- Create: `src/argus/processor/main.py`

- [ ] **Step 1: Write `main.py`**

`src/argus/processor/main.py`:
```python
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import uvicorn
from argus.common.config import Settings, load_watchlist
from argus.common.db import create_engine, create_session_factory
from argus.common.logging import setup_logging, get_logger
from argus.common.health import create_app
from argus.llm.deepseek import DeepSeekProvider
from argus.processor.classifier import Classifier
from argus.processor.filters import WatchlistFilter
from argus.processor.worker import ProcessorWorker

log = get_logger(__name__)


async def ping(engine) -> bool:
    try:
        async with engine.connect() as conn:
            await conn.execute("SELECT 1")
        return True
    except Exception:
        return False


async def main() -> None:
    settings = Settings()
    setup_logging(settings.log_level)
    watchlist = load_watchlist(settings.watchlist_path)

    engine = create_engine(settings.database_url)
    sessions = create_session_factory(engine)

    llm = DeepSeekProvider(api_key=settings.deepseek_api_key, model=settings.deepseek_model)
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
```

- [ ] **Step 2: Smoke-test import**

Run: `uv run python -c "from argus.processor.main import main; print('OK')"`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add src/argus/processor/main.py
git commit -m "feat(processor): scheduler entrypoint"
```

---

## Phase 5: Notifier

### Task 23: Discord embed builder

**Files:**
- Create: `src/argus/notifier/__init__.py`
- Create: `src/argus/notifier/discord.py`
- Create: `tests/unit/test_discord_builder.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_discord_builder.py`:
```python
from datetime import datetime, UTC
from argus.common.models import RawEvent
from argus.notifier.discord import build_embed, COLOR_POSITIVE, COLOR_NEGATIVE, COLOR_NEUTRAL


def make_event(**kwargs):
    base = dict(
        source="sec_form4", external_id="x", content_hash="h",
        ticker="NVDA", title="Form 4 - Huang",
        body="Transaction: P", url="https://sec.gov/...",
        sentiment="positive", relevance_score=92,
        llm_summary="CEO Jensen Huang bought 50,000 shares for $2.4M.",
        poller_meta={"filer_name": "Huang Jensen", "transaction_type": "P", "value_usd": 2_410_000},
    )
    base.update(kwargs)
    return RawEvent(**base)


def test_build_embed_positive():
    e = make_event()
    payload = build_embed(e)
    assert "embeds" in payload
    embed = payload["embeds"][0]
    assert "NVDA" in embed["title"]
    assert embed["color"] == COLOR_POSITIVE
    assert embed["url"] == "https://sec.gov/..."
    assert any(f["name"] == "Relevance" for f in embed["fields"])
    assert any(f["name"] == "Summary" for f in embed["fields"])
    assert embed["footer"]["text"].startswith("ArgusCore")


def test_build_embed_negative_color():
    e = make_event(sentiment="negative")
    payload = build_embed(e)
    assert payload["embeds"][0]["color"] == COLOR_NEGATIVE


def test_build_embed_neutral_color():
    e = make_event(sentiment="neutral")
    payload = build_embed(e)
    assert payload["embeds"][0]["color"] == COLOR_NEUTRAL


def test_build_embed_rss_source():
    e = make_event(source="rss", sentiment="neutral", poller_meta=None)
    payload = build_embed(e)
    assert "RSS" in payload["embeds"][0]["title"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_discord_builder.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `discord.py`**

`src/argus/notifier/discord.py`:
```python
from argus.common.models import RawEvent

COLOR_POSITIVE = 3066993   # green
COLOR_NEGATIVE = 15158332  # red
COLOR_NEUTRAL = 9807270    # grey

_SOURCE_LABEL = {"sec_form4": "SEC Form 4", "rss": "RSS"}


def _color_for(sentiment: str | None) -> int:
    if sentiment == "positive":
        return COLOR_POSITIVE
    if sentiment == "negative":
        return COLOR_NEGATIVE
    return COLOR_NEUTRAL


def _format_value(meta: dict | None) -> str:
    if not meta:
        return "n/a"
    value = meta.get("value_usd")
    if value is None:
        return "n/a"
    return f"${value / 1_000_000:.2f}M" if value >= 1_000_000 else f"${value / 1000:.0f}k"


def build_embed(event: RawEvent) -> dict:
    source_label = _SOURCE_LABEL.get(event.source, event.source)
    title_parts = []
    if event.poller_meta and event.poller_meta.get("transaction_type"):
        code = event.poller_meta["transaction_type"]
        action = "INSIDER BUY" if code == "P" else "INSIDER SALE" if code == "S" else f"FORM 4 ({code})"
        title_parts.append(action)
    title = " | ".join([*title_parts, event.ticker, source_label] if event.ticker else [source_label])

    fields = [
        {"name": "Source", "value": source_label, "inline": True},
        {"name": "Relevance", "value": f"{event.relevance_score}/100", "inline": True},
        {"name": "Sentiment", "value": event.sentiment or "neutral", "inline": True},
    ]
    if event.poller_meta and event.poller_meta.get("filer_name"):
        fields.append({"name": "Filer", "value": event.poller_meta["filer_name"], "inline": True})
        fields.append({"name": "Value", "value": _format_value(event.poller_meta), "inline": True})
    if event.llm_summary:
        fields.append({"name": "Summary", "value": event.llm_summary})

    return {
        "embeds": [{
            "title": title[:256],
            "url": event.url,
            "color": _color_for(event.sentiment),
            "fields": fields,
            "footer": {"text": "ArgusCore • DeepSeek"},
        }]
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_discord_builder.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/notifier/ tests/unit/test_discord_builder.py
git commit -m "feat(notifier): Discord embed builder"
```

---

### Task 24: Discord webhook client

**Files:**
- Modify: `src/argus/notifier/discord.py` (append client)
- Create: `tests/unit/test_discord_client.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_discord_client.py`:
```python
import pytest
import respx
from argus.notifier.discord import DiscordClient


@pytest.mark.asyncio
async def test_send_embed_success():
    with respx.mock:
        route = respx.post("https://discord.com/api/webhooks/1/abc").respond(204)
        client = DiscordClient(webhook_url="https://discord.com/api/webhooks/1/abc")
        ok = await client.send_embed({"embeds": [{"title": "t"}]})
        assert ok is True
        assert route.call_count == 1
        await client.close()


@pytest.mark.asyncio
async def test_send_embed_rate_limit_retries():
    import httpx
    with respx.mock as mock:
        route = mock.post("https://discord.com/api/webhooks/1/abc").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "0.01"}),
                httpx.Response(204),
            ]
        )
        client = DiscordClient(webhook_url="https://discord.com/api/webhooks/1/abc", retry_backoff_base=0.01)
        ok = await client.send_embed({"embeds": [{"title": "t"}]})
        assert ok is True
        assert route.call_count == 2
        await client.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_discord_client.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Append client to `discord.py`**

Append to `src/argus/notifier/discord.py`:
```python
import asyncio
import httpx
from argus.common.logging import get_logger

log = get_logger(__name__)


class DiscordClient:
    def __init__(self, webhook_url: str, max_attempts: int = 3,
                 retry_backoff_base: float = 1.0) -> None:
        self._url = webhook_url
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(15.0))
        self._max_attempts = max_attempts
        self._backoff_base = retry_backoff_base

    async def send_embed(self, payload: dict) -> bool:
        attempt = 0
        while True:
            attempt += 1
            try:
                res = await self._client.post(self._url, json=payload)
                if res.status_code == 429:
                    retry_after = float(res.headers.get("Retry-After", self._backoff_base))
                    if attempt >= self._max_attempts:
                        log.warning("discord.rate_limited_giving_up", attempts=attempt)
                        return False
                    log.warning("discord.rate_limited", retry_after=retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                res.raise_for_status()
                return True
            except httpx.HTTPStatusError as e:
                if e.response.status_code not in {500, 502, 503, 504} or attempt >= self._max_attempts:
                    log.warning("discord.http_error", status=e.response.status_code)
                    return False
                await asyncio.sleep(self._backoff_base * (2 ** (attempt - 1)))
            except Exception:
                log.exception("discord.send_failed")
                return False

    async def close(self) -> None:
        await self._client.aclose()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_discord_client.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/notifier/discord.py tests/unit/test_discord_client.py
git commit -m "feat(notifier): Discord webhook client with rate-limit handling"
```

---

### Task 25: Notifier worker

**Files:**
- Create: `src/argus/notifier/worker.py`
- Create: `tests/integration/test_notifier_worker.py`

- [ ] **Step 1: Write the failing test**

`tests/integration/test_notifier_worker.py`:
```python
import pytest
from unittest.mock import AsyncMock
from sqlalchemy import select
from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent
from argus.notifier.worker import NotifierWorker


@pytest.fixture
async def db(testcontainer_postgres):
    engine = create_engine(testcontainer_postgres)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = create_session_factory(engine)
    yield sessions
    await engine.dispose()


async def test_sends_only_above_threshold(db):
    discord = AsyncMock()
    discord.send_embed.return_value = True
    async with db() as session:
        session.add(RawEvent(
            source="sec_form4", external_id="hi", content_hash="h1",
            ticker="NVDA", title="t", body="b", url="u",
            status="processed", relevance_score=85,
        ))
        session.add(RawEvent(
            source="sec_form4", external_id="lo", content_hash="h2",
            ticker="AMD", title="t", body="b", url="u",
            status="processed", relevance_score=40,
        ))
        await session.commit()

    worker = NotifierWorker(sessions=db, discord=discord, min_score=70)
    sent = await worker.run_once()
    assert sent == 1
    discord.send_embed.assert_called_once()

    async with db() as session:
        events = {e.external_id: e.status for e in (await session.execute(select(RawEvent))).scalars().all()}
        assert events["hi"] == "sent"
        assert events["lo"] == "processed"  # remains


async def test_failed_send_keeps_status_processed(db):
    discord = AsyncMock()
    discord.send_embed.return_value = False
    async with db() as session:
        session.add(RawEvent(
            source="sec_form4", external_id="x", content_hash="h",
            ticker="NVDA", title="t", body="b", url="u",
            status="processed", relevance_score=85,
        ))
        await session.commit()

    worker = NotifierWorker(sessions=db, discord=discord, min_score=70)
    sent = await worker.run_once()
    assert sent == 0
    async with db() as session:
        e = (await session.execute(select(RawEvent))).scalar_one()
        assert e.status == "processed"
        assert e.retry_count == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_notifier_worker.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `worker.py`**

`src/argus/notifier/worker.py`:
```python
from datetime import datetime, UTC
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker
from argus.common.logging import get_logger
from argus.common.models import RawEvent, Notification
from argus.notifier.discord import DiscordClient, build_embed

log = get_logger(__name__)


class NotifierWorker:
    def __init__(self, sessions: async_sessionmaker, discord: DiscordClient,
                 min_score: int, batch_size: int = 10) -> None:
        self._sessions = sessions
        self._discord = discord
        self._min_score = min_score
        self._batch_size = batch_size

    async def run_once(self) -> int:
        sent_count = 0
        async with self._sessions() as session:
            result = await session.execute(
                select(RawEvent)
                .where(
                    RawEvent.status == "processed",
                    RawEvent.relevance_score >= self._min_score,
                )
                .order_by(RawEvent.relevance_score.desc())
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            events = list(result.scalars().all())
            for event in events:
                payload = build_embed(event)
                ok = await self._discord.send_embed(payload)
                if ok:
                    event.status = "sent"
                    event.sent_at = datetime.now(UTC)
                    session.add(Notification(
                        event_id=event.id, channel="discord", delivered=True,
                    ))
                    sent_count += 1
                else:
                    event.retry_count += 1
                    if event.retry_count >= 3:
                        event.status = "failed"
                    session.add(Notification(
                        event_id=event.id, channel="discord", delivered=False,
                        error="max_retries_exceeded" if event.retry_count >= 3 else "send_failed",
                    ))
            await session.commit()
        log.info("notifier.batch_complete", sent=sent_count)
        return sent_count
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_notifier_worker.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/argus/notifier/worker.py tests/integration/test_notifier_worker.py
git commit -m "feat(notifier): worker with threshold filter"
```

---

### Task 26: Notifier main entrypoint

**Files:**
- Create: `src/argus/notifier/main.py`

- [ ] **Step 1: Write `main.py`**

`src/argus/notifier/main.py`:
```python
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import uvicorn
from argus.common.config import Settings, load_watchlist
from argus.common.db import create_engine, create_session_factory
from argus.common.logging import setup_logging, get_logger
from argus.common.health import create_app
from argus.notifier.discord import DiscordClient
from argus.notifier.worker import NotifierWorker

log = get_logger(__name__)


async def ping(engine) -> bool:
    try:
        async with engine.connect() as conn:
            await conn.execute("SELECT 1")
        return True
    except Exception:
        return False


async def main() -> None:
    settings = Settings()
    setup_logging(settings.log_level)
    watchlist = load_watchlist(settings.watchlist_path)

    engine = create_engine(settings.database_url)
    sessions = create_session_factory(engine)
    discord = DiscordClient(webhook_url=settings.discord_webhook_url)

    worker = NotifierWorker(
        sessions=sessions, discord=discord,
        min_score=watchlist.thresholds["min_relevance_score"],
    )

    scheduler = AsyncIOScheduler()
    scheduler.add_job(worker.run_once, IntervalTrigger(seconds=15))
    scheduler.start()
    log.info("notifier.scheduler_started")

    app = create_app(db_ping=lambda: ping(engine), scheduler_running=True, last_run_iso=None)
    config = uvicorn.Config(app, host="0.0.0.0", port=settings.health_port, log_config=None)
    server = uvicorn.Server(config)

    try:
        await server.serve()
    finally:
        scheduler.shutdown(wait=False)
        await discord.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Smoke-test import**

Run: `uv run python -c "from argus.notifier.main import main; print('OK')"`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add src/argus/notifier/main.py
git commit -m "feat(notifier): scheduler entrypoint"
```

---

## Phase 6: Docker, CI, Polish

### Task 27: Dockerfile

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`

- [ ] **Step 1: Write `Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src/ src/
COPY alembic/ alembic/
COPY alembic.ini watchlist.example.yaml feeds.example.yaml ./
RUN uv sync --frozen --no-dev

FROM python:3.12-slim AS runtime
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2 libxslt1.1 curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app /app
ENV PATH="/app/.venv/bin:$PATH"

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -fsS http://localhost:8080/healthz || exit 1

EXPOSE 8080
CMD ["python", "-m", "argus.processor.main"]
```

- [ ] **Step 2: Write `.dockerignore`**

```
.git
.github
.venv
__pycache__
*.pyc
.pytest_cache
.mypy_cache
.ruff_cache
tests/
docs/
*.md
.env
watchlist.yaml
feeds.yaml
```

- [ ] **Step 3: Build the image**

Run: `docker build -t argus-core:test .`
Expected: image builds successfully (look for `Successfully tagged argus-core:test`).

- [ ] **Step 4: Verify the image runs**

Run: `docker run --rm argus-core:test python -c "from argus.common.config import Settings; print('OK')"`
Expected: `OK`.

- [ ] **Step 5: Commit**

```bash
git add Dockerfile .dockerignore
git commit -m "feat(docker): multi-stage build"
```

---

### Task 28: docker-compose.yml

**Files:**
- Create: `docker-compose.yml`
- Create: `docker-compose.override.yml` (dev convenience)

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-argus}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB:-argus}
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "${POSTGRES_USER:-argus}"]
      interval: 10s
      timeout: 5s
      retries: 5

  sec-poller:
    build: .
    env_file: .env
    command: ["python", "-m", "argus.sec_poller.main"]
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped

  rss-poller:
    build: .
    env_file: .env
    command: ["python", "-m", "argus.rss_poller.main"]
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped

  processor:
    build: .
    env_file: .env
    command: ["sh", "-c", "alembic upgrade head && python -m argus.processor.main"]
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-st

  notifier:
    build: .
    env_file: .env
    command: ["python", "-m", "argus.notifier.main"]
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped

volumes:
  pgdata:
```

- [ ] **Step 2: Write `docker-compose.override.yml` (dev: mount source, no rebuild)**

```yaml
services:
  sec-poller:
    volumes:
      - ./src:/app/src:ro
      - ./watchlist.yaml:/app/watchlist.yaml:ro
      - ./feeds.yaml:/app/feeds.yaml:ro
  rss-poller:
    volumes:
      - ./src:/app/src:ro
      - ./watchlist.yaml:/app/watchlist.yaml:ro
      - ./feeds.yaml:/app/feeds.yaml:ro
  processor:
    volumes:
      - ./src:/app/src:ro
      - ./alembic:/app/alembic:ro
      - ./watchlist.yaml:/app/watchlist.yaml:ro
  notifier:
    volumes:
      - ./src:/app/src:ro
      - ./watchlist.yaml:/app/watchlist.yaml:ro
```

- [ ] **Step 3: Spin up the stack**

Run (with valid `.env`, `watchlist.yaml`, `feeds.yaml` in place):
```bash
docker compose up --build -d
docker compose ps
docker compose logs processor | head -50
```
Expected: all 5 services show status `running` or `healthy`; processor logs `processor.scheduler_started`.

- [ ] **Step 4: Tear down**

Run: `docker compose down`
Expected: clean removal.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml docker-compose.override.yml
git commit -m "feat(docker): compose stack with 4 services + postgres"
```

---

### Task 29: Pre-commit hooks

**Files:**
- Create: `.pre-commit-config.yaml`

- [ ] **Step 1: Write `.pre-commit-config.yaml`**

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.4.10
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.10.0
    hooks:
      - id: mypy
        additional_dependencies:
          - pydantic>=2.6
          - types-pyyaml>=6.0
          - sqlalchemy>=2.0
        args: [--strict, src/argus]

  - repo: local
    hooks:
      - id: pytest-unit
        name: pytest (unit)
        entry: uv run pytest tests/unit -x --ff
        language: system
        pass_filenames: false
        stages: [commit]
```

- [ ] **Step 2: Install hooks**

Run: `uv run pre-commit install`
Expected: `pre-commit installed at .git/hooks/pre-commit`.

- [ ] **Step 3: Run pre-commit on all files**

Run: `uv run pre-commit run --all-files`
Expected: all hooks pass (fix any failures inline).

- [ ] **Step 4: Commit**

```bash
git add .pre-commit-config.yaml
git commit -m "chore: pre-commit hooks (ruff, mypy, pytest)"
```

---

### Task 30: GitHub Actions CI

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Write `ci.yml`**

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
      - run: uv sync --all-extras
      - run: uv run ruff check .
      - run: uv run ruff format --check .
      - run: uv run mypy src

  unit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
      - run: uv sync --all-extras
      - run: uv run pytest tests/unit -v

  integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
      - run: uv sync --all-extras
      - run: uv run pytest tests/integration -v

  docker:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - run: docker build -t argus-core:ci .
      - run: docker run --rm argus-core:ci python -c "from argus.common.config import Settings; print('OK')"
```

- [ ] **Step 2: Push to branch and verify CI runs**

```bash
git checkout -b ci-verify
git push -u origin ci-verify
gh run watch
```
Expected: all jobs green (or known issues to fix).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: GitHub Actions workflow"
git checkout main
git branch -D ci-verify
```

---

### Task 31: README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Write `README.md`**

```markdown
# ArgusCore

Automated 24/7 data pipeline filtering alternative market data (SEC Form 4, RSS feeds) into high-signal Discord alerts via LLM classification.

## Quickstart

1. **Clone & install:**
   ```bash
   git clone https://github.com/<your-user>/ArgusCore.git
   cd ArgusCore
   uv sync --all-extras
   ```

2. **Configure:**
   ```bash
   cp .env.example .env
   cp watchlist.example.yaml watchlist.yaml
   cp feeds.example.yaml feeds.yaml
   # Edit .env (DeepSeek API key, Discord webhook URL, SEC User-Agent email)
   # Edit watchlist.yaml / feeds.yaml to your needs
   ```

3. **Run the stack:**
   ```bash
   docker compose up --build -d
   docker compose logs -f processor
   ```

## Architecture

Four Python services + Postgres, orchestrated via Docker Compose:

- **sec-poller** — pulls SEC EDGAR Form 4 filings hourly, filters by transaction value
- **rss-poller** — pulls RSS/Atom feeds every 10 minutes, filters by ticker/keyword
- **processor** — classifies new events via DeepSeek LLM (relevance score 0-100, sentiment, summary)
- **notifier** — sends high-relevance events (score >= threshold) to Discord

Services communicate via shared Postgres: the `raw_events.status` column acts as a queue.

## Manual Requeue

Failed events (status='failed') can be retried:
```sql
UPDATE raw_events SET status='new', retry_count=0 WHERE status='failed';
```

## Cost Monitoring

```sql
SELECT DATE(created_at) AS day, SUM(cost_usd) AS usd
FROM llm_calls
WHERE created_at > NOW() - INTERVAL '30 days'
GROUP BY day ORDER BY day;
```

## Development

```bash
uv sync --all-extras
uv run pre-commit install
uv run pytest
```

Test pyramid: `tests/unit` (fast), `tests/integration` (testcontainers + Postgres), `tests/e2e` (Docker Compose).

## Tech Stack

Python 3.12, httpx, APScheduler, SQLAlchemy 2.0 (async), Alembic, Pydantic v2, structlog, feedparser, lxml, FastAPI (mini healthcheck), pytest + testcontainers, ruff + mypy, uv.

## License

MIT
```

- [ ] **Step 2: Verify rendering (optional)**

Open `README.md` in your editor's preview, or run `mdcat README.md` if available.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: project README"
```

---

### Task 32: E2E test

**Files:**
- Create: `tests/e2e/__init__.py`
- Create: `tests/e2e/test_full_flow.py`
- Create: `tests/e2e/docker-compose.e2e.yml`

- [ ] **Step 1: Write E2E compose (mocks for external services)**

`tests/e2e/docker-compose.e2e.yml`:
```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: argus
      POSTGRES_PASSWORD: test
      POSTGRES_DB: argus
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "argus"]
      interval: 5s
      timeout: 5s
      retries: 5
    ports:
      - "5432"

  mock-sec:
    image: kennethreitz/httpbin:latest
    ports:
      - "80"

  mock-discord:
    image: mockserver/mockserver:latest
    ports:
      - "1080"

  mock-llm:
    image: mockserver/mockserver:latest
    ports:
      - "1080"
```

- [ ] **Step 2: Write the E2E test**

`tests/e2e/test_full_flow.py`:
```python
"""E2E test: spin up compose stack with mocks, inject event, verify flow.

Run with: uv run pytest tests/e2e/test_full_flow.py -v -s

Prerequisites: Docker running, ports 5433/1080/1081 free.
"""
import asyncio
import os
import subprocess
import time
import pytest


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


@pytest.fixture(scope="module")
def e2e_stack():
    compose = ["docker", "compose", "-f", "tests/e2e/docker-compose.e2e.yml"]
    run(compose + ["up", "-d", "--wait"])
    yield
    run(compose + ["down", "-v"], check=False)


def test_full_pipeline_flows_end_to_end(e2e_stack):
    """Inject a Form 4 filing at the SEC mock; assert it lands in Discord mock."""
    # 1. Configure expectations on mocks (mockserver client API)
    # 2. Start the 4 ArgusCore services against mocks (could be `docker compose up`)
    # 3. Inject test Form 4 XML into mock-sec
    # 4. Poll Postgres for raw_events.status='sent' (timeout 60s)
    # 5. Assert mock-discord received an embed
    pytest.skip("E2E scaffolding is environment-specific; implement on first deploy")
```

- [ ] **Step 3: Document the E2E workflow**

Append to `README.md`:
```markdown

## E2E Testing

The E2E test (`tests/e2e/test_full_flow.py`) requires Docker and validates the full pipeline against mock SEC/LLM/Discord endpoints. It is environment-specific and skipped by default — implement the mock expectations on first deployment to verify end-to-end behavior.
```

- [ ] **Step 4: Run all tests to confirm nothing else broke**

Run: `uv run pytest`
Expected: all unit + integration tests pass; E2E skips.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/
git commit -m "test(e2e): scaffolding for full-flow test against mocks"
```

---

## Final Verification

After completing all 32 tasks:

- [ ] Run the full test suite: `uv run pytest -v` — all green (E2E skipped)
- [ ] Run linters clean: `uv run ruff check . && uv run mypy src`
- [ ] Build Docker image: `docker build -t argus-core:final .`
- [ ] Bring up compose stack: `docker compose up --build -d`
- [ ] Verify healthchecks: `docker compose ps` — all services healthy
- [ ] Check logs: `docker compose logs -f processor` — `processor.scheduler_started` visible
- [ ] Push branch and open PR
