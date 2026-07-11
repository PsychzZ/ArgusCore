# Form-4 Cluster Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Detect when ≥ `form4_cluster_min_filings` (default 2) Form-4 filings for the same ticker land within `form4_cluster_window_h` (default 48h) and send one instant Discord embed per cluster, separate from the per-event pings and the daily digest.

**Architecture:** A singleton cron job (`Form4ClusterJob`) scans `raw_events WHERE source='sec_form4'` in the sliding window, groups by ticker, and for each group with no open `form4_clusters` row sends a `build_cluster_embed` via Discord, then persists the row (send-then-insert → at-least-once). Open rows suppress re-alerts; later scans refresh the member set. A new `form4_clusters` table + migration `0003` record clusters; `SecPipeline` is enriched to also store `filer_role` in `poller_meta` (JSONB, no migration needed).

**Tech Stack:** Python 3.12, SQLAlchemy 2.0 async, Alembic, APScheduler, pytest + respx + testcontainers. Test runs use `uv run --extra dev pytest ...`.

**Spec:** `docs/superpowers/specs/2026-07-11-form4-cluster-design.md`

---

### Task 1: Model + migration `0003` — `form4_clusters`

**Files:**
- Modify: `src/argus/common/models.py` (add `Form4Cluster` after `PollingState`)
- Create: `alembic/versions/0003_form4_clusters.py`
- Modify: `tests/unit/test_models.py` (append a test)

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_models.py`:

```python
def test_form4_cluster_columns():
    from argus.common.models import Form4Cluster

    cols = Form4Cluster.__table__.columns
    assert cols["ticker"].nullable is False
    assert cols["member_event_ids"].nullable is False
    assert cols["alerted_at"].nullable is True
    index_names = {i.name for i in Form4Cluster.__table__.indexes}
    assert "idx_form4_clusters_ticker_created" in index_names
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run --extra dev pytest tests/unit/test_models.py -q`
Expected: FAIL with `ImportError: cannot import name 'Form4Cluster'`

- [ ] **Step 3: Add the model**

At the end of `src/argus/common/models.py` (after the `PollingState` class):

```python
class Form4Cluster(Base):
    """A detected Form-4 filing cluster (same ticker, >= N filings in a window).

    Used for idempotent cluster alerting: one row per ticker per ~window, with
    ``alerted_at`` set only after a successful Discord send. Later scans refresh
    ``member_event_ids`` / ``window_end`` on the open row without re-sending.
    """

    __tablename__ = "form4_clusters"
    __table_args__ = (
        Index("idx_form4_clusters_ticker_created", "ticker", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    ticker: Mapped[str] = mapped_column(String(16), nullable=False)
    member_event_ids: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    alerted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
```

Note: `uuid`, `String`, `JSONB`, `DateTime`, `Index`, `Mapped`, `mapped_column` are already imported in `models.py`.

- [ ] **Step 4: Create the migration**

Create `alembic/versions/0003_form4_clusters.py`:

```python
"""add form4_clusters

Revision ID: 0003_form4_clusters
Revises: 0002_duplicate_of
Create Date: 2026-07-11

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003_form4_clusters"
down_revision: str | None = "0002_duplicate_of"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "form4_clusters",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("member_event_ids", postgresql.JSONB(), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("alerted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_form4_clusters_ticker_created",
        "form4_clusters",
        ["ticker", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_form4_clusters_ticker_created", table_name="form4_clusters")
    op.drop_table("form4_clusters")
```

- [ ] **Step 5: Run the model test to verify it passes**

Run: `uv run --extra dev pytest tests/unit/test_models.py -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/argus/common/models.py alembic/versions/0003_form4_clusters.py tests/unit/test_models.py
git commit -m "feat: add form4_clusters table and model for cluster tracking"
```

---

### Task 2: Config — `form4_cluster_min_filings` + `form4_cluster_window_h`

**Files:**
- Modify: `src/argus/common/config.py` (`WatchlistConfig`, `load_watchlist`, `Settings`)
- Modify: `.env.example`
- Modify: `watchlist.example.yaml`
- Modify: `tests/unit/test_config.py` (append two tests)

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_config.py`:

```python
def test_thresholds_include_form4_cluster_min_filings_default(tmp_path):
    from argus.common.config import load_watchlist

    p = tmp_path / "watchlist.yaml"
    p.write_text("watchlist: []\nkeywords: []\n", encoding="utf-8")
    cfg = load_watchlist(p)
    assert cfg.thresholds["form4_cluster_min_filings"] == 2


def test_settings_default_form4_cluster_window(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/1/abc")
    monkeypatch.setenv("SEC_USER_AGENT", "Test/1.0 (test@example.com)")
    from argus.common.config import Settings

    assert Settings().form4_cluster_window_h == 48
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run --extra dev pytest tests/unit/test_config.py -q`
Expected: FAIL with `KeyError: 'form4_cluster_min_filings'` (and `AttributeError` for the settings test)

- [ ] **Step 3: Implement**

In `src/argus/common/config.py`:

1. In `WatchlistConfig.thresholds` `default_factory`, add the key (both the model default AND the merge dict in `load_watchlist` must carry it):

```python
class WatchlistConfig(BaseModel):
    watchlist: list[dict[str, str]] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    thresholds: dict[str, int] = Field(
        default_factory=lambda: {
            "min_relevance_score": 70,
            "instant_score": 90,
            "form4_buy_min_usd": 100_000,
            "form4_sell_min_usd": 1_000_000,
            "form4_cluster_min_filings": 2,
        }
    )
```

2. In `load_watchlist`, add the same key to the merge dict:

```python
    data["thresholds"] = {
        "min_relevance_score": 70,
        "instant_score": 90,
        "form4_buy_min_usd": 100_000,
        "form4_sell_min_usd": 1_000_000,
        "form4_cluster_min_filings": 2,
        **thresholds,
    }
```

3. In `Settings`, add under the `# Operational` block:

```python
    digest_hour: int = 18
    form4_cluster_window_h: int = 48
```

- [ ] **Step 4: Update example files**

In `.env.example`, under `# Operational` add after `DIGEST_HOUR=18`:

```ini
# Hours (sliding window) within which N same-ticker Form 4 filings form a cluster
FORM4_CLUSTER_WINDOW_H=48
```

In `watchlist.example.yaml`, under `thresholds:` add after `form4_sell_min_usd: 1000000`:

```yaml
  form4_cluster_min_filings: 2  # >= N same-ticker Form 4 filings within FORM4_CLUSTER_WINDOW_H form a cluster
```

- [ ] **Step 5: Run the config tests to verify they pass**

Run: `uv run --extra dev pytest tests/unit/test_config.py -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/argus/common/config.py .env.example watchlist.example.yaml tests/unit/test_config.py
git commit -m "feat: add form4_cluster_min_filings and form4_cluster_window_h config"
```

---

### Task 3: Enrich `SecPipeline` `poller_meta` with `filer_role`

**Files:**
- Modify: `src/argus/sec_poller/pipeline.py` (`poller_meta` dict)
- Create: `tests/integration/test_sec_pipeline.py`

- [ ] **Step 1: Write the failing integration test**

Create `tests/integration/test_sec_pipeline.py`:

```python
from datetime import UTC, datetime

import pytest

from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent
from argus.sec_poller.edgar import parse_form4
from argus.sec_poller.pipeline import Form4Filter, SecPipeline

_FORM4_XML = b"""<ownershipDocument>
  <issuer><issuerTradingSymbol>NVDA</issuerTradingSymbol></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Jane Doe</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><officerTitle>CFO</officerTitle></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable><nonDerivativeTransaction>
    <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
    <transactionAmounts>
      <transactionShares><value>1000</value></transactionShares>
      <transactionPricePerShare><value>10.5</value></transactionPricePerShare>
    </transactionAmounts>
  </nonDerivativeTransaction></nonDerivativeTable>
</ownershipDocument>"""


class _FakeClient:
    async def list_recent_form4_urls(self, since: str) -> list[str]:
        return ["https://sec.gov/archives/nvda/form4.xml"]

    async def fetch_filing_xml(self, url: str) -> bytes:
        return _FORM4_XML

    async def close(self) -> None:
        return None


@pytest.fixture
async def db(testcontainer_postgres):
    engine = create_engine(testcontainer_postgres)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())
    sessions = create_session_factory(engine)
    yield sessions
    await engine.dispose()


async def test_pipeline_stores_filer_role_in_poller_meta(db):
    pipeline = SecPipeline(
        client=_FakeClient(),
        session_factory=db,
        form4_filter=Form4Filter(buy_min_usd=1, sell_min_usd=1),
    )
    inserted = await pipeline.run(since="2020-01-01")
    assert inserted == 1

    async with db() as session:
        from sqlalchemy import select

        event = (await session.execute(select(RawEvent))).scalars().one()
        assert event.poller_meta["filer_role"] == "CFO"
        assert event.poller_meta["filer_name"] == "Jane Doe"
        assert event.poller_meta["transaction_type"] == "P"


def test_parse_form4_role_from_title():
    data = parse_form4(_FORM4_XML)
    assert data.filer_role == "CFO"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run --extra dev pytest tests/integration/test_sec_pipeline.py -q`
Expected: FAIL — `poller_meta["filer_role"]` raises `KeyError` (only `filer_name`/`transaction_type`/`value_usd` are stored today)

- [ ] **Step 3: Implement**

In `src/argus/sec_poller/pipeline.py`, extend the `poller_meta` dict written when persisting a `sec_form4` event:

```python
                    poller_meta={
                        "filer_name": data.filer_name,
                        "filer_role": data.filer_role,
                        "transaction_type": data.transaction_code,
                        "value_usd": data.value_usd,
                    },
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run --extra dev pytest tests/integration/test_sec_pipeline.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/argus/sec_poller/pipeline.py tests/integration/test_sec_pipeline.py
git commit -m "feat: persist filer_role in sec_form4 poller_meta"
```

---

### Task 4: `find_form4_clusters` query

**Files:**
- Create: `src/argus/processor/cluster.py`
- Create: `tests/integration/test_cluster.py`

- [ ] **Step 1: Write the failing integration test**

Create `tests/integration/test_cluster.py`:

```python
from datetime import UTC, datetime, timedelta

import pytest

from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent
from argus.processor.cluster import find_form4_clusters

CLUSTER_STATUS = ("new", "processed", "sent")


@pytest.fixture
async def db(testcontainer_postgres):
    engine = create_engine(testcontainer_postgres)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())
    sessions = create_session_factory(engine)
    yield sessions
    await engine.dispose()


def _mk(**kw) -> RawEvent:
    defaults = dict(
        source="sec_form4",
        external_id=kw.get("external_id", "x1"),
        content_hash="h",
        ticker="NVDA",
        title="t",
        body="b",
        url="u",
        status="new",
        poller_meta={"transaction_type": "P", "value_usd": 1},
    )
    defaults.update(kw)
    return RawEvent(**defaults)


async def test_groups_by_ticker_respects_min(db):
    async with db() as session:
        session.add_all([_mk(external_id="a"), _mk(external_id="b"), _mk(external_id="c", ticker="TSLA")])
        await session.commit()
    since = datetime.now(UTC) - timedelta(hours=48)
    result = await find_form4_clusters(session=db, since=since, min_filings=2)
    tickers = {t for t, _ in result}
    assert tickers == {"NVDA"}  # TSLA has only 1


async def test_excludes_old_filings(db):
    async with db() as session:
        session.add_all([
            _mk(external_id="a"),
            _mk(external_id="b", fetched_at=datetime.now(UTC) - timedelta(hours=49)),
        ])
        await session.commit()
    since = datetime.now(UTC) - timedelta(hours=48)
    result = await find_form4_clusters(session=db, since=since, min_filings=2)
    assert result == []


async def test_excludes_null_ticker(db):
    async with db() as session:
        session.add_all([_mk(external_id="a"), _mk(external_id="b", ticker=None)])
        await session.commit()
    since = datetime.now(UTC) - timedelta(hours=48)
    result = await find_form4_clusters(session=db, since=since, min_filings=2)
    assert result == []


async def test_excludes_non_cluster_status(db):
    async with db() as session:
        session.add_all([_mk(external_id="a"), _mk(external_id="b", status="duplicate")])
        await session.commit()
    since = datetime.now(UTC) - timedelta(hours=48)
    result = await find_form4_clusters(session=db, since=since, min_filings=2)
    assert result == []
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run --extra dev pytest tests/integration/test_cluster.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'argus.processor.cluster'`

- [ ] **Step 3: Implement**

Create `src/argus/processor/cluster.py`:

```python
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from argus.common.models import RawEvent

CLUSTER_STATUSES = ("new", "processed", "sent")


async def find_form4_clusters(
    session: AsyncSession, since: datetime, min_filings: int
) -> list[tuple[str, list[RawEvent]]]:
    """Group ``sec_form4`` events since ``since`` by ticker.

    Returns ``(ticker, members)`` pairs for tickers with at least ``min_filings``
    events (excluding ``duplicate``/``failed`` and events with no ticker). Used by
    ``Form4ClusterJob`` to decide which tickers form a cluster.
    """
    stmt = (
        select(RawEvent)
        .where(
            RawEvent.source == "sec_form4",
            RawEvent.fetched_at >= since,
            RawEvent.ticker.is_not(None),
            RawEvent.status.in_(CLUSTER_STATUSES),
        )
        .order_by(RawEvent.ticker, RawEvent.fetched_at)
    )
    rows = list((await session.execute(stmt)).scalars().all())
    groups: dict[str, list[RawEvent]] = {}
    for event in rows:
        groups.setdefault(event.ticker, []).append(event)
    return [
        (ticker, members)
        for ticker, members in groups.items()
        if len(members) >= min_filings
    ]
```

- [ ] **Step 4: Run it to verify it passes**

Run: `uv run --extra dev pytest tests/integration/test_cluster.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/argus/processor/cluster.py tests/integration/test_cluster.py
git commit -m "feat: add find_form4_clusters grouping query"
```

---

### Task 5: `build_cluster_embed`

**Files:**
- Modify: `src/argus/notifier/discord.py` (add `COLOR_ALERT` + `build_cluster_embed`)
- Modify: `tests/unit/test_discord_client.py` (append two tests)

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_discord_client.py`:

```python
def _mk_form4(filer_name, role, ttype, value, ticker="NVDA"):
    from argus.common.models import RawEvent

    return RawEvent(
        source="sec_form4",
        external_id=f"{filer_name}-{ttype}",
        content_hash="h",
        ticker=ticker,
        title=f"Form 4 - {filer_name} ({role})",
        body="b",
        url="u",
        status="new",
        poller_meta={
            "filer_name": filer_name,
            "filer_role": role,
            "transaction_type": ttype,
            "value_usd": value,
        },
    )


def test_cluster_embed_shows_ticker_and_net():
    from argus.notifier.discord import COLOR_ALERT, build_cluster_embed

    members = [
        _mk_form4("Alice", "CFO", "P", 100_000),
        _mk_form4("Bob", "Director", "S", 50_000),
    ]
    embed = build_cluster_embed("NVDA", members)
    assert "NVDA" in embed["title"]
    assert "2 filings" in embed["description"]
    assert "Buys: $100,000" in embed["description"]
    assert "Sells: $50,000" in embed["description"]
    assert embed["color"] == COLOR_ALERT


def test_cluster_embed_caps_at_ten():
    from argus.notifier.discord import build_cluster_embed

    members = [_mk_form4(f"P{i}", "", "P", 1000 + i) for i in range(12)]
    embed = build_cluster_embed("NVDA", members)
    lines = [ln for ln in embed["description"].splitlines() if ln.startswith("•")]
    assert len(lines) == 10
    assert "+2 more" in embed["description"]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run --extra dev pytest tests/unit/test_discord_client.py -q`
Expected: FAIL with `ImportError: cannot import name 'build_cluster_embed'`

- [ ] **Step 3: Implement**

In `src/argus/notifier/discord.py`, add the color constant near the others:

```python
COLOR_ALERT = 15105570  # orange
```

And add `build_cluster_embed` after `build_digest_embed`:

```python
CLUSTER_MAX_ITEMS = 10


def build_cluster_embed(ticker: str, members: list[RawEvent]) -> dict[str, Any]:
    """Build a standalone cluster alert embed for one ticker's Form-4 cluster.

    Shows filing count, net buys/sells (from ``poller_meta``), and a capped filer
    list. Returns a bare embed dict; callers wrap it in ``{"embeds": [...]}`` for
    ``DiscordClient.send_embed`` (same convention as ``build_digest_embed``).
    """
    buys = sum(
        m.poller_meta.get("value_usd", 0)
        for m in members
        if m.poller_meta.get("transaction_type") == "P"
    )
    sells = sum(
        m.poller_meta.get("value_usd", 0)
        for m in members
        if m.poller_meta.get("transaction_type") == "S"
    )
    lines = []
    for m in sorted(members, key=lambda e: e.fetched_at):
        meta = m.poller_meta or {}
        role = meta.get("filer_role") or "—"
        ttype = meta.get("transaction_type", "?")
        value = meta.get("value_usd", 0)
        lines.append(f"• {meta.get('filer_name', '?')} ({role}) — {ttype} ${value:,.0f}")
    shown = lines[:CLUSTER_MAX_ITEMS]
    description = "\n".join(shown)
    if len(lines) > CLUSTER_MAX_ITEMS:
        description += f"\n+{len(lines) - CLUSTER_MAX_ITEMS} more"
    return {
        "title": f"Form 4 Cluster — {ticker.upper()}",
        "description": f"{len(members)} filings in 48h\nBuys: ${buys:,.0f} · Sells: ${sells:,.0f}\n{description}",
        "color": COLOR_ALERT,
    }
```

- [ ] **Step 4: Run them to verify they pass**

Run: `uv run --extra dev pytest tests/unit/test_discord_client.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/argus/notifier/discord.py tests/unit/test_discord_client.py
git commit -m "feat: add Form-4 cluster embed builder"
```

---

### Task 6: `Form4ClusterJob`

**Files:**
- Create: `src/argus/notifier/form4_cluster.py`
- Create: `tests/integration/test_form4_cluster_job.py`

- [ ] **Step 1: Write the failing integration test**

Create `tests/integration/test_form4_cluster_job.py`:

```python
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, Form4Cluster, RawEvent
from argus.notifier.form4_cluster import Form4ClusterJob


@pytest.fixture
async def db(testcontainer_postgres):
    engine = create_engine(testcontainer_postgres)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())
    sessions = create_session_factory(engine)
    yield sessions
    await engine.dispose()


def _mk_form4(external_id, ticker, filer_name, role, ttype, value, **kw) -> RawEvent:
    return RawEvent(
        source="sec_form4",
        external_id=external_id,
        content_hash=f"h-{external_id}",
        ticker=ticker,
        title=f"Form 4 - {filer_name} ({role})",
        body="b",
        url="u",
        status=kw.get("status", "new"),
        poller_meta={
            "filer_name": filer_name,
            "filer_role": role,
            "transaction_type": ttype,
            "value_usd": value,
        },
        **{k: v for k, v in kw.items() if k != "status"},
    )


async def test_cluster_sends_once_and_records_row(db):
    discord = AsyncMock()
    discord.send_embed.return_value = True
    async with db() as session:
        session.add_all([
            _mk_form4("a", "NVDA", "Alice", "CFO", "P", 100_000),
            _mk_form4("b", "NVDA", "Bob", "Director", "S", 50_000),
        ])
        await session.commit()

    job = Form4ClusterJob(sessions=db, discord=discord, window_h=48, min_filings=2)
    assert await job.run_once() == 1
    discord.send_embed.assert_called_once()

    # Second run must not re-alert the open cluster.
    assert await job.run_once() == 0
    discord.send_embed.assert_called_once()

    async with db() as session:
        from sqlalchemy import select

        row = (await session.execute(select(Form4Cluster))).scalars().one()
        assert row.ticker == "NVDA"
        assert row.alerted_at is not None
        assert len(row.member_event_ids) == 2


async def test_single_filing_no_alert(db):
    discord = AsyncMock()
    async with db() as session:
        session.add(_mk_form4("a", "NVDA", "Alice", "CFO", "P", 100_000))
        await session.commit()
    job = Form4ClusterJob(sessions=db, discord=discord, window_h=48, min_filings=2)
    assert await job.run_once() == 0
    discord.send_embed.assert_not_called()


async def test_old_filing_excluded(db):
    discord = AsyncMock()
    async with db() as session:
        session.add_all([
            _mk_form4("a", "NVDA", "Alice", "CFO", "P", 100_000),
            _mk_form4("b", "NVDA", "Bob", "Director", "S", 50_000,
                      fetched_at=datetime.now(UTC) - timedelta(hours=49)),
        ])
        await session.commit()
    job = Form4ClusterJob(sessions=db, discord=discord, window_h=48, min_filings=2)
    assert await job.run_once() == 0
    discord.send_embed.assert_not_called()


async def test_send_failure_retries(db):
    discord = AsyncMock()
    discord.send_embed.return_value = False
    async with db() as session:
        session.add_all([
            _mk_form4("a", "NVDA", "Alice", "CFO", "P", 100_000),
            _mk_form4("b", "NVDA", "Bob", "Director", "S", 50_000),
        ])
        await session.commit()
    job = Form4ClusterJob(sessions=db, discord=discord, window_h=48, min_filings=2)
    assert await job.run_once() == 0
    async with db() as session:
        from sqlalchemy import select

        assert (await session.execute(select(Form4Cluster))).scalars().first() is None


async def test_multiple_tickers_one_embed_each(db):
    discord = AsyncMock()
    discord.send_embed.return_value = True
    async with db() as session:
        session.add_all([
            _mk_form4("a", "NVDA", "Alice", "CFO", "P", 100_000),
            _mk_form4("b", "NVDA", "Bob", "Director", "S", 50_000),
            _mk_form4("c", "TSLA", "Carol", "CEO", "P", 200_000),
            _mk_form4("d", "TSLA", "Dan", "", "P", 80_000),
        ])
        await session.commit()
    job = Form4ClusterJob(sessions=db, discord=discord, window_h=48, min_filings=2)
    assert await job.run_once() == 2
    assert discord.send_embed.call_count == 2
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run --extra dev pytest tests/integration/test_form4_cluster_job.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'argus.notifier.form4_cluster'`

- [ ] **Step 3: Implement**

Create `src/argus/notifier/form4_cluster.py`:

```python
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.common.logging import get_logger
from argus.common.models import Form4Cluster, RawEvent
from argus.notifier.discord import DiscordClient, build_cluster_embed
from argus.processor.cluster import find_form4_clusters

log = get_logger(__name__)


class Form4ClusterJob:
    """Detect Form-4 clusters and send one instant Discord embed per cluster.

    A cluster is >= ``min_filings`` ``sec_form4`` events for the same ticker within
    ``window_h`` hours. Idempotency: an "open" cluster row (``created_at`` within
    the window) suppresses re-alerts; later scans refresh its member set without
    re-sending. The embed is sent BEFORE the row is persisted, so a failed send is
    retried on the next scan (at-least-once). The job is a singleton cron trigger,
    so no row locking is needed.
    """

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        discord: DiscordClient,
        window_h: int,
        min_filings: int,
    ) -> None:
        self._sessions = sessions
        self._discord = discord
        self._window_h = window_h
        self._min_filings = min_filings

    async def run_once(self) -> int:
        now = datetime.now(UTC)
        since = now - timedelta(hours=self._window_h)
        sent = 0
        try:
            async with self._sessions() as session:
                candidates = await find_form4_clusters(session, since, self._min_filings)
                for ticker, members in candidates:
                    open_cluster = await self._find_open_cluster(session, ticker, now)
                    if open_cluster is not None:
                        open_cluster.member_event_ids = [str(m.id) for m in members]
                        open_cluster.window_end = now
                        continue
                    embed = build_cluster_embed(ticker, members)
                    ok = await self._discord.send_embed({"embeds": [embed]})
                    if not ok:
                        log.warning("cluster.send_failed", ticker=ticker)
                        continue
                    session.add(
                        Form4Cluster(
                            ticker=ticker,
                            member_event_ids=[str(m.id) for m in members],
                            window_start=since,
                            window_end=now,
                            alerted_at=now,
                        )
                    )
                    sent += 1
                await session.commit()
        except Exception:
            log.exception("cluster.run_failed")
            return 0
        log.info("cluster.run_complete", sent=sent)
        return sent

    async def _find_open_cluster(
        self, session: AsyncSession, ticker: str, now: datetime
    ) -> Form4Cluster | None:
        cutoff = now - timedelta(hours=self._window_h)
        result = await session.execute(
            select(Form4Cluster).where(
                Form4Cluster.ticker == ticker,
                Form4Cluster.created_at >= cutoff,
            )
        )
        return result.scalars().first()
```

- [ ] **Step 4: Run it to verify it passes**

Run: `uv run --extra dev pytest tests/integration/test_form4_cluster_job.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/argus/notifier/form4_cluster.py tests/integration/test_form4_cluster_job.py
git commit -m "feat: add Form4ClusterJob with idempotent cluster alerting"
```

---

### Task 7: Wire `Form4ClusterJob` into the notifier

**Files:**
- Modify: `src/argus/notifier/main.py`

- [ ] **Step 1: Add the import**

In `src/argus/notifier/main.py`, add to the import block:

```python
from argus.notifier.form4_cluster import Form4ClusterJob
```

- [ ] **Step 2: Construct the job**

After the `digest = DigestJob(...)` block in `main()`:

```python
    cluster_job = Form4ClusterJob(
        sessions=sessions,
        discord=discord,
        window_h=settings.form4_cluster_window_h,
        min_filings=watchlist.thresholds["form4_cluster_min_filings"],
    )
```

- [ ] **Step 3: Schedule the job**

After the `scheduler.add_job(...)` for `digest`, add:

```python
    scheduler.add_job(
        tracked(cluster_job.run_once, last_run),
        CronTrigger(hour="*", minute=5),
    )
```

- [ ] **Step 4: Verify it imports and lints**

Run: `uv run --extra dev ruff check src tests && uv run --extra dev mypy src`
Expected: no issues

- [ ] **Step 5: Commit**

```bash
git add src/argus/notifier/main.py
git commit -m "feat: wire Form4ClusterJob into notifier scheduler"
```

---

### Task 8: Docs + full verification

**Files:**
- Modify: `README.md` (notifier architecture bullet)
- Full suite + lint + types

- [ ] **Step 1: Document in README**

In `README.md`, replace the `notifier` bullet (line 36):

```markdown
- **notifier** — hybrid delivery: events with score >= `instant_score` (default 90) ping Discord immediately; the band between `min_relevance_score` and `instant_score` is collected into one daily digest embed sent at `DIGEST_HOUR` UTC (default 18). A separate hourly job detects Form-4 clusters (>= `form4_cluster_min_filings` same-ticker filings within `form4_cluster_window_h`) and sends one instant "Form 4 Cluster" embed per ticker.
```

- [ ] **Step 2: Run the full verification**

Run: `uv run --extra dev pytest tests/unit tests/integration -q && uv run --extra dev ruff check src tests && uv run --extra dev mypy src`
Expected: all tests pass, ruff clean, mypy clean

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: document Form-4 cluster detection"
```
