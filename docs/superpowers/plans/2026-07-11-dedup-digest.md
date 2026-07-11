# Dedup + Hybrid-Digest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Doppelte Stories über Quellen hinweg nur einmal verarbeiten und Discord-Pings in Sofort-Ping (Score ≥ 90) vs. täglichen Digest aufteilen.

**Architecture:** Dedup läuft im Processor VOR dem LLM-Call (Heuristik: Ticker + Fuzzy-Titel + 48h-Fenster, fail-open). Der Notifier pingt nur noch Events ≥ `instant_score` sofort; ein neuer täglicher Digest-Job sammelt das Band `[min_relevance_score, instant_score)` und schickt ein Embed. Digest-Cursor liegt in `polling_state`.

**Tech Stack:** Python 3.12, SQLAlchemy async, Alembic, difflib (stdlib), pytest + respx + testcontainers. Testlauf immer mit `uv run --extra dev pytest ...`.

**Spec:** `docs/superpowers/specs/2026-07-11-dedup-digest-design.md`

---

### Task 1: Schema — `duplicate_of`-Spalte + Migration 0002

**Files:**
- Modify: `src/argus/common/models.py` (RawEvent, nach `retry_count`)
- Create: `alembic/versions/0002_duplicate_of.py`
- Test: `tests/unit/test_models.py` (neu anlegen)

- [ ] **Step 1: Failing Test schreiben**

```python
# tests/unit/test_models.py
from argus.common.models import RawEvent


def test_raw_event_has_duplicate_of_column():
    col = RawEvent.__table__.columns["duplicate_of"]
    assert col.nullable is True
    fks = list(col.foreign_keys)
    assert fks and fks[0].column.table.name == "raw_events"
```

- [ ] **Step 2: Test rot sehen**

Run: `uv run --extra dev pytest tests/unit/test_models.py -q`
Expected: FAIL mit `KeyError: 'duplicate_of'`

- [ ] **Step 3: Model erweitern**

In `src/argus/common/models.py` in `RawEvent` direkt unter `retry_count`:

```python
    duplicate_of: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("raw_events.id"), nullable=True
    )
```

`ForeignKey` zu den bestehenden `sqlalchemy`-Imports hinzufügen, falls nicht vorhanden.

- [ ] **Step 4: Migration anlegen**

```python
# alembic/versions/0002_duplicate_of.py
"""add raw_events.duplicate_of

Revision ID: 0002_duplicate_of
Revises: 0001_initial
Create Date: 2026-07-11

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_duplicate_of"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "raw_events",
        sa.Column(
            "duplicate_of",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("raw_events.id"),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("raw_events", "duplicate_of")
```

- [ ] **Step 5: Test grün sehen + ganze Suite**

Run: `uv run --extra dev pytest tests/unit/test_models.py tests/unit -q`
Expected: PASS, keine Regressionen

- [ ] **Step 6: Commit**

```bash
git add src/argus/common/models.py alembic/versions/0002_duplicate_of.py tests/unit/test_models.py
git commit -m "feat: add raw_events.duplicate_of column for dedup"
```

---

### Task 2: Dedup-Heuristik (pure Funktionen)

**Files:**
- Create: `src/argus/processor/dedup.py`
- Test: `tests/unit/test_dedup.py`

- [ ] **Step 1: Failing Tests schreiben**

```python
# tests/unit/test_dedup.py
from argus.processor.dedup import normalize_title, titles_similar


def test_normalize_title_strips_punctuation_case_whitespace():
    assert normalize_title("  NVIDIA: CEO buys  50,000 shares!  ") == (
        "nvidia ceo buys 50000 shares"
    )


def test_titles_similar_rephrased_story():
    a = "NVIDIA CEO Jensen Huang buys 50,000 shares"
    b = "Nvidia CEO Huang buys 50000 shares!"
    assert titles_similar(a, b) is True


def test_titles_similar_different_story_same_ticker():
    a = "NVIDIA CEO buys 50,000 shares"
    b = "NVIDIA announces new datacenter GPU lineup for 2027"
    assert titles_similar(a, b) is False
```

- [ ] **Step 2: Test rot sehen**

Run: `uv run --extra dev pytest tests/unit/test_dedup.py -q`
Expected: FAIL mit `ModuleNotFoundError: No module named 'argus.processor.dedup'`

- [ ] **Step 3: Implementieren**

```python
# src/argus/processor/dedup.py
import re
from difflib import SequenceMatcher

SIMILARITY_THRESHOLD = 0.75
WINDOW_HOURS = 48

_PUNCT_RE = re.compile(r"[^\w\s]")
_WS_RE = re.compile(r"\s+")


def normalize_title(title: str) -> str:
    t = _PUNCT_RE.sub("", title.lower())
    return _WS_RE.sub(" ", t).strip()


def titles_similar(a: str, b: str, threshold: float = SIMILARITY_THRESHOLD) -> bool:
    return SequenceMatcher(None, normalize_title(a), normalize_title(b)).ratio() >= threshold
```

- [ ] **Step 4: Test grün sehen**

Run: `uv run --extra dev pytest tests/unit/test_dedup.py -q`
Expected: PASS (falls der Rephrase-Test knapp scheitert: Schwelle NICHT senken, sondern prüfen ob normalize korrekt arbeitet)

- [ ] **Step 5: Commit**

```bash
git add src/argus/processor/dedup.py tests/unit/test_dedup.py
git commit -m "feat: add title-similarity dedup heuristic"
```

---

### Task 3: `find_duplicate` — Kandidatensuche in der DB

**Files:**
- Modify: `src/argus/processor/dedup.py`
- Test: `tests/integration/test_dedup_db.py`

- [ ] **Step 1: Failing Integrationstests schreiben**

Fixture-Muster aus `tests/integration/test_notifier_worker.py` übernehmen (`testcontainer_postgres`, Truncate über `reversed(Base.metadata.sorted_tables)`).

```python
# tests/integration/test_dedup_db.py
from datetime import UTC, datetime, timedelta

import pytest

from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, RawEvent
from argus.processor.dedup import find_duplicate


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


def make_event(**kw) -> RawEvent:
    defaults = dict(
        source="rss",
        external_id=kw.get("external_id", "x1"),
        content_hash="h",
        ticker="NVDA",
        title="NVIDIA CEO buys 50,000 shares",
        body="b",
        url="u",
        status="processed",
    )
    defaults.update(kw)
    return RawEvent(**defaults)


async def test_finds_rephrased_duplicate_same_ticker(db):
    async with db() as session:
        original = make_event(external_id="a")
        session.add(original)
        await session.commit()
        candidate = make_event(
            external_id="b", source="sec_form4",
            title="Nvidia CEO Huang buys 50000 shares!",
        )
        session.add(candidate)
        await session.flush()
        dup = await find_duplicate(session, candidate)
        assert dup is not None and dup.id == original.id


async def test_no_duplicate_outside_window(db):
    async with db() as session:
        old = make_event(
            external_id="a",
            fetched_at=datetime.now(UTC) - timedelta(hours=49),
        )
        session.add(old)
        await session.commit()
        candidate = make_event(external_id="b")
        session.add(candidate)
        await session.flush()
        assert await find_duplicate(session, candidate) is None


async def test_no_ticker_only_matches_same_source(db):
    async with db() as session:
        session.add(make_event(external_id="a", ticker=None, source="rss"))
        await session.commit()
        candidate = make_event(external_id="b", ticker=None, source="sec_form4")
        session.add(candidate)
        await session.flush()
        assert await find_duplicate(session, candidate) is None
```

- [ ] **Step 2: Tests rot sehen**

Run: `uv run --extra dev pytest tests/integration/test_dedup_db.py -q`
Expected: FAIL mit `ImportError: cannot import name 'find_duplicate'`

- [ ] **Step 3: Implementieren**

An `src/argus/processor/dedup.py` anfügen:

```python
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from argus.common.models import RawEvent


async def find_duplicate(session: AsyncSession, event: RawEvent) -> RawEvent | None:
    """Return the original event this one duplicates, or None."""
    cutoff = datetime.now(UTC) - timedelta(hours=WINDOW_HOURS)
    stmt = select(RawEvent).where(
        RawEvent.id != event.id,
        RawEvent.fetched_at >= cutoff,
        RawEvent.status.in_(("new", "processed", "sent")),
    )
    if event.ticker:
        stmt = stmt.where(RawEvent.ticker == event.ticker)
    else:
        stmt = stmt.where(RawEvent.ticker.is_(None), RawEvent.source == event.source)
    candidates = (await session.execute(stmt)).scalars().all()
    for candidate in candidates:
        if titles_similar(event.title, candidate.title):
            return candidate
    return None
```

Imports an den Dateianfang sortieren (ruff macht das beim Commit-Hook sonst selbst).

- [ ] **Step 4: Tests grün sehen**

Run: `uv run --extra dev pytest tests/integration/test_dedup_db.py -q`
Expected: PASS (braucht Docker für testcontainers)

- [ ] **Step 5: Commit**

```bash
git add src/argus/processor/dedup.py tests/integration/test_dedup_db.py
git commit -m "feat: add DB-backed duplicate candidate search"
```

---

### Task 4: Processor-Worker markiert Duplikate (fail-open, kein LLM-Call)

**Files:**
- Modify: `src/argus/processor/worker.py` (run_once-Schleife)
- Test: `tests/integration/test_processor_worker.py` (Tests anfügen)

- [ ] **Step 1: Failing Test schreiben**

An `tests/integration/test_processor_worker.py` anfügen (bestehende Fixtures/Muster der Datei nutzen; Classifier als `AsyncMock`):

```python
async def test_duplicate_skips_llm_and_marks_status(db):
    from unittest.mock import AsyncMock

    from argus.processor.worker import ProcessorWorker

    classifier = AsyncMock()
    async with db() as session:
        original = RawEvent(
            source="rss", external_id="a", content_hash="h1", ticker="NVDA",
            title="NVIDIA CEO buys 50,000 shares", body="b", url="u",
            status="processed", relevance_score=80,
        )
        dup = RawEvent(
            source="sec_form4", external_id="b", content_hash="h2", ticker="NVDA",
            title="Nvidia CEO Huang buys 50000 shares!", body="b", url="u",
            status="new",
        )
        session.add_all([original, dup])
        await session.commit()
        dup_id, orig_id = dup.id, original.id

    worker = ProcessorWorker(sessions=db, classifier=classifier)
    await worker.run_once()

    classifier.classify.assert_not_called()
    async with db() as session:
        row = await session.get(RawEvent, dup_id)
        assert row.status == "duplicate"
        assert row.duplicate_of == orig_id
```

- [ ] **Step 2: Test rot sehen**

Run: `uv run --extra dev pytest tests/integration/test_processor_worker.py -q`
Expected: FAIL — `classifier.classify` wurde aufgerufen bzw. Status bleibt nicht `duplicate`

- [ ] **Step 3: Worker anpassen**

In `src/argus/processor/worker.py`, in der `for event in events:`-Schleife VOR dem `classify`-try-Block:

```python
                try:
                    duplicate_of = await find_duplicate(session, event)
                except Exception:
                    # fail open: lieber doppelt klassifizieren als Event verlieren
                    log.exception("worker.dedup_failed", event_id=str(event.id))
                    duplicate_of = None
                if duplicate_of is not None:
                    event.status = "duplicate"
                    event.duplicate_of = duplicate_of.id
                    log.info(
                        "worker.duplicate_skipped",
                        event_id=str(event.id),
                        original_id=str(duplicate_of.id),
                    )
                    continue
```

Import ergänzen: `from argus.processor.dedup import find_duplicate`

- [ ] **Step 4: Tests grün sehen**

Run: `uv run --extra dev pytest tests/integration/test_processor_worker.py tests/unit -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/argus/processor/worker.py tests/integration/test_processor_worker.py
git commit -m "feat: processor marks cross-source duplicates before LLM call"
```

---

### Task 5: Config — `instant_score`-Threshold + `digest_hour`-Env

**Files:**
- Modify: `src/argus/common/config.py` (WatchlistConfig-Defaults, load_watchlist-Merge, Settings)
- Modify: `.env.example`, `watchlist.example.yaml`
- Test: `tests/unit/test_config.py` (Tests anfügen; Datei existiert — Muster übernehmen, sonst neu anlegen)

- [ ] **Step 1: Failing Test schreiben**

```python
def test_thresholds_include_instant_score_default(tmp_path):
    from argus.common.config import load_watchlist

    p = tmp_path / "watchlist.yaml"
    p.write_text("watchlist: []\nkeywords: []\n", encoding="utf-8")
    cfg = load_watchlist(p)
    assert cfg.thresholds["instant_score"] == 90
```

- [ ] **Step 2: Test rot sehen**

Run: `uv run --extra dev pytest tests/unit/test_config.py -q`
Expected: FAIL mit `KeyError: 'instant_score'`

- [ ] **Step 3: Implementieren**

In `src/argus/common/config.py` an BEIDEN Stellen (WatchlistConfig `default_factory`-Dict UND `load_watchlist`-Merge-Dict) ergänzen:

```python
            "instant_score": 90,
```

In `Settings` unter `# Operational`:

```python
    digest_hour: int = 18
```

In `.env.example` unter `# Operational`:

```
# Hour (UTC) for the daily digest message
DIGEST_HOUR=18
```

In `watchlist.example.yaml` im `thresholds`-Block: `instant_score: 90` mit Kommentar `# score >= instant_score pings immediately; below goes to the daily digest`.

- [ ] **Step 4: Tests grün sehen**

Run: `uv run --extra dev pytest tests/unit/test_config.py tests/unit -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/argus/common/config.py .env.example watchlist.example.yaml tests/unit/test_config.py
git commit -m "feat: add instant_score threshold and digest_hour setting"
```

---

### Task 6: Digest-Embed-Builder

**Files:**
- Modify: `src/argus/notifier/discord.py`
- Test: `tests/unit/test_discord_client.py` (Tests anfügen)

- [ ] **Step 1: Failing Tests schreiben**

```python
def _mk(title: str, score: int, ticker: str = "NVDA"):
    from argus.common.models import RawEvent

    return RawEvent(
        source="rss", external_id=title, content_hash="h", ticker=ticker,
        title=title, body="b", url=f"https://x/{title}", status="processed",
        relevance_score=score,
    )


def test_digest_embed_sorts_and_caps_at_ten():
    from argus.notifier.discord import build_digest_embed

    events = [_mk(f"story {i}", score=70 + i) for i in range(12)]
    embed = build_digest_embed(events)
    lines = embed["description"].splitlines()
    assert "story 11" in lines[0]  # höchster Score zuerst
    assert sum(1 for line in lines if line.startswith("**")) == 10
    assert embed["footer"]["text"] == "+2 weitere"
    assert "Digest" in embed["title"]


def test_digest_embed_no_footer_when_ten_or_fewer():
    from argus.notifier.discord import build_digest_embed

    embed = build_digest_embed([_mk("only one", score=75)])
    assert "footer" not in embed
```

- [ ] **Step 2: Tests rot sehen**

Run: `uv run --extra dev pytest tests/unit/test_discord_client.py -q`
Expected: FAIL mit `ImportError: cannot import name 'build_digest_embed'`

- [ ] **Step 3: Implementieren**

An `src/argus/notifier/discord.py` anfügen:

```python
DIGEST_MAX_ITEMS = 10


def build_digest_embed(events: list[RawEvent]) -> dict[str, Any]:
    ordered = sorted(events, key=lambda e: e.relevance_score or 0, reverse=True)
    shown = ordered[:DIGEST_MAX_ITEMS]
    lines = []
    for e in shown:
        ticker = f"`{e.ticker}` " if e.ticker else ""
        lines.append(f"**[{e.title}]({e.url})** — {ticker}{e.relevance_score}/100")
    embed: dict[str, Any] = {
        "title": f"Daily Digest — {len(ordered)} Events",
        "description": "\n".join(lines),
        "color": COLOR_NEUTRAL,
    }
    if len(ordered) > DIGEST_MAX_ITEMS:
        embed["footer"] = {"text": f"+{len(ordered) - DIGEST_MAX_ITEMS} weitere"}
    return embed
```

- [ ] **Step 4: Tests grün sehen**

Run: `uv run --extra dev pytest tests/unit/test_discord_client.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/argus/notifier/discord.py tests/unit/test_discord_client.py
git commit -m "feat: add daily digest embed builder"
```

---

### Task 7: DigestJob — sammeln, senden, Cursor

**Files:**
- Create: `src/argus/notifier/digest.py`
- Test: `tests/integration/test_digest_job.py`

- [ ] **Step 1: Failing Integrationstests schreiben**

```python
# tests/integration/test_digest_job.py
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from argus.common.db import create_engine, create_session_factory
from argus.common.models import Base, PollingState, RawEvent
from argus.notifier.digest import DigestJob


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


def _mk(external_id: str, score: int, status: str = "processed") -> RawEvent:
    return RawEvent(
        source="rss", external_id=external_id, content_hash="h", ticker="NVDA",
        title=f"story {external_id}", body="b", url="u", status=status,
        relevance_score=score,
    )


async def test_digest_sends_band_and_marks_sent(db):
    discord = AsyncMock()
    discord.send_embed.return_value = True
    async with db() as session:
        session.add_all([_mk("in-band", 75), _mk("instant", 95), _mk("low", 40)])
        await session.commit()

    job = DigestJob(sessions=db, discord=discord, min_score=70, instant_score=90)
    sent = await job.run_once()

    assert sent == 1
    discord.send_embed.assert_called_once()
    async with db() as session:
        from sqlalchemy import select

        result = await session.execute(select(RawEvent))
        by_id = {e.external_id: e for e in result.scalars()}
        assert by_id["in-band"].status == "sent"
        assert by_id["instant"].status == "processed"  # gehört dem Sofort-Pfad
        assert by_id["low"].status == "processed"
        cursor = await session.get(PollingState, "digest:last_sent")
        assert cursor is not None


async def test_digest_empty_band_sends_nothing(db):
    discord = AsyncMock()
    job = DigestJob(sessions=db, discord=discord, min_score=70, instant_score=90)
    assert await job.run_once() == 0
    discord.send_embed.assert_not_called()


async def test_digest_send_failure_keeps_cursor(db):
    discord = AsyncMock()
    discord.send_embed.return_value = False
    async with db() as session:
        session.add(_mk("in-band", 75))
        await session.commit()

    job = DigestJob(sessions=db, discord=discord, min_score=70, instant_score=90)
    assert await job.run_once() == 0
    async with db() as session:
        assert await session.get(PollingState, "digest:last_sent") is None
        from sqlalchemy import select
        result = await session.execute(select(RawEvent))
        assert result.scalars().one().status == "processed"
```

- [ ] **Step 2: Tests rot sehen**

Run: `uv run --extra dev pytest tests/integration/test_digest_job.py -q`
Expected: FAIL mit `ModuleNotFoundError: No module named 'argus.notifier.digest'`

- [ ] **Step 3: Implementieren**

```python
# src/argus/notifier/digest.py
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.common.logging import get_logger
from argus.common.models import Notification, PollingState, RawEvent
from argus.notifier.discord import DiscordClient, build_digest_embed

log = get_logger(__name__)

CURSOR_KEY = "digest:last_sent"
FIRST_RUN_WINDOW_H = 24


class DigestJob:
    """Sendet einmal täglich ein Sammel-Embed für Events im Digest-Score-Band.

    Band: min_score <= relevance_score < instant_score. Events >= instant_score
    hat der Sofort-Pfad (NotifierWorker) bereits gepingt. Der Cursor wird nur
    nach erfolgreichem Send vorgerückt (at-least-once).
    """

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        discord: DiscordClient,
        min_score: int,
        instant_score: int,
    ) -> None:
        self._sessions = sessions
        self._discord = discord
        self._min_score = min_score
        self._instant_score = instant_score

    async def run_once(self) -> int:
        now = datetime.now(UTC)
        async with self._sessions() as session:
            cursor_row = await session.get(PollingState, CURSOR_KEY)
            since = (
                datetime.fromisoformat(cursor_row.cursor)
                if cursor_row
                else now - timedelta(hours=FIRST_RUN_WINDOW_H)
            )
            result = await session.execute(
                select(RawEvent)
                .where(
                    RawEvent.status == "processed",
                    RawEvent.relevance_score >= self._min_score,
                    RawEvent.relevance_score < self._instant_score,
                    RawEvent.fetched_at >= since,
                )
                .with_for_update(skip_locked=True)
            )
            events = list(result.scalars().all())
            if not events:
                log.info("digest.empty")
                return 0

            ok = await self._discord.send_embed(build_digest_embed(events))
            if not ok:
                log.warning("digest.send_failed", count=len(events))
                return 0

            for event in events:
                event.status = "sent"
                event.sent_at = now
                session.add(
                    Notification(event_id=event.id, channel="discord", delivered=True)
                )
            if cursor_row is None:
                session.add(PollingState(source=CURSOR_KEY, cursor=now.isoformat()))
            else:
                cursor_row.cursor = now.isoformat()
            await session.commit()
            log.info("digest.sent", count=len(events))
            return len(events)
```

- [ ] **Step 4: Tests grün sehen**

Run: `uv run --extra dev pytest tests/integration/test_digest_job.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/argus/notifier/digest.py tests/integration/test_digest_job.py
git commit -m "feat: add daily digest job with polling_state cursor"
```

---

### Task 8: Notifier-Wiring — Sofort-Pfad auf `instant_score`, Digest-Cron

**Files:**
- Modify: `src/argus/notifier/main.py`
- Test: manueller Wiring-Check (main() hat keine Unit-Tests; Verhalten ist durch Task 5–7 abgedeckt)

- [ ] **Step 1: main() umbauen**

In `src/argus/notifier/main.py`:

```python
from apscheduler.triggers.cron import CronTrigger

from argus.notifier.digest import DigestJob
```

`NotifierWorker`-Konstruktion ändern von `min_score=watchlist.thresholds["min_relevance_score"]` zu:

```python
    worker = NotifierWorker(
        sessions=sessions,
        discord=discord,
        min_score=watchlist.thresholds["instant_score"],
    )
    digest = DigestJob(
        sessions=sessions,
        discord=discord,
        min_score=watchlist.thresholds["min_relevance_score"],
        instant_score=watchlist.thresholds["instant_score"],
    )
```

Nach dem bestehenden `scheduler.add_job(...)` für den Worker:

```python
    scheduler.add_job(
        tracked(digest.run_once, last_run),
        CronTrigger(hour=settings.digest_hour, minute=0),
    )
```

- [ ] **Step 2: Volle Suite + Lint**

Run: `uv run --extra dev pytest tests/unit tests/integration -q && uv run --extra dev ruff check src tests && uv run --extra dev mypy src`
Expected: alles grün

- [ ] **Step 3: Commit**

```bash
git add src/argus/notifier/main.py
git commit -m "feat: wire hybrid delivery - instant pings >= instant_score, daily digest below"
```

---

### Task 9: Doku

**Files:**
- Modify: `README.md` (Abschnitt zu Notifications/Konfiguration, falls vorhanden)

- [ ] **Step 1: README ergänzen**

Kurzer Absatz: Hybrid-Zustellung (instant_score, Digest um DIGEST_HOUR UTC), Dedup-Verhalten (48h/Fuzzy-Titel, Status `duplicate`).

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document hybrid delivery and dedup behavior"
```
