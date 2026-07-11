# ArgusCore — Design-Spec

**Datum:** 2026-07-05
**Status:** Draft (wartet auf User-Review)
**Autor:** Max Lehmann

## Übersicht

ArgusCore ist eine 24/7-laufende Data-Pipeline, die alternative Marktdaten (SEC-Insider-Filings, RSS-News) sammelt, über eine LLM-Klassifizierung filtert und hoch-relevante Signale als strukturierte Discord-Alerts ausliefert.

**Ziele:**
- Sauberer, produktionsreifer Code als Portfolio-/Resume-Projekt
- Docker-Compose-basiert, isolierte Services
- Niedrige Betriebskosten (<€1/Monat für LLM)
- Robust gegen single-source-failures

**Nicht-Ziele (v1):**
- On-Chain/Krypto-Quellen (v2)
- Web-UI zur Verwaltung
- Historisches Backfill
- Multi-User / Multi-Tenant
- Teure Echtzeit-Streams (Twitter/Bloomberg)

## Architektur

### Container-Topologie

Vier Python-Services + Postgres, orchestriert via Docker Compose:

```
┌────────────────────────────────────────────────────────────┐
│                       Docker Compose                       │
│                                                            │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐    │
│  │ sec-poller   │   │ rss-poller   │   │  processor   │    │
│  │ hourly       │   │ every 10min  │   │  (LLM API)   │    │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘    │
│         │                  │                  │            │
│         ▼                  ▼                  ▼            │
│  ┌────────────────────────────────────────────────┐        │
│  │           PostgreSQL (volume: pgdata)          │        │
│  │   raw_events / polling_state / llm_calls /     │        │
│  │   notifications                                │        │
│  └────────────────────┬───────────────────────────┘        │
│                       │                                    │
│                ┌──────▼───────┐                            │
│                │   notifier   │                            │
│                │  (Discord)   │                            │
│                └──────────────┘                            │
└────────────────────────────────────────────────────────────┘
```

Jeder Service hat:
- Eigenen Container
- APScheduler (`AsyncIOScheduler`) als In-Process-Scheduler
- Gemeinsames Basis-Image (ein Dockerfile, unterschiedliche `command:` pro Service)
- Healthcheck-Endpoint `GET /healthz` auf Port 8080

### Datenfluss (Registry-Pattern über DB)

Status-Spalte in `raw_events` treibt die Pipeline:

```
[SEC/RSS Poller]  →  INSERT status='new'
                          │
                          ▼
[Processor]       →  SELECT FOR UPDATE SKIP LOCKED WHERE status='new'
                     → LLM-Klassifizierung
                     → UPDATE status='processed', relevance_score, sentiment
                          │
                          ▼
[Notifier]        →  SELECT WHERE status='processed' AND score >= threshold
                     → POST Discord Webhook
                     → UPDATE status='sent', sent_at
```

`SELECT FOR UPDATE SKIP LOCKED` erlaubt horizontale Skalierung des Processors ohne Race-Conditions.

### Polling-State (Idempotenz)

Jeder Poller trackt seinen Cursor in `polling_state`:
- SEC: letzte verarbeitete Filing-URL/Timestamp
- RSS: letztes `pubDate` pro Feed

Crash + Restart = kein Re-Processing, keine Duplicate. Zusätzliche Safety-Net: Unique-Constraint auf `(source, external_id)`.

## Tech-Stack

| Bereich | Wahl | Begründung |
|---|---|---|
| Sprache | Python 3.12 | Modern, async, starkes Data/Parsing-Ökosystem |
| HTTP-Client | httpx (async, HTTP/2) | Ein Client für SEC, RSS, LLM, Discord |
| RSS-Parser | feedparser | De-facto-Standard, robust gegen kaputte Feeds |
| Scheduler | APScheduler (`AsyncIOScheduler`) | Reifes In-Process-Scheduling, keine extra Infra |
| ORM | SQLAlchemy 2.0 (async) + Alembic | Industrie-Standard, Migrations für Resume |
| DB-Driver | psycopg[binary] v3 | Modern, async, schnell |
| Config | Pydantic v2 (`BaseSettings`) + PyYAML | Type-safe Settings, Watchlist-Validierung |
| Logging | structlog | JSON-Logs, strukturiert |
| Testing | pytest + pytest-asyncio + respx | Standard-Stack |
| Linting | ruff + mypy (strict) | Modern, schnell |
| Dependency-Mgmt | uv | Schnell, modern, aktuelle Best Practice |

Bewusst **kein** Celery, Redis, FastAPI-Framework (nur Mini-Endpoint für Healthcheck) oder weitere schwere Dependencies.

## Projektstruktur

```
argus-core/
├── docker-compose.yml
├── docker-compose.prod.yml
├── Dockerfile                       # Ein Image, 4 Entrypoints
├── pyproject.toml                   # uv-lock
├── README.md
├── .env.example
├── watchlist.example.yaml
├── feeds.example.yaml
│
├── src/argus/
│   ├── __init__.py
│   ├── common/                      # Geteilter Code
│   │   ├── config.py                # Pydantic BaseSettings (Env + YAML)
│   │   ├── db.py                    # Async-Engine, Session-Factory
│   │   ├── models.py                # SQLAlchemy-ORM
│   │   ├── http.py                  # httpx-Client mit Retry/Backoff
│   │   ├── logging.py               # structlog-Setup
│   │   └── health.py                # FastAPI-Mini-Endpoint für /healthz
│   ├── llm/
│   │   ├── base.py                  # LLMProvider-Interface
│   │   ├── deepseek.py              # Default-Implementation
│   │   └── prompts.py               # Prompt-Templates
│   ├── sec_poller/
│   │   ├── __init__.py
│   │   ├── main.py                  # APScheduler-Entry
│   │   ├── edgar.py                 # SEC-Client + XML-Parser
│   │   └── pipeline.py              # Fetch → Filter → Insert
│   ├── rss_poller/
│   │   ├── main.py
│   │   ├── parser.py                # feedparser-Wrapper
│   │   └── pipeline.py
│   ├── processor/
│   │   ├── main.py
│   │   ├── worker.py                # SELECT FOR UPDATE SKIP LOCKED
│   │   └── classifier.py            # LLM-Call + Scoring-Logik
│   └── notifier/
│       ├── main.py
│       ├── worker.py
│       └── discord.py               # Webhook-Embed-Builder
│
├── alembic/
│   ├── env.py
│   └── versions/
│       └── 0001_initial.py
│
├── tests/
│   ├── conftest.py                  # pytest-fixtures, testcontainers-postgres
│   ├── fixtures/                    # SEC-XML, RSS-XML als .xml files
│   ├── unit/
│   ├── integration/
│   └── e2e/
│
└── docs/
    └── architecture.md
```

**Ein Dockerfile** mit Multi-Stage-Build. In `docker-compose.yml` bekommen alle 4 Services dasselbe Image, aber unterschiedliche `command:`-Werte (`python -m argus.sec_poller.main`, etc.). Spart Build-Zeit, hält Images konsistent.

## Datenbank-Schema

### `raw_events` (zentraler Hub)

```sql
CREATE TABLE raw_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    source          TEXT NOT NULL,        -- 'sec_form4' | 'rss'
    external_id     TEXT NOT NULL,        -- SEC accession# oder RSS-GUID
    content_hash    TEXT NOT NULL,        -- SHA-256(normalized body)
    
    ticker          TEXT,
    title           TEXT NOT NULL,
    body            TEXT NOT NULL,
    url             TEXT NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    status          TEXT NOT NULL DEFAULT 'new',
                    -- 'new' | 'processed' | 'sent' | 'skipped' | 'failed'
    error_message   TEXT,
    retry_count     INT NOT NULL DEFAULT 0,
    
    relevance_score INT,                  -- 0-100
    sentiment       TEXT,                 -- 'positive' | 'negative' | 'neutral'
    llm_summary     TEXT,
    llm_classified_at TIMESTAMPTZ,
    
    sent_at         TIMESTAMPTZ,
    
    poller_meta     JSONB,
    
    CONSTRAINT uq_source_external UNIQUE (source, external_id)
);

CREATE INDEX idx_events_status ON raw_events (status, fetched_at);
CREATE INDEX idx_events_score  ON raw_events (relevance_score) WHERE status = 'processed';
CREATE INDEX idx_events_hash   ON raw_events (content_hash);
```

### `polling_state` (Idempotenz-Cursor)

```sql
CREATE TABLE polling_state (
    source      TEXT PRIMARY KEY,         -- 'sec_form4' | 'rss:feed_url_1'
    cursor      TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### `llm_calls` (Kosten-Tracking & Audit)

```sql
CREATE TABLE llm_calls (
    id                BIGSERIAL PRIMARY KEY,
    event_id          UUID REFERENCES raw_events(id) ON DELETE CASCADE,
    provider          TEXT NOT NULL,
    model             TEXT NOT NULL,
    prompt_tokens     INT,
    completion_tokens INT,
    cost_usd          NUMERIC(10,6),  -- Direkte USD-Summe, SUM() für Monatscost
    latency_ms        INT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_llm_event ON llm_calls (event_id);
```

### `notifications` (Audit-Log für Discord)

```sql
CREATE TABLE notifications (
    id           BIGSERIAL PRIMARY KEY,
    event_id     UUID REFERENCES raw_events(id),
    channel      TEXT NOT NULL,           -- 'discord'
    webhook_id   TEXT,
    delivered    BOOLEAN NOT NULL DEFAULT false,
    discord_msg_id TEXT,
    error        TEXT,
    sent_at      TIMESTAMPTZ DEFAULT now()
);
```

### Migrations-Strategie

- `alembic revision --autogenerate` für Schema-Änderungen
- Beim Start des **processor**-Services wird `alembic upgrade head` ausgeführt (Single-Point-of-Truth, sonst rennen sich 4 Services in die Quere)
- Migrations sind idempotent (`CREATE INDEX IF NOT EXISTS`, etc.)

## Config-Files

### `watchlist.yaml`

```yaml
# Ticker-Sektor-Zuordnung. Ein Event mit Ticker auf dieser Liste wird geboostet.
# Sektor ist Metadaten (für Reporting/Filter), kein eigenständiger Match-Trigger.
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

# Keyword-Triggers (case-insensitive substring match gegen title+body).
# Relevante Einträge für RSS-Items ohne bekannten Ticker.
keywords:
  - "insider buy"
  - "FDA approval"
  - "Phase 3"
  - "share buyback program"

thresholds:
  min_relevance_score: 70        # 0-100, processor setzt Score
  form4_buy_min_usd: 100000      # Nur Form-4-Käufe ab $100k triggern
  form4_sell_min_usd: 1000000    # Verkäufe müssen größer sein (weniger Noise)
```

**Match-Logik (pre-filter im Processor):**
- Ticker auf watchlist → `relevance_score = 80` (Boost)
- Nur Keyword matcht (kein Ticker) → `relevance_score = 50` (LLM muss noch scorieren)
- Weder noch → `status='skipped'` (LLM-Call gespart)

Sektor-Zugehörigkeit allein (ohne bekannten Ticker) ist kein Match-Trigger, weil RSS-Items ohne Ticker und SEC-Filings (die immer Ticker haben) den Sektor nicht direkt ableiten lassen.

### `feeds.yaml`

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

### `.env`

```bash
POSTGRES_HOST=postgres
POSTGRES_DB=argus
POSTGRES_USER=argus
POSTGRES_PASSWORD=__CHANGE_ME__

LLM_PROVIDER=deepseek
DEEPSEEK_API_KEY=__CHANGE_ME__
DEEPSEEK_MODEL=deepseek-chat

DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...

SEC_USER_AGENT=ArgusCore/1.0 (your-email@example.com)

LOG_LEVEL=INFO
```

`.env.example` wird committet, echte `.env` ist gitignored. In `docker-compose.yml` via `env_file:` geladen.

## Service-Details

### 1. SEC Form 4 Poller

**Datenquelle:** SEC EDGAR Full-Text Search API
- Endpoint: `https://efts.sec.gov/LATEST/search-index?q="form type":"4"&dateRange=custom&startDt=...`
- Detail-XML: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=4&CIK=...`
- Pflicht: `User-Agent: ArgusCore/1.0 (email)` — SEC blockt Requests ohne.

**Verarbeitung pro Filing:**
1. XML via `lxml` parsen
2. Extrahieren: `issuer` → Ticker (SEC-Ticker-Map-Lookup), `reporting_owner`, `transaction_code` (`'P'`/`'S'`/`'A'`/`'G'`), `transaction_shares`, `transaction_price_per_share` → USD-Value
3. Filter: nur `'P'` und `'S'`, nur Value ≥ Threshold
4. `content_hash` = SHA-256(`accession_number + transaction_line`)
5. INSERT in `raw_events` mit `status='new'`, `poller_meta={'filer_name': ..., 'transaction_type': 'P', 'value_usd': ...}`

**Cadence:** stündlich via APScheduler `CronTrigger`.
**Rate-Limit:** SEC erlaubt ~10 req/s. Poller hält sich an `asyncio.Semaphore(5)`.

### 2. RSS Poller

**Verarbeitung pro Feed:**
1. Feed via `httpx` abrufen, an `feedparser` übergeben
2. Items nach `pubDate` sortieren (neueste zuerst)
3. Gegen `polling_state['rss:<feed_url>']` filtern — nur Items neuer als Cursor
4. Pro Item:
   - Ticker extrahieren: Regex `\b[A-Z]{1,5}\b` im Title, gegen NYSE/NASDAQ-Ticker-Liste validieren
   - Wenn Ticker matcht → relevant für `tickers`-Watchlist
   - Wenn Keyword aus `keywords`-Liste im Title/Body → relevant
   - Sonst: Skip, nicht in DB (spart LLM-Kosten)
5. INSERT in `raw_events` mit `status='new'`

**Cadence:** Default 10min, pro Feed überschreibbar.
**Edge-Cases:**
- Feed kaputt/404 → log warning, Continue mit nächstem Feed, Container stürzt nicht ab
- Feed ohne `pubDate` → Fallback auf Reihenfolge im XML (Cache letzte 10 GUIDs als Cursor)

### 3. Processor (LLM)

**Loop** via APScheduler `IntervalTrigger`, alle 30s:

```python
async def process_batch():
    async with db.session() as session:
        events = await session.execute(
            select(RawEvent)
            .where(RawEvent.status == 'new')
            .order_by(RawEvent.fetched_at)
            .limit(20)
            .with_for_update(skip_locked=True)
        )
        for event in events.scalars():
            await classify(event)
```

**Pre-Filter (cheap, spart LLM-Calls):**
- Ticker in Watchlist → `relevance_score = 80` (Boost)
- Nur Keyword matcht (kein Ticker oder Ticker nicht auf Watchlist) → `relevance_score = 50` (LLM muss noch scorieren)
- Nichts matcht → `status='skipped'` (LLM-Call gespart)

(Detaillierte Match-Logik siehe `watchlist.yaml`-Sektion.)

**LLM-Call** für nicht-skipped Events:
- System-Prompt: statisch (siehe LLM-Strategie)
- User-Prompt: Event-Context + Watchlist-Hinweis
- Response-Format: `response_format={'type': 'json_object'}`
- Pydantic-Validator fängt kaputte Responses ab, 1× Reprompt bei ungültigem JSON

**Update** in DB: `status='processed'`, `relevance_score`, `sentiment`, `llm_summary`, `llm_classified_at`.
**Log** in `llm_calls`: Token-Counts, Kosten, Latenz.

### 4. Notifier

**Loop** via APScheduler `IntervalTrigger`, alle 15s:

```python
async def notify_batch():
    events = await session.execute(
        select(RawEvent)
        .where(
            RawEvent.status == 'processed',
            RawEvent.relevance_score >= settings.min_relevance_score
        )
        .order_by(RawEvent.relevance_score.desc())
        .limit(10)
        .with_for_update(skip_locked=True)
    )
    for event in events.scalars():
        await send_discord_alert(event)
        event.status = 'sent'
        event.sent_at = now()
```

**Discord-Embed-Format:**

```json
{
  "embeds": [{
    "title": "INSIDER BUY | NVDA",
    "url": "https://www.sec.gov/...",
    "color": 3066993,
    "timestamp": "2026-07-05T13:00:00Z",
    "fields": [
      {"name": "Filer", "value": "Jensen Huang", "inline": true},
      {"name": "Value", "value": "$2.4M", "inline": true},
      {"name": "Relevance", "value": "92/100", "inline": true},
      {"name": "Summary", "value": "CEO Jensen Huang kaufte 50.000 Aktien..."},
      {"name": "Source", "value": "SEC Form 4"}
    ],
    "footer": {"text": "ArgusCore • DeepSeek • $0.000023"}
  }]
}
```

**Farben:** Grün (`3066993`) = positive, Rot (`15158332`) = negative, Grau (`9807270`) = neutral.

**Error-Handling:** Discord 429 → `Retry-After` respektieren, max 3 Retries, danach `status='failed'`.

## LLM-Strategie

### Provider-Abstraction

```python
class LLMProvider(Protocol):
    name: str
    model: str
    
    async def classify(self, event: RawEvent, context: str) -> Classification:
        """Returns Pydantic-validated Classification or raises."""
```

Implementations: `DeepSeekProvider` (Default), `OpenAIProvider` (Stub), `GroqProvider` (Stub). Auswahl via `LLM_PROVIDER`-Env-Var und Factory-Funktion.

### Prompt (Englisch)

**System-Prompt** (statisch, via Prompt-Caching gratis):

```
You are ArgusCore, a market intelligence classifier for insider filings 
and financial news. You receive a single event and return strict JSON.

Scoring rubric (0-100):
- 90-100: Material insider action (CEO/CFO buy ≥$500k, FDA approval, M&A)
- 70-89:  Notable but expected (regular insider activity, earnings beats)
- 50-69:  Minor relevance (analyst rating, sector news)
- 0-49:   Noise (routine filings, low-impact news)

Sentiment: positive | negative | neutral
Summary: 1 sentence, English, ≤120 chars, lead with the action.

Respond ONLY with valid JSON. No markdown, no preamble.
```

**User-Prompt (pro Event):**

```
EVENT:
Source: SEC Form 4
Ticker: NVDA
Title: Form 4 - Huang Jensen (CEO)
Body: Transaction Code: P (Purchase)
      Shares: 50,000
      Price: $48.20
      Total Value: $2,410,000
      Filer Role: Chief Executive Officer

Watchlist context: Ticker NVDA is on watchlist (Semiconductors sector).
```

**Response-Validator:**

```python
class Classification(BaseModel):
    sentiment: Literal['positive', 'negative', 'neutral']
    relevance_score: int = Field(ge=0, le=100)
    summary: str = Field(max_length=120)
```

### Kosten-Schätzung

| Szenario | Events/Tag | Tokens/Event | Kosten/Tag | Kosten/Monat |
|---|---|---|---|---|
| Low (nur SEC) | 5 | ~600 | $0.001 | $0.03 |
| Medium (SEC + RSS, pre-filter) | 50 | ~600 | $0.01 | $0.30 |
| Heavy (RSS ohne Filter) | 500 | ~600 | $0.10 | $3.00 |

Realistisch für v1 mit Pre-Filter: **<€1/Monat.**

### Fallback-Strategie

| Fehler | Verhalten |
|---|---|
| LLM 429 (Rate-Limit) | Exponential Backoff (2s, 4s, 8s), max 3 Retries |
| LLM 5xx | Retry mit Jitter, max 3 |
| LLM timeout (>30s) | Als `error` markieren, nächster Cycle |
| Kaputte JSON-Response | Reprompt mit "Please respond with valid JSON only" (1×) |
| Pre-Validator schlägt fehl | Reprompt (1×), danach `status='failed'` |
| API-Key ungültig | Container-Healthcheck rot, log critical |

Events mit `status='failed'` bleiben in DB. Manueller Requeue via `UPDATE raw_events SET status='new', retry_count=0` (dokumentiert im README).

## Error-Handling

### HTTP-Calls

Eigener `httpx.AsyncClient` mit montierten Transports:

```python
transport = httpx.AsyncHTTPTransport(
    retries=3,
    http2=True,
)
client = httpx.AsyncClient(
    timeout=httpx.Timeout(30.0, connect=10.0),
    transport=transport,
    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
)
```

Application-Level:
- `429` → `Retry-After` Header respektieren
- `5xx` → exponential backoff (1s, 2s, 4s, 8s) + Jitter
- `4xx` (außer 429) → kein Retry, hardcoded failure
- Network timeout → retry, max 3

Implementiert als Decorator `@with_retry(max_attempts=3, retryable_status={429, 500, 502, 503, 504})`.

### DB-Calls

```python
engine = create_async_engine(
    settings.database_url,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=3600,
)
```

Bei `OperationalError` (DB weg): Service schläft 5s, retryt unbegrenzt. Loggt jede Connection-Trennung als Warning.

### Container-Health

`GET /healthz` via Mini-FastAPI-Endpoint:

```python
@app.get("/healthz")
async def health():
    checks = {
        "db": await ping_db(),
        "scheduler": scheduler.running,
        "last_run": scheduler.last_run_iso,
    }
    healthy = all(checks.values())
    return JSONResponse(checks, status_code=200 if healthy else 503)
```

Docker Compose Healthcheck:
```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:8080/healthz"]
  interval: 30s
  retries: 3
  start_period: 30s
restart: unless-stopped
```

Bei 3 Failed-Checks → Compose zieht Container hoch.

### Event-Failure-States

| Status | Bedeutung | Recovery |
|---|---|---|
| `failed` | LLM/Discord nach Retries nicht durch | Manueller Requeue via SQL |
| `skipped` | Pre-Filter hat Event verworfen | Keiner, bleibt für Audit |
| `error` | Transienter Fehler, nächste Cycle retryt | Automatic |

`retry_count` Spalte in `raw_events`. Nach 3 Strikes → `failed`. Verhindert Poison-Pillen, die den Processor blockieren.

### Logging

`structlog` mit JSON-Output, korrelierte Logs über `event_id`:

```json
{"event": "processor.classify", "level": "info", "event_id": "abc-123",
 "ticker": "NVDA", "source": "sec_form4", "score": 92, "latency_ms": 480,
 "cost_usd": 0.000023, "timestamp": "2026-07-05T13:00:00Z"}
```

Produktion: STDOUT. Lokaler Dev-Mode: pretty-printed Console.

## Testing

### Test-Pyramide

```
        ┌──────────┐
        │   E2E    │  ← 1-2 Tests, voller Docker-Compose-Stack mit Mock-Servern
        ├──────────┤
        │   Int.   │  ← DB-Integration mit testcontainers-postgres
        ├──────────┤
        │   Unit   │  ← Parser, Classifier, Embed-Builder (Coverage-Ziel 85%+)
        └──────────┘
```

### Unit-Tests

- `test_edgar_parser.py` — Fixtures aus `tests/fixtures/sec/` (echte Form-4-XMLs anonymisiert)
- `test_rss_parser.py` — Verschiedene Feed-Formate (RSS 2.0, Atom, kaputte Feeds)
- `test_classifier.py` — Mock LLM-Responses (`respx`), Pydantic-Validator-Tests
- `test_discord_builder.py` — Embed-Format, Farben, Field-Order
- `test_watchlist_filter.py` — Sector/Ticker/Keyword-Logik

### Integration-Tests

- `test_pipeline_sec.py` — SEC-Mock-Server → Poller → DB-Assert
- `test_pipeline_processor.py` — DB mit `status='new'` → Processor → Assert Klassifizierung
- Postgres via **testcontainers** (echte DB, kein SQLite-Mock — wichtig für PG-spezifische Features wie `SKIP LOCKED`)

### E2E-Test

`test_full_flow.py`:
1. Docker-Compose up mit Mock-Servern (SEC, RSS, LLM, Discord)
2. Inject Test-Event in Mock-SEC
3. Warte bis Event in DB `status='sent'`
4. Assert: Mock-Discord hat Embed empfangen
5. Tear down

Läuft in CI (GitHub Actions), nicht lokal bei jedem Commit.

### CI/CD (GitHub Actions)

```yaml
jobs:
  lint:     # ruff + mypy
  unit:     # pytest tests/unit
  integration:  # pytest tests/integration (mit testcontainers)
  e2e:      # nur bei PRs nach main
  docker:   # docker build testen
```

### Pre-Commit-Hooks

- `ruff check --fix`
- `ruff format`
- `mypy src`
- `pytest tests/unit -x --ff`

Via `pre-commit` framework, `uv run pre-commit install`.

## Observability (Lightweight)

**Keine** externe Observability-Stack (Prometheus/Grafana) in v1 — Overkill für Portfolio-Zwecke.

Stattdessen:
- `GET /healthz` für Compose-Healthchecks
- Strukturierte JSON-Logs → können später nach Loki/Datadog geschickt werden
- Tägliche Cost-Aggregation aus `llm_calls`-Tabelle (CLI: `uv run argus-cli costs --since 7d`)

## Out of Scope (v2-Candidates)

- On-Chain-Quellen (Alchemy/QuickNode RPC, Whale-Tracking)
- Web-UI zur Watchlist-Verwaltung
- Historisches Backfill der SEC-Daten
- Backtesting-Framework
- Cost-Anomaly-Detection mit Slack/Discord-Warnings
- Multi-Channel-Notifications (Telegram, Email)
- Message-Broker (Redis Streams / NATS) für höhere Throughput-Anforderungen

## Open Decisions / Spätere Reviews

- **Form-4-Thresholds** (`form4_buy_min_usd: 100000`, `form4_sell_min_usd: 1000000`): v1-Defaults, sollen nach ersten 2-4 Wochen Echtbetrieb auf Signal/Noise-Verhältnis überprüft und justiert werden.
- **`restart: unless-stopped` vs `restart: always`**: v1 nimmt `unless-stopped`. Falls Container sich wiederholt selbst beenden (z.B. Config-Fehler), hilft `always` — aber dann muss Healthcheck strikt sein.
- **E2E-Test in CI**: v1 ja (CI-Zeit ~2min). Falls es sich als flappy herausstellt → v2 in Nightly-Job verschieben.
