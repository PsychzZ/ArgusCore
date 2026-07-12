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
- **processor** — classifies new events via LLM (DeepSeek or Gemini, relevance score 0-100, sentiment, summary); cross-source duplicates (same ticker, similar title within 48h) are marked `duplicate` before the LLM call and never notified
- **notifier** — hybrid delivery: events with score >= `instant_score` (default 90) ping Discord immediately; the band between `min_relevance_score` and `instant_score` is collected into one daily digest embed sent at `DIGEST_HOUR` UTC (default 18). A separate hourly job detects Form-4 clusters (>= `form4_cluster_min_filings` same-ticker filings within `form4_cluster_window_h`) and sends one instant "Form 4 Cluster" embed per ticker

Services communicate via shared Postgres: the `raw_events.status` column acts as a queue
(`new → processed → sent`, with `duplicate` and `failed` as terminal side states).

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ sec-poller   │   │ rss-poller   │   │  processor   │
│   (hourly)   │   │  (10 min)    │   │  (LLM API)   │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                  │                  │
       ▼                  ▼                  ▼
   ┌────────────────────────────────────────────┐
   │         PostgreSQL (raw_events)            │
   │   status: new → processed → sent           │
   └──────────────────┬─────────────────────────┘
                      │
              ┌───────▼───────┐
              │   notifier    │
              │   (Discord)   │
              └───────────────┘
```

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

## E2E Testing

The E2E test (`tests/e2e/test_full_flow.py`) requires Docker and validates the full pipeline against mock SEC/LLM/Discord endpoints. It is environment-specific and skipped by default — implement the mock expectations on first deployment to verify end-to-end behavior.

## License

MIT
