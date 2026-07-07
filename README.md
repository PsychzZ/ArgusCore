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

## License

MIT
