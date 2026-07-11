# Form-4 Cluster Detection — Design Spec

> **Goal:** Detect when multiple Form-4 filings for the same ticker land within a
> short window and surface that as a single, high-value Discord alert — separate
> from the per-event pings and the daily digest.

**Status:** Design approved 2026-07-11. Implementation plan: `docs/superpowers/plans/2026-07-11-form4-cluster.md`.

**Tech stack:** Python 3.12, SQLAlchemy async, Alembic, pytest + respx + testcontainers.
Test runs use `uv run --extra dev pytest ...`.

---

## 1. Definition of a cluster

A **cluster** is ≥ `form4_cluster_min_filings` (default **2**) `Form 4` filings for the
**same ticker** within a `form4_cluster_window_h` (default **48h**) sliding window,
**regardless of transaction type** (buys and sells both count). Scope is **all
tickers** — the SEC poller already ingests every `sec_form4` event above its USD
thresholds, so clustering is detected across the whole market, not just the watchlist.

Clusters are detected by a **batch scan job** on cron (hourly, 5 min after the SEC
poll), not at insert time. Each detected cluster is delivered as its **own instant
Discord embed**. Member events continue to flow through their normal instant/digest
path independently.

---

## 2. Architecture & components

| File | Responsibility |
|------|----------------|
| `src/argus/processor/cluster.py` | `find_form4_clusters(session, since, min_filings)` — pure-ish query that groups `source='sec_form4'` events in `[since, now]` by ticker and returns clusters with ≥ `min_filings` members. |
| `src/argus/notifier/discord.py` | `build_cluster_embed(ticker, members)` — embed builder, next to `build_embed` / `build_digest_embed`. |
| `src/argus/notifier/form4_cluster.py` | `Form4ClusterJob` — scan → detect → upsert cluster row (idempotency) → send embed. |
| `src/argus/notifier/main.py` | Wire `Form4ClusterJob` into the scheduler at `minute=5` hourly. |
| `alembic/versions/0003_form4_clusters.py` | New `form4_clusters` table. |

`Form4ClusterJob` is a **singleton cron job** (one scheduled trigger per scheduler
process), not a horizontally-pooled `SELECT … FOR UPDATE SKIP LOCKED` worker like
`ProcessorWorker` / `NotifierWorker`. No cross-worker locking is required.

---

## 3. Data model — `form4_clusters` table (migration `0003`)

```text
form4_clusters
  id                UUID PK
  ticker            VARCHAR(16)  NOT NULL
  member_event_ids  JSONB        NOT NULL   -- list[uuid] of RawEvent.id
  window_start      TIMESTAMPTZ  NOT NULL
  window_end        TIMESTAMPTZ  NOT NULL
  alerted_at        TIMESTAMPTZ  NULL       -- set only after a successful send
  created_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
```

Indexes:
- `idx_form4_clusters_ticker_created` on `(ticker, created_at)` — supports the
  open-cluster lookup in `run_once`.

`down_revision = "0002_duplicate_of"`.

**`poller_meta` enrichment (no migration):** `SecPipeline` currently persists
`{filer_name, transaction_type, value_usd}` into `poller_meta`. Add `filer_role` so the
cluster embed can render the filer line entirely from structured fields (JSONB needs no
schema migration). The cluster job reads `filer_name`, `filer_role`, `transaction_type`,
and `value_usd` from `poller_meta` — no `body`/`title` parsing.

### Idempotency rule (one alert per ticker per ~window)

On each scan, for a ticker group with ≥ `min_filings` members:

- **Open cluster exists** (`ticker == T AND created_at >= now - window_h`):
  update `member_event_ids` and `window_end = now`. Keeps the record current for the
  future dashboard. **No re-alert.**
- **No open cluster**: insert a row with `member_event_ids`, `window_start = since`,
  `window_end = now`, and `alerted_at = now`; send the embed.

Rationale: one alert per ticker per ~48h of activity avoids alert fatigue while still
capturing a continuous burst as a single signal. The stored member set is kept fresh
so the (later) dashboard can show the full cluster. Re-alert-on-growth is deliberately
out of scope for v1 (YAGNI).

---

## 4. Detection & data flow

### `find_form4_clusters(session, since, min_filings)`

```sql
SELECT ticker, array_agg(id ORDER BY fetched_at), count(*)
FROM raw_events
WHERE source = 'sec_form4'
  AND fetched_at >= :since
  AND ticker IS NOT NULL
  AND status IN ('new', 'processed', 'sent')
GROUP BY ticker
HAVING count(*) >= :min_filings
```

Returns a list of lightweight candidates, each carrying `ticker` and the member
`RawEvent` rows (so the embed can read `poller_meta` for filer/type/value).

### `Form4ClusterJob.run_once`

1. `now = datetime.now(UTC)`; `since = now - timedelta(hours=window_h)`.
2. `candidates = await find_form4_clusters(session, since, min_filings)`.
3. For each candidate (in a single transaction):
   - Look up an open cluster row for the ticker.
   - If open row exists → `UPDATE member_event_ids, window_end` (no send).
   - Else → `embed = build_cluster_embed(ticker, members)`;
     `ok = await discord.send_embed(embed)`.
     - **Send-then-insert:** only `INSERT` the cluster row (with `alerted_at = now`)
       after `ok` is true. If send fails, do **not** persist the row, so the next
       scan retries (at-least-once delivery).
4. Return the number of embeds sent.

Members are read within the same session; no row locking is needed because the job is
a singleton.

---

## 5. Embed format — `build_cluster_embed(ticker, members)`

- **Title:** `Form 4 Cluster — {TICKER}` (uppercased ticker), using a distinct alert
  color (e.g. orange) so it stands apart from the neutral daily digest and the default
  per-event embed.
- **Body lines:**
  - `N filings in 48h` (count over the window).
  - `Buys: $X · Sells: $Y` — net from each member's `poller_meta.transaction_type`
    (`P`/`S`) and `poller_meta.value_usd`.
  - Filer list, one line per member (all fields from `poller_meta`):
    `• {filer_name} ({filer_role or "—"}) — {P|S} ${value_usd:,.0f}`.
    Capped at **10** lines; if more, append `+{N} more`.
- The embed reads `filer_name`, `filer_role`, `transaction_type`, and `value_usd` from
  each member's `poller_meta` (see §3 enrichment). Per-share price/quantity are not
  surfaced in the cluster embed (kept in the per-event embed and `body` text).

The member events themselves are **not** mutated or marked by the cluster job — they
keep their own `status` and are delivered through `NotifierWorker` / `DigestJob` as
before.

---

## 6. Configuration

- `Settings.form4_cluster_window_h: int = 48` — added under the `# Operational`
  block. Documented in `.env.example` as `FORM4_CLUSTER_WINDOW_H=48`.
- `form4_cluster_min_filings: 2` — added to **both** the `WatchlistConfig`
  `default_factory` thresholds dict **and** the `load_watchlist` merge dict (so an
  existing watchlist YAML without the key still gets the default). Documented in
  `watchlist.example.yaml` next to `form4_buy_min_usd` with a comment.

Both values are overridable without code changes; the window lives in Settings, the
count in the watchlist thresholds (parallel to the existing `form4_*` thresholds).

---

## 7. Wiring — `notifier/main.py`

After constructing `NotifierWorker` and `DigestJob`:

```python
cluster_job = Form4ClusterJob(
    sessions=sessions,
    discord=discord,
    window_h=settings.form4_cluster_window_h,
    min_filings=watchlist.thresholds["form4_cluster_min_filings"],
)
scheduler.add_job(
    tracked(cluster_job.run_once, last_run),
    CronTrigger(hour="*", minute=5),
)
```

Runs at `:05` hourly — 5 minutes after the SEC poller (`minute=0`) so the latest
hour's filings are present.

---

## 8. Error handling (fail-open)

- Cluster query / grouping raises → log exception, `run_once` returns 0; the scheduler
  keeps running (matches `ProcessorWorker` / `DigestJob` behavior).
- `discord.send_embed` returns falsy → log warning, **do not** persist the cluster
  row, continue. Retried on the next scan (at-least-once).
- A crash between a successful send and the `INSERT` commit is rare; it yields a
  possible duplicate embed on retry rather than a lost one (favoring over-delivery).

---

## 9. Testing

- **Unit** (`tests/unit/test_discord_client.py` additions): `build_cluster_embed` —
  correct title/color, `Buys/Sells` net math, 10-line cap + `+N more`, single-filing
  excluded by the caller (grouping), empty filer role falls back to `—`.
- **Unit/integration** (`tests/unit` or `tests/integration/test_cluster.py`):
  `find_form4_clusters` — groups by ticker, excludes `duplicate`/`failed` statuses,
  drops groups below `min_filings`, excludes filings older than `since`, ignores
  `ticker IS NULL`.
- **Integration** (`tests/integration/test_form4_cluster_job.py`, testcontainers):
  - 2 filings for one ticker within window → exactly 1 embed sent, 1 `form4_clusters`
    row with `alerted_at` set.
  - Re-run `run_once` → no additional embed (open cluster updated, not re-alerted).
  - 1 filing only → no embed, no row.
  - Filing older than window → excluded.
  - `discord.send_embed` returns `False` → no row persisted; `run_once` returns 0;
    retried (row absent) on next call.
  - Different tickers each with ≥2 filings → one embed per ticker.

Full verification before merge: `uv run --extra dev pytest tests/unit tests/integration -q && uv run --extra dev ruff check src tests && uv run --extra dev mypy src`.

---

## 10. Out of scope (v1)

- Re-alerting when a cluster's member count grows after the first alert.
- Restricting clusters to watchlist tickers (flag may come later).
- Dashboard UI for viewing stored clusters (the `form4_clusters` table exists to
  support this later).
- Per-filer (insider) clustering across tickers.
