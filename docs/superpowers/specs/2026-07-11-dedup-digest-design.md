# Design: Cross-Source-Dedup + Hybrid-Digest

Datum: 2026-07-11
Status: Approved (User, 2026-07-11)

## Ziel

Doppelte Stories über Quellen hinweg nur einmal melden und den Discord-Kanal
beruhigen: nur sehr relevante Events pingen sofort, der Rest kommt einmal
täglich gebündelt.

## Entscheidungen

- Zustellung: **Hybrid** — Score ≥ `instant_score` sofort, sonst täglicher Digest.
- Dedup-Methode: **Heuristik** (Ticker + Fuzzy-Titel + Zeitfenster), kein LLM,
  keine Embeddings, keine neuen Dependencies.

## 1. Deduplizierung (Processor, vor dem LLM-Call)

Beim Verarbeiten eines Events mit Status `new` prüft der Processor, ob ein
früheres Event existiert mit:

- gleichem `ticker` (bei Events ohne Ticker: gleicher `source`),
- `fetched_at` innerhalb der letzten 48 Stunden,
- Titel-Ähnlichkeit ≥ 0.75 via `difflib.SequenceMatcher` auf normalisierten
  Titeln (lowercase, Whitespace kollabiert, Satzzeichen entfernt).

Trifft das zu:

- Status → `duplicate`, neues Feld `duplicate_of` (UUID, FK auf Original),
- kein LLM-Call (spart Kosten), keine Notification, kein Digest-Eintrag.

Kandidatensuche per SQL (Ticker/Source + Zeitfenster + Status in
(`processed`, `new`)), Fuzzy-Vergleich in Python über die Kandidaten.

## 2. Hybrid-Zustellung (Notifier)

Neuer Threshold `instant_score` (Default **90**) in `watchlist.yaml`
`thresholds`:

- `relevance_score ≥ instant_score` → sofortiger Einzel-Ping (wie bisher).
- `min_relevance_score ≤ score < instant_score` → kein Ping; Event wird vom
  täglichen Digest eingesammelt.

Digest-Job (eigener Cron im Notifier, Default 18:00 UTC, konfigurierbar via
Env `DIGEST_HOUR`):

- sammelt alle `processed`-Events seit dem letzten Digest-Lauf im
  Digest-Score-Band, sortiert nach Score absteigend,
- sendet **ein** Discord-Embed mit max. 10 Einträgen (Titel, Ticker, Score,
  Link); mehr als 10 → Fußzeile „+N weitere",
- sendet nichts, wenn keine Events anliegen,
- Cursor (`digest:last_sent`, ISO-Timestamp) in `polling_state`; existiert
  noch kein Cursor (Erstlauf), werden die letzten 24 h eingesammelt.

Bereits sofort gepingte Events (≥ instant_score) erscheinen nicht nochmal im
Digest.

## 3. Schema-Migration

Eine Alembic-Revision:

- `raw_events.duplicate_of UUID NULL` (FK `raw_events.id`),
- kein DB-Enum — `status` ist String; neuer Wert `duplicate` braucht nur den
  neuen Index-freundlichen Statuswert.

Rückwärtskompatibel; bestehende Zeilen unverändert.

## 4. Fehlerbehandlung

- Dedup-Fehler (z. B. DB-Timeout bei Kandidatensuche) → Event wird normal
  klassifiziert (fail open, lieber ein Duplikat zu viel als ein Event
  verloren).
- Digest-Sendefehler → Cursor wird NICHT vorgerückt; nächster Lauf versucht
  dieselben Events erneut. Discord-Retry übernimmt der bestehende
  DiscordClient.

## 5. Tests

Unit:

- Dedup: umformulierte gleiche Story → Duplikat; gleicher Ticker, andere
  Story → kein Duplikat; 49 h Abstand → kein Duplikat; Event ohne Ticker
  nur gegen gleiche Quelle.
- Routing: Score 89 → Digest-Band, 90 → Sofort-Ping.
- Digest-Formatierung: Sortierung, Top-10-Kappung, leerer Digest → kein Send.

Integration:

- Processor markiert Duplikat und macht keinen LLM-Call.
- Digest-Lauf end-to-end gegen DB-Fixtures inkl. Cursor-Fortschritt und
  Nicht-Fortschritt bei Sendefehler.
