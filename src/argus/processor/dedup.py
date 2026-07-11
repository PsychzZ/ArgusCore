"""Title-similarity heuristics for cross-source duplicate detection."""

import re
from datetime import UTC, datetime, timedelta
from difflib import SequenceMatcher

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from argus.common.models import RawEvent

SIMILARITY_THRESHOLD = 0.75
WINDOW_HOURS = 48

_PUNCT_RE = re.compile(r"[^\w\s]")
_WS_RE = re.compile(r"\s+")


def normalize_title(title: str) -> str:
    """Lowercase, strip punctuation, and collapse whitespace."""
    t = _PUNCT_RE.sub("", title.lower())
    return _WS_RE.sub(" ", t).strip()


def titles_similar(a: str, b: str, threshold: float = SIMILARITY_THRESHOLD) -> bool:
    """Return True if the normalized titles are similar enough to be duplicates."""
    ratio = SequenceMatcher(None, normalize_title(a), normalize_title(b)).ratio()
    return ratio >= threshold


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
