"""Title-similarity heuristics for cross-source duplicate detection."""

import re
from difflib import SequenceMatcher

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
