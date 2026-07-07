from dataclasses import dataclass
from enum import StrEnum

from argus.common.config import WatchlistConfig
from argus.common.models import RawEvent


class Action(StrEnum):
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
