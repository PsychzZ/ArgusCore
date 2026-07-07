from argus.common.config import WatchlistConfig
from argus.common.models import RawEvent
from argus.processor.filters import WatchlistFilter


def make_event(ticker=None, title="t", body="b"):
    return RawEvent(
        source="rss",
        external_id="x",
        content_hash="h",
        ticker=ticker,
        title=title,
        body=body,
        url="u",
    )


def make_wl():
    return WatchlistConfig.model_validate(
        {
            "watchlist": [
                {"ticker": "NVDA", "sector": "Semiconductors"},
                {"ticker": "MRNA", "sector": "Biotech"},
            ],
            "keywords": ["insider buy", "Phase 3"],
            "thresholds": {},
        }
    )


def test_ticker_match_boost():
    f = WatchlistFilter(make_wl())
    result = f.classify(make_event(ticker="NVDA"))
    assert result.score == 80
    assert result.action == "classify"


def test_keyword_match_only():
    f = WatchlistFilter(make_wl())
    result = f.classify(make_event(ticker="UNKNOWN", title="Company X Phase 3 results"))
    assert result.score == 50
    assert result.action == "classify"


def test_no_match_skip():
    f = WatchlistFilter(make_wl())
    result = f.classify(make_event(ticker="UNKN", title="random", body="boring"))
    assert result.action == "skip"


def test_ticker_not_in_list_but_keyword_present():
    f = WatchlistFilter(make_wl())
    result = f.classify(make_event(ticker="UNKNOWN", title=" insider buy reported"))
    assert result.action == "classify"
    assert result.score == 50
