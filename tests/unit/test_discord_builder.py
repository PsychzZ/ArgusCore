from argus.common.models import RawEvent
from argus.notifier.discord import (
    COLOR_NEGATIVE,
    COLOR_NEUTRAL,
    COLOR_POSITIVE,
    build_embed,
)


def make_event(**kwargs):
    base = dict(
        source="sec_form4",
        external_id="x",
        content_hash="h",
        ticker="NVDA",
        title="Form 4 - Huang",
        body="Transaction: P",
        url="https://sec.gov/...",
        sentiment="positive",
        relevance_score=92,
        llm_summary="CEO Jensen Huang bought 50,000 shares for $2.4M.",
        poller_meta={"filer_name": "Huang Jensen", "transaction_type": "P", "value_usd": 2_410_000},
    )
    base.update(kwargs)
    return RawEvent(**base)


def test_build_embed_positive():
    e = make_event()
    payload = build_embed(e)
    assert "embeds" in payload
    embed = payload["embeds"][0]
    assert "NVDA" in embed["title"]
    assert embed["color"] == COLOR_POSITIVE
    assert embed["url"] == "https://sec.gov/..."
    assert any(f["name"] == "Relevance" for f in embed["fields"])
    assert any(f["name"] == "Summary" for f in embed["fields"])
    assert embed["footer"]["text"].startswith("ArgusCore")


def test_build_embed_negative_color():
    e = make_event(sentiment="negative")
    payload = build_embed(e)
    assert payload["embeds"][0]["color"] == COLOR_NEGATIVE


def test_build_embed_neutral_color():
    e = make_event(sentiment="neutral")
    payload = build_embed(e)
    assert payload["embeds"][0]["color"] == COLOR_NEUTRAL


def test_build_embed_rss_source():
    e = make_event(source="rss", sentiment="neutral", poller_meta=None)
    payload = build_embed(e)
    assert "RSS" in payload["embeds"][0]["title"]
