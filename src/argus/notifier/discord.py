from typing import Any

from argus.common.models import RawEvent

COLOR_POSITIVE = 3066993   # green
COLOR_NEGATIVE = 15158332  # red
COLOR_NEUTRAL = 9807270    # grey

_SOURCE_LABEL = {"sec_form4": "SEC Form 4", "rss": "RSS"}


def _color_for(sentiment: str | None) -> int:
    if sentiment == "positive":
        return COLOR_POSITIVE
    if sentiment == "negative":
        return COLOR_NEGATIVE
    return COLOR_NEUTRAL


def _format_value(meta: dict[str, Any] | None) -> str:
    if not meta:
        return "n/a"
    value = meta.get("value_usd")
    if value is None:
        return "n/a"
    return f"${value / 1_000_000:.2f}M" if value >= 1_000_000 else f"${value / 1000:.0f}k"


def build_embed(event: RawEvent) -> dict[str, Any]:
    source_label = _SOURCE_LABEL.get(event.source, event.source)
    title_parts = []
    if event.poller_meta and event.poller_meta.get("transaction_type"):
        code = event.poller_meta["transaction_type"]
        if code == "P":
            action = "INSIDER BUY"
        elif code == "S":
            action = "INSIDER SALE"
        else:
            action = f"FORM 4 ({code})"
        title_parts.append(action)
    parts = [*title_parts, event.ticker, source_label] if event.ticker else [source_label]
    title = " | ".join(parts)

    fields = [
        {"name": "Source", "value": source_label, "inline": True},
        {"name": "Relevance", "value": f"{event.relevance_score}/100", "inline": True},
        {"name": "Sentiment", "value": event.sentiment or "neutral", "inline": True},
    ]
    if event.poller_meta and event.poller_meta.get("filer_name"):
        fields.append({"name": "Filer", "value": event.poller_meta["filer_name"], "inline": True})
        fields.append({"name": "Value", "value": _format_value(event.poller_meta), "inline": True})
    if event.llm_summary:
        fields.append({"name": "Summary", "value": event.llm_summary})

    return {
        "embeds": [{
            "title": title[:256],
            "url": event.url,
            "color": _color_for(event.sentiment),
            "fields": fields,
            "footer": {"text": "ArgusCore • DeepSeek"},
        }]
    }
