import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import feedparser

from argus.common.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class FeedItem:
    title: str
    body: str
    url: str
    guid: str
    published: datetime | None


def _parse_date(entry: Any) -> datetime | None:
            return datetime(*v[:6], tzinfo=UTC)


def parse_feed(xml_bytes: bytes, base_url: str = "") -> list[FeedItem]:
    parsed = feedparser.parse(xml_bytes)
    if parsed.bozo and not parsed.entries:
        log.warning("rss.parse_failed", base_url=base_url, error=str(parsed.bozo_exception))
        return []
    items: list[FeedItem] = []
    for entry in parsed.entries:
        items.append(
            FeedItem(
                title=entry.get("title", ""),
                body=entry.get("description") or entry.get("summary", ""),
                url=entry.get("link", ""),
                guid=entry.get("id") or entry.get("guid") or entry.get("link", ""),
                published=_parse_date(entry),
            )
        )
    return items
