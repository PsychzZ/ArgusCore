from pathlib import Path

from argus.rss_poller.parser import FeedItem, parse_feed

FIXTURES = Path(__file__).parent.parent / "fixtures" / "rss"


def test_parse_rss20():
    items = parse_feed((FIXTURES / "rss20.xml").read_bytes(), base_url="https://tc.com")
    assert len(items) == 2
    first = items[0]
    assert isinstance(first, FeedItem)
    assert first.title == "Nvidia announces new AI chip NVDA-X"
    assert first.guid == "tc-001"
    assert first.url == "https://techcrunch.com/2026/07/04/nvidia-x"
    assert first.published is not None


def test_parse_atom():
    items = parse_feed((FIXTURES / "atom.xml").read_bytes(), base_url="https://example.com")
    assert len(items) == 1
    assert items[0].guid == "tag:example.com,2026:1"
    assert items[0].title == "Biotech Phase 3 results: MRNA"


def test_parse_empty_feed():
    items = parse_feed(b"<rss version='2.0'><channel></channel></rss>", base_url="x")
    assert items == []


def test_parse_invalid_returns_empty():
    items = parse_feed(b"not xml at all", base_url="x")
    assert items == []
