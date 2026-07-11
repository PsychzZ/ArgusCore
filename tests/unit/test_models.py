from argus.common.models import LlmCall, RawEvent


def test_raw_event_default_status():
    e = RawEvent(
        source="sec_form4",
        external_id="0001209191-24-000123",
        content_hash="abc",
        title="Form 4",
        body="...",
        url="https://sec.gov/...",
    )
    assert e.status == "new"
    assert e.retry_count == 0


def test_raw_event_status_setter():
    e = RawEvent(
        source="rss",
        external_id="guid-1",
        content_hash="x",
        title="t",
        body="b",
        url="u",
    )
    e.status = "processed"
    e.relevance_score = 85
    assert e.status == "processed"
    assert e.relevance_score == 85


def test_raw_event_has_duplicate_of_column():
    col = RawEvent.__table__.columns["duplicate_of"]
    assert col.nullable is True
    fks = list(col.foreign_keys)
    assert fks and fks[0].column.table.name == "raw_events"


def test_llm_call_relationship():
    call = LlmCall(
        event_id="00000000-0000-0000-0000-000000000000",
        provider="deepseek",
        model="deepseek-chat",
        prompt_tokens=100,
        completion_tokens=50,
        cost_usd=0.0001,
        latency_ms=480,
    )
    assert call.provider == "deepseek"
