import json


def test_log_emits_json(capsys):
    from argus.common.logging import get_logger, setup_logging

    setup_logging("INFO")
    log = get_logger("test")
    log.info("hello", ticker="NVDA", score=92)
    out = capsys.readouterr().out.strip()
    parsed = json.loads(out)
    assert parsed["event"] == "hello"
    assert parsed["ticker"] == "NVDA"
    assert parsed["score"] == 92
    assert parsed["level"] == "info"


def test_log_respects_level(capsys):
    from argus.common.logging import get_logger, setup_logging

    setup_logging("WARNING")
    log = get_logger("test")
    log.info("should-not-appear")
    log.warning("should-appear")
    out = capsys.readouterr().out.strip()
    assert "should-not-appear" not in out
    assert "should-appear" in out
