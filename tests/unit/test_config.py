import pytest
from pydantic import ValidationError


def test_settings_loads_from_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "dbhost")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/1/abc")
    monkeypatch.setenv("SEC_USER_AGENT", "Test/1.0 (test@example.com)")
    from argus.common.config import Settings

    s = Settings()
    assert s.postgres_host == "dbhost"
    assert s.database_url == "postgresql+psycopg://argus:secret@dbhost:5432/argus"


def test_settings_requires_api_key(monkeypatch):
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    from argus.common.config import Settings

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_watchlist_loads_yaml(tmp_path):
    yaml_file = tmp_path / "watchlist.yaml"
    yaml_file.write_text("""
watchlist:
  - ticker: NVDA
    sector: Semiconductors
keywords:
  - "insider buy"
thresholds:
  min_relevance_score: 70
  form4_buy_min_usd: 100000
  form4_sell_min_usd: 1000000
""")
    from argus.common.config import load_watchlist

    wl = load_watchlist(yaml_file)
    assert wl.watchlist[0]["ticker"] == "NVDA"
    assert "insider buy" in wl.keywords
    assert wl.thresholds["form4_buy_min_usd"] == 100000


def test_thresholds_include_instant_score_default(tmp_path):
    from argus.common.config import load_watchlist

    p = tmp_path / "watchlist.yaml"
    p.write_text("watchlist: []\nkeywords: []\n", encoding="utf-8")
    cfg = load_watchlist(p)
    assert cfg.thresholds["instant_score"] == 90


def test_thresholds_include_form4_cluster_min_filings_default(tmp_path):
    from argus.common.config import load_watchlist

    p = tmp_path / "watchlist.yaml"
    p.write_text("watchlist: []\nkeywords: []\n", encoding="utf-8")
    cfg = load_watchlist(p)
    assert cfg.thresholds["form4_cluster_min_filings"] == 2


def test_settings_default_form4_cluster_window(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/1/abc")
    monkeypatch.setenv("SEC_USER_AGENT", "Test/1.0 (test@example.com)")
    from argus.common.config import Settings

    assert Settings().form4_cluster_window_h == 48
