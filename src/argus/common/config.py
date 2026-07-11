from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Postgres
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "argus"
    postgres_user: str = "argus"
    postgres_password: str

    # LLM
    llm_provider: str = "deepseek"
    deepseek_api_key: str | None = None
    deepseek_model: str = "deepseek-chat"
    gemini_api_key: str | None = None
    gemini_model: str = "gemini-2.5-flash"

    # Discord
    discord_webhook_url: str

    # SEC
    sec_user_agent: str

    # Operational
    log_level: str = "INFO"
    health_port: int = 8080
    digest_hour: int = 18
    form4_cluster_window_h: int = 48
    watchlist_path: str = "watchlist.yaml"
    feeds_path: str = "feeds.yaml"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


class WatchlistConfig(BaseModel):
    watchlist: list[dict[str, str]] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    thresholds: dict[str, int] = Field(
        default_factory=lambda: {
            "min_relevance_score": 70,
            "instant_score": 90,
            "form4_buy_min_usd": 100_000,
            "form4_sell_min_usd": 1_000_000,
            "form4_cluster_min_filings": 2,
        }
    )


def load_watchlist(path: str | Path) -> WatchlistConfig:
    """Load a watchlist YAML file and return a typed WatchlistConfig."""
    data: dict[str, Any] = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    thresholds = data.get("thresholds") or {}
    data["thresholds"] = {
        "min_relevance_score": 70,
        "instant_score": 90,
        "form4_buy_min_usd": 100_000,
        "form4_sell_min_usd": 1_000_000,
        "form4_cluster_min_filings": 2,
        **thresholds,
    }
    return WatchlistConfig.model_validate(data)


def load_feeds(path: str | Path) -> dict[str, Any]:
    """Load a feeds YAML file and return its parsed contents."""
    data: dict[str, Any] = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return data
