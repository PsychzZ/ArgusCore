import pytest

from argus.common.config import Settings
from argus.llm import create_provider
from argus.llm.deepseek import DeepSeekProvider
from argus.llm.gemini import GeminiProvider

BASE_ENV = {
    "postgres_password": "pw",
    "discord_webhook_url": "http://example.com/hook",
    "sec_user_agent": "test agent",
}


def make_settings(**overrides):
    return Settings(_env_file=None, **BASE_ENV, **overrides)


@pytest.mark.asyncio
async def test_create_deepseek_provider():
    settings = make_settings(llm_provider="deepseek", deepseek_api_key="sk-test")
    provider = create_provider(settings)
    assert isinstance(provider, DeepSeekProvider)
    await provider.close()


@pytest.mark.asyncio
async def test_create_gemini_provider():
    settings = make_settings(llm_provider="gemini", gemini_api_key="g-test")
    provider = create_provider(settings)
    assert isinstance(provider, GeminiProvider)
    assert provider.model == settings.gemini_model
    await provider.close()


def test_unknown_provider_raises():
    settings = make_settings(llm_provider="nope", deepseek_api_key="sk-test")
    with pytest.raises(ValueError, match="nope"):
        create_provider(settings)


def test_missing_api_key_raises():
    settings = make_settings(llm_provider="gemini")
    with pytest.raises(ValueError, match="GEMINI_API_KEY"):
        create_provider(settings)
