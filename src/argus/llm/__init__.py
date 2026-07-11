from argus.common.config import Settings
from argus.llm.base import LLMProvider


def create_provider(settings: Settings) -> LLMProvider:
    """Build the LLM provider selected via LLM_PROVIDER."""
    if settings.llm_provider == "deepseek":
        if not settings.deepseek_api_key:
            raise ValueError("DEEPSEEK_API_KEY is required for llm_provider=deepseek")
        from argus.llm.deepseek import DeepSeekProvider

        return DeepSeekProvider(api_key=settings.deepseek_api_key, model=settings.deepseek_model)
    if settings.llm_provider == "gemini":
        if not settings.gemini_api_key:
            raise ValueError("GEMINI_API_KEY is required for llm_provider=gemini")
        from argus.llm.gemini import GeminiProvider

        return GeminiProvider(api_key=settings.gemini_api_key, model=settings.gemini_model)
    raise ValueError(f"Unknown llm_provider: {settings.llm_provider}")
