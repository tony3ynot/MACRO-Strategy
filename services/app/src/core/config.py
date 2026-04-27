from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")

    database_url: str
    sync_database_url: str
    redis_url: str

    api_port: int = 8000
    log_level: str = "INFO"
    tz: str = "UTC"

    polygon_api_key: str | None = None
    deribit_client_id: str | None = None
    deribit_client_secret: str | None = None

    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None


@lru_cache
def get_settings() -> Settings:
    return Settings()
