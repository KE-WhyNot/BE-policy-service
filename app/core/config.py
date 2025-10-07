### config.py ###
# pydantic-settings (환경변수/비밀키 로드)

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List

class Settings(BaseSettings):
    app_env: str = "dev"
    pg_dsn_async: str
    redis_url: str | None = None
    cors_origins: List[str] = []

    model_config = SettingsConfigDict(env_file=".env", env_prefix="", case_sensitive=False)

settings = Settings()