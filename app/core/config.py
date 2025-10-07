# app/core/config.py
# 환경변수 / 비밀키 로드 설정 (pydantic-settings 기반)

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Optional

class Settings(BaseSettings):
    # 기본 환경
    app_env: str = "dev"

    # 데이터베이스 (비동기용)
    pg_dsn_async: str

    # Redis 등 추가 리소스
    redis_url: Optional[str] = None

    # CORS 허용 도메인
    cors_origins: List[str] = []

    # 추가적인 환경 변수는 무시 (예: PG_DSN, DATABASE_URL, ETL_SOURCE 등)
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="",           # .env 키 그대로 사용
        case_sensitive=False,    # 대소문자 무시
        extra="ignore"           # Settings에 없는 키는 무시
    )

# 인스턴스 생성
settings = Settings()