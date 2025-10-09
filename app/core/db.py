### db.py ###
# SQLAlchemy Async 엔진/세션 팩토리

import os
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import NullPool
from .config import settings

class Base(DeclarativeBase):
    pass

# 테스트 실행 여부를 감지해 asyncpg 이벤트 루프 충돌을 방지
_is_test_env = settings.app_env.lower() == "test" or os.getenv("PYTEST_CURRENT_TEST") is not None

# Policy DB
engine_kwargs = {"pool_pre_ping": True}
if _is_test_env:
    engine_kwargs["poolclass"] = NullPool

engine = create_async_engine(settings.pg_dsn_async, **engine_kwargs)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        yield session


# FinProduct DB
fin_engine_kwargs = {"pool_pre_ping": True}
if _is_test_env:
    fin_engine_kwargs["poolclass"] = NullPool

fin_engine = create_async_engine(settings.pg_dsn_async_fin, **fin_engine_kwargs)
FinSessionLocal = async_sessionmaker(fin_engine, class_=AsyncSession, expire_on_commit=False)
    
async def get_fin_db() -> AsyncGenerator[AsyncSession, None]:
    async with FinSessionLocal() as session:
        yield session
