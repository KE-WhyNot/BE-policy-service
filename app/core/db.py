### db.py ###
# SQLAlchemy Async 엔진/세션 팩토리

from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import NullPool
from .config import settings

class Base(DeclarativeBase):
    pass

# Policy DB
engine = create_async_engine(
    settings.pg_dsn_async,
    poolclass=NullPool,  # sync TestClient 환경에서도 안전하도록 풀을 사용하지 않음
)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        yield session


# FinProduct DB
fin_engine = create_async_engine(
    settings.pg_dsn_async_fin,
    poolclass=NullPool,
)
FinSessionLocal = async_sessionmaker(fin_engine, class_=AsyncSession, expire_on_commit=False)
    
async def get_fin_db() -> AsyncGenerator[AsyncSession, None]:
    async with FinSessionLocal() as session:
        yield session
