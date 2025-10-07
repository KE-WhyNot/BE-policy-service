### db.py ###
# SQLAlchemy Async 엔진/세션 팩토리

from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from .config import settings

class Base(DeclarativeBase):
    pass

# Policy DB

engine = create_async_engine(settings.pg_dsn_async, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        yield session


# FinProduct DB

fin_engine = create_async_engine(settings.pg_dsn_async_fin, pool_pre_ping=True)
FinSessionLocal = async_sessionmaker(fin_engine, class_=AsyncSession, expire_on_commit=False)
    
async def get_fin_db() -> AsyncGenerator[AsyncSession, None]:
    async with FinSessionLocal() as session:
        yield session