"""
Pytest configuration file for Policy Service API Tests
비동기 환경에서 올바른 테스트를 위한 설정
"""
import pytest
import asyncio
from httpx import AsyncClient
from fastapi.testclient import TestClient
from app.main import create_app


# Event Loop 설정
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def app():
    """Create and configure a new app instance for each test."""
    return create_app()


@pytest.fixture
def client(app):
    """동기식 테스트 클라이언트 (DB 연결 없는 간단한 테스트용)"""
    return TestClient(app)


@pytest.fixture
async def async_client(app):
    """비동기 테스트 클라이언트 (DB 연결 테스트용)"""
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac


@pytest.fixture
async def db_session():
    """테스트용 DB 세션"""
    from app.core.db import engine, SessionLocal
    
    # 테스트 후 엔진 정리
    async with SessionLocal() as session:
        yield session
        
    # 테스트 종료 후 연결 정리
    await engine.dispose()