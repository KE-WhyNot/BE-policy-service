"""
Pytest configuration file for Policy Service API Tests
"""
import pytest
import asyncio
from fastapi.testclient import TestClient
from app.main import create_app


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("Event loop is closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    yield loop
    # 모든 테스트 종료 후 DB 엔진 정리
    try:
        from app.core.db import engine, fin_engine
        # 동기적으로 비동기 dispose 실행
        loop.run_until_complete(engine.dispose())
        loop.run_until_complete(fin_engine.dispose())
    except Exception as e:
        print(f"Warning: Error disposing engines: {e}")
    finally:
        loop.close()


@pytest.fixture
def app():
    """Create and configure a new app instance for each test."""
    return create_app()


@pytest.fixture
def client(app, event_loop):
    """A test client for the app."""
    with TestClient(app) as client:
        yield client

