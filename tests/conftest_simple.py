"""
Simple test configuration without complex database mocking
"""
import pytest
from fastapi.testclient import TestClient
from app.main import create_app


@pytest.fixture
def app():
    """Create FastAPI app instance"""
    return create_app()


@pytest.fixture
def client(app):
    """Create test client"""
    return TestClient(app)