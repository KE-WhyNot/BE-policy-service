"""
Health Check API 테스트
"""
import pytest
from fastapi.testclient import TestClient
from app.main import create_app


def test_health_check_endpoint_exists(client):
    """Health check 엔드포인트 존재 확인"""
    response = client.get("/api/health")
    
    # 엔드포인트가 존재하고 응답이 있어야 함
    assert response.status_code == 200


def test_health_check_response_format(client):
    """Health check 응답 형식 확인"""
    response = client.get("/api/health")
    
    assert response.status_code == 200
    assert "application/json" in response.headers["content-type"]
    
    data = response.json()
    assert isinstance(data, dict)
    assert "status" in data
    assert "service" in data


def test_health_check_response_values(client):
    """Health check 응답 값 확인"""
    response = client.get("/api/health")
    data = response.json()
    
    assert data["status"] == "ok"
    assert data["service"] == "policy-api"


def test_health_check_method_not_allowed(client):
    """Health check POST 요청 시 405 에러 확인"""
    response = client.post("/api/health")
    assert response.status_code == 405


def test_health_check_performance(client):
    """Health check 응답 시간 확인"""
    import time
    
    start_time = time.time()
    response = client.get("/api/health")
    end_time = time.time()
    
    response_time = end_time - start_time
    
    assert response.status_code == 200
    # Health check는 1초 이내에 응답해야 함
    assert response_time < 1.0, f"Health check took {response_time:.3f} seconds"
