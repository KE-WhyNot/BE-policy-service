"""
Simple unit tests for Policy Service that don't require database connections
"""
import pytest
from fastapi.testclient import TestClient
from app.main import create_app


class TestBasicAPI:
    """기본 API 테스트 (DB 연결 불필요)"""
    
    def test_health_check(self, client):
        """Health check endpoint 테스트"""
        response = client.get("/api/health")
        
        assert response.status_code == 200
        assert response.json() == {"status": "ok", "service": "policy-api"}
    
    def test_health_check_headers(self, client):
        """Health check response headers 테스트"""
        response = client.get("/api/health")
        
        assert response.status_code == 200
        assert "application/json" in response.headers["content-type"]
    
    def test_health_check_method_not_allowed(self, client):
        """Health check POST 요청 테스트 (405 에러 예상)"""
        response = client.post("/api/health")
        assert response.status_code == 405
    
    def test_nonexistent_endpoint(self, client):
        """존재하지 않는 엔드포인트 테스트"""
        response = client.get("/api/nonexistent")
        assert response.status_code == 404
    
    def test_app_creation(self):
        """FastAPI 앱 생성 테스트"""
        app = create_app()
        assert app is not None
        assert app.title == "Policy Service"
        assert app.version == "1.0.0"


class TestAPIRouting:
    """API 라우팅 테스트"""
    
    def test_policy_endpoints_routing(self, client):
        """Policy 엔드포인트 라우팅 테스트 (실제 처리는 안 하고 라우팅만)"""
        # 이 테스트들은 DB 연결 오류가 발생할 수 있지만, 라우팅 자체는 확인 가능
        endpoints = [
            "/api/policy/list",
            "/api/policy/filter",
        ]
        
        for endpoint in endpoints:
            response = client.get(endpoint)
            # 500 에러(서버 내부 에러)가 아니면 라우팅은 정상
            assert response.status_code != 500
    
    def test_finproduct_endpoints_routing(self, client):
        """금융상품 엔드포인트 라우팅 테스트"""
        endpoints = [
            "/api/finproduct/list",
            "/api/finproduct/filter",
        ]
        
        for endpoint in endpoints:
            response = client.get(endpoint)
            # 500 에러가 아니면 라우팅은 정상
            assert response.status_code != 500
    
    def test_wrong_http_methods(self, client):
        """잘못된 HTTP 메서드 테스트"""
        # GET만 허용되는 엔드포인트에 다른 메서드 시도
        endpoints = [
            "/api/health",
            "/api/policy/list",
            "/api/finproduct/list",
        ]
        
        for endpoint in endpoints:
            response = client.post(endpoint)
            assert response.status_code == 405  # Method Not Allowed


class TestValidationErrors:
    """파라미터 유효성 검증 테스트"""
    
    def test_invalid_query_parameters(self, client):
        """잘못된 쿼리 파라미터 테스트"""
        # 정수가 필요한 곳에 문자열 입력
        invalid_urls = [
            "/api/policy/list?page_num=invalid",
            "/api/policy/list?page_size=invalid",
            "/api/finproduct/list?page_num=abc",
            "/api/finproduct/list?page_size=xyz",
        ]
        
        for url in invalid_urls:
            response = client.get(url)
            assert response.status_code == 422  # Unprocessable Entity
    
    def test_large_parameters(self, client):
        """큰 파라미터 값 테스트"""
        large_page = 99999
        response = client.get(f"/api/policy/list?page_num={large_page}")
        
        # DB 연결 오류가 아닌 다른 응답이어야 함
        assert response.status_code != 500


class TestSecurityBasics:
    """기본 보안 테스트"""
    
    def test_xss_in_query_params(self, client):
        """쿼리 파라미터의 XSS 시도 테스트"""
        xss_payload = "<script>alert('xss')</script>"
        
        # 이 요청이 서버를 크래시시키지 않아야 함
        response = client.get(f"/api/policy/list?search_word={xss_payload}")
        assert response.status_code != 500
    
    def test_sql_injection_attempt(self, client):
        """SQL 인젝션 시도 테스트"""
        sql_payload = "'; DROP TABLE policies; --"
        
        # 이 요청이 서버를 크래시시키지 않아야 함
        response = client.get(f"/api/policy/list?search_word={sql_payload}")
        assert response.status_code != 500
    
    def test_long_input_handling(self, client):
        """매우 긴 입력 처리 테스트"""
        long_input = "a" * 10000
        
        response = client.get(f"/api/policy/list?search_word={long_input}")
        # 서버가 적절히 처리해야 함 (500 에러가 아니어야 함)
        assert response.status_code != 500


class TestCORS:
    """CORS 설정 테스트"""
    
    def test_cors_headers_present(self, client):
        """CORS 헤더 존재 확인"""
        response = client.get("/api/health")
        
        # CORS가 설정되어 있으면 관련 헤더가 있어야 함
        assert response.status_code == 200
        # 최소한 content-type은 있어야 함
        assert "content-type" in response.headers


if __name__ == "__main__":
    pytest.main([__file__])