"""
Policy Service API 실제 동작 통합 테스트
데이터베이스 연결 상태와 실제 API 동작을 검증합니다.
"""
import pytest
from fastapi.testclient import TestClient
import time
import concurrent.futures
from app.main import create_app


@pytest.fixture
def client():
    """Test client fixture"""
    app = create_app()
    return TestClient(app)


class TestAPIConnectivity:
    """전체 API 연결성 및 기본 동작 테스트"""
    
    def test_all_main_endpoints_respond(self, client):
        """주요 엔드포인트들이 모두 응답하는지 확인"""
        endpoints = [
            "/api/health",
            "/api/policy/list", 
            "/api/finproduct/list"
        ]
        
        for endpoint in endpoints:
            response = client.get(endpoint)
            # 500 에러가 발생하지 않아야 함
            assert response.status_code != 500, f"{endpoint}에서 서버 에러 발생"
            # JSON 응답이어야 함
            assert "application/json" in response.headers.get("content-type", ""), f"{endpoint}가 JSON을 반환하지 않음"
            print(f"✅ {endpoint}: {response.status_code}")
    
    def test_cors_configuration(self, client):
        """CORS 설정 확인"""
        response = client.get("/api/health")
        assert response.status_code == 200
        
        # OPTIONS 요청 테스트 (CORS preflight)
        response = client.options("/api/health")
        # OPTIONS가 허용되지 않더라도 500 에러는 아니어야 함
        assert response.status_code != 500
    
    def test_api_versioning_consistency(self, client):
        """API 버전 일관성 확인"""
        # 모든 엔드포인트가 /api/ prefix를 사용하는지 확인
        response = client.get("/health")  # prefix 없는 경우
        assert response.status_code == 404
        
        response = client.get("/api/health")  # prefix 있는 경우
        assert response.status_code == 200


class TestErrorHandlingAndValidation:
    """실제 에러 처리 및 유효성 검증 테스트"""
    
    def test_parameter_validation_consistency(self, client):
        """파라미터 유효성 검증 일관성 테스트"""
        validation_tests = [
            ("/api/policy/list?page_num=invalid", 422),
            ("/api/policy/list?page_size=invalid", 422), 
            ("/api/finproduct/list?page_num=invalid", 422),
            ("/api/finproduct/list?page_size=invalid", 422),
        ]
        
        for endpoint, expected_status in validation_tests:
            response = client.get(endpoint)
            assert response.status_code == expected_status, f"{endpoint}의 유효성 검증이 예상과 다름"
    
    def test_404_error_consistency(self, client):
        """404 에러 처리 일관성 테스트"""
        nonexistent_endpoints = [
            "/api/nonexistent",
            "/api/policy/nonexistent", 
            "/api/finproduct/nonexistent"
        ]
        
        for endpoint in nonexistent_endpoints:
            response = client.get(endpoint)
            assert response.status_code == 404, f"{endpoint}가 404를 반환하지 않음"
    
    def test_method_not_allowed_consistency(self, client):
        """HTTP 메서드 제한 일관성 테스트"""
        get_only_endpoints = [
            "/api/health",
            "/api/policy/list",
            "/api/finproduct/list"
        ]
        
        for endpoint in get_only_endpoints:
            # POST 요청은 허용되지 않아야 함
            response = client.post(endpoint)
            assert response.status_code == 405, f"{endpoint}가 POST를 허용함"


class TestPerformanceAndReliability:
    """성능 및 신뢰성 테스트"""
    
    def test_response_time_reasonable(self, client):
        """응답 시간이 합리적인지 확인"""
        endpoints = [
            "/api/health",
            "/api/policy/list?page_size=5",
            "/api/finproduct/list?page_size=5"
        ]
        
        for endpoint in endpoints:
            start_time = time.time()
            response = client.get(endpoint)
            end_time = time.time()
            
            response_time = end_time - start_time
            
            # Health check는 1초, 다른 API는 10초 이내
            max_time = 1.0 if "health" in endpoint else 10.0
            assert response_time < max_time, f"{endpoint}의 응답시간이 너무 김: {response_time:.3f}초"
            print(f"⏱️  {endpoint}: {response_time:.3f}초")
    
    def test_concurrent_request_handling(self, client):
        """동시 요청 처리 능력 테스트"""
        def make_health_request():
            return client.get("/api/health")
        
        # 5개의 동시 요청
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(make_health_request) for _ in range(5)]
            responses = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        # 모든 요청이 성공해야 함
        success_count = sum(1 for r in responses if r.status_code == 200)
        assert success_count == 5, f"동시 요청 중 {5-success_count}개 실패"
        print(f"✅ 동시 요청 5개 모두 성공")


class TestDatabaseConnectionStatus:
    """데이터베이스 연결 상태 및 데이터 확인"""
    
    def test_policy_database_status(self, client):
        """정책 데이터베이스 연결 상태 확인"""
        response = client.get("/api/policy/list?page_size=1")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ 정책 데이터베이스 연결 성공")
            if isinstance(data, dict) and "data" in data:
                policy_count = len(data.get("data", []))
                total_count = data.get("total_count", 0)
                print(f"ℹ️  정책 데이터 상태: {policy_count}개 조회됨 (전체: {total_count}개)")
        else:
            print(f"⚠️  정책 데이터베이스 상태: HTTP {response.status_code}")
        
        # 500 에러는 발생하지 않아야 함
        assert response.status_code != 500
    
    def test_finproduct_database_status(self, client):
        """금융상품 데이터베이스 연결 상태 확인"""
        response = client.get("/api/finproduct/list?page_size=1")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ 금융상품 데이터베이스 연결 성공")
            if isinstance(data, dict) and "data" in data:
                product_count = len(data.get("data", []))
                total_count = data.get("total_count", 0)
                print(f"ℹ️  금융상품 데이터 상태: {product_count}개 조회됨 (전체: {total_count}개)")
        else:
            print(f"⚠️  금융상품 데이터베이스 상태: HTTP {response.status_code}")
        
        # 500 에러는 발생하지 않아야 함
        assert response.status_code != 500


class TestSecurityBasics:
    """기본 보안 확인"""
    
    def test_no_sensitive_info_in_errors(self, client):
        """에러 응답에 민감한 정보가 노출되지 않는지 확인"""
        # 잘못된 요청으로 에러 유발
        response = client.get("/api/policy/list?page_num=invalid")
        
        if response.status_code == 422:
            error_text = response.text.lower()
            # 데이터베이스 연결 정보나 내부 경로가 노출되지 않아야 함
            sensitive_keywords = ["password", "connection", "database", "postgresql", "localhost"]
            for keyword in sensitive_keywords:
                assert keyword not in error_text, f"에러 응답에 민감한 정보 '{keyword}' 노출"
    
    def test_sql_injection_basic_protection(self, client):
        """기본적인 SQL 인젝션 보호 확인"""
        malicious_inputs = [
            "'; DROP TABLE--",
            "' OR 1=1--",
            "UNION SELECT"
        ]
        
        for payload in malicious_inputs:
            response = client.get(f"/api/policy/list?search_word={payload}")
            # 서버 에러가 발생하지 않아야 함
            assert response.status_code != 500, f"SQL 인젝션 시도로 서버 에러 발생: {payload}"