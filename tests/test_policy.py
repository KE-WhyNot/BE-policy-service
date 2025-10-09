"""
Policy API 실제 동작 테스트 (데이터베이스 연결 상태 확인)
"""
import pytest
from fastapi.testclient import TestClient
from app.main import create_app


class TestPolicyAPIConnectivity:
    """Policy API 연결성 및 기본 동작 테스트"""
    
    def test_policy_list_endpoint_responds(self, client):
        """정책 리스트 엔드포인트 응답 확인"""
        response = client.get("/api/policy/list")
        
        # 데이터베이스 연결이 안되어도 최소한 엔드포인트는 존재해야 함
        # 500 에러가 아닌 다른 응답이 와야 함 (200, 404, 503 등)
        assert response.status_code != 500, "서버 내부 에러가 발생했습니다"
        
        # 응답이 JSON 형태여야 함
        assert "application/json" in response.headers.get("content-type", "")
    
    def test_policy_list_with_basic_params(self, client):
        """정책 리스트 기본 파라미터 테스트"""
        response = client.get("/api/policy/list?page_num=1&page_size=5")
        
        # 파라미터 유효성 검증이 통과해야 함
        assert response.status_code != 422, "파라미터 유효성 검증 실패"
        
        # 데이터베이스 관련 에러가 아닌 경우 응답 구조 확인
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, dict)
    
    def test_policy_list_parameter_validation(self, client):
        """정책 리스트 파라미터 유효성 검증 테스트"""
        
        # 잘못된 page_num 타입
        response = client.get("/api/policy/list?page_num=invalid")
        assert response.status_code == 422
        
        # 잘못된 page_size 타입  
        response = client.get("/api/policy/list?page_size=invalid")
        assert response.status_code == 422
    
    def test_policy_detail_endpoint_structure(self, client):
        """정책 상세 엔드포인트 구조 테스트"""
        # 실제 존재할 가능성이 낮은 ID로 테스트
        response = client.get("/api/policy/nonexistent_id")
        
        # 엔드포인트는 존재해야 함 (404이든 다른 응답이든)
        assert response.status_code != 500
        assert "application/json" in response.headers.get("content-type", "")


class TestPolicyAPIErrorHandling:
    """Policy API 에러 처리 테스트"""
    
    def test_nonexistent_policy_endpoints(self, client):
        """존재하지 않는 정책 엔드포인트 테스트"""
        response = client.get("/api/policy/nonexistent_endpoint")
        assert response.status_code == 404
    
    def test_invalid_http_methods(self, client):
        """잘못된 HTTP 메서드 테스트"""
        # GET만 허용되는 엔드포인트에 POST 요청
        response = client.post("/api/policy/list")
        assert response.status_code == 405
        
        response = client.put("/api/policy/list")  
        assert response.status_code == 405
        
        response = client.delete("/api/policy/list")
        assert response.status_code == 405


class TestPolicyDatabaseConnectivity:
    """Policy API 데이터베이스 연결 상태 테스트"""
    
    def test_database_connection_handling(self, client):
        """데이터베이스 연결 상태 처리 확인"""
        response = client.get("/api/policy/list")
        
        if response.status_code == 200:
            # 데이터베이스 연결 성공
            data = response.json()
            assert isinstance(data, dict)
            print("✅ 데이터베이스 연결 성공 - 실제 데이터로 테스트 가능")
            
        elif response.status_code == 503:
            # 서비스 사용 불가 (데이터베이스 연결 실패)
            print("⚠️  데이터베이스 연결 실패 - 서비스 사용 불가 상태")
            
        elif response.status_code == 404:
            # 데이터 없음
            print("ℹ️  데이터베이스는 연결되었지만 데이터가 없음")
            
        else:
            # 기타 상태
            print(f"ℹ️  API 응답 상태: {response.status_code}")
        
        # 어떤 경우든 500 에러는 발생하지 않아야 함
        assert response.status_code != 500, "서버 내부 에러 발생"
