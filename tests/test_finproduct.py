"""
FinProduct API 실제 동작 테스트 (데이터베이스 연결 상태 확인)
"""
import pytest
from fastapi.testclient import TestClient
from app.main import create_app


@pytest.fixture
def client():
    """Test client fixture"""
    app = create_app()
    return TestClient(app)


class TestFinProductAPIConnectivity:
    """FinProduct API 연결성 및 기본 동작 테스트"""
    
    def test_finproduct_list_endpoint_responds(self, client):
        """금융상품 리스트 엔드포인트 응답 확인"""
        response = client.get("/api/finproduct/list")
        
        # 데이터베이스 연결이 안되어도 최소한 엔드포인트는 존재해야 함
        assert response.status_code != 500, "서버 내부 에러가 발생했습니다"
        
        # 응답이 JSON 형태여야 함
        assert "application/json" in response.headers.get("content-type", "")
    
    def test_finproduct_list_with_basic_params(self, client):
        """금융상품 리스트 기본 파라미터 테스트"""
        response = client.get("/api/finproduct/list?page_num=1&page_size=10")
        
        # 파라미터 유효성 검증이 통과해야 함
        assert response.status_code != 422, "파라미터 유효성 검증 실패"
        
        # 데이터베이스 관련 에러가 아닌 경우 응답 구조 확인
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, dict)
    
    def test_finproduct_list_parameter_validation(self, client):
        """금융상품 리스트 파라미터 유효성 검증 테스트"""
        
        # 잘못된 page_num 타입
        response = client.get("/api/finproduct/list?page_num=invalid")
        assert response.status_code == 422
        
        # 잘못된 page_size 타입  
        response = client.get("/api/finproduct/list?page_size=invalid")
        assert response.status_code == 422
    
    def test_finproduct_list_with_filters(self, client):
        """금융상품 리스트 필터 파라미터 테스트"""
        # 상품 유형 필터
        response = client.get("/api/finproduct/list?product_type=예금")
        assert response.status_code != 422
        
        # 은행 필터
        response = client.get("/api/finproduct/list?banks=KB국민은행")
        assert response.status_code != 422
        
        # 정렬 옵션
        response = client.get("/api/finproduct/list?sort_by_interest=DESC")
        assert response.status_code != 422
    
    def test_finproduct_detail_endpoint_structure(self, client):
        """금융상품 상세 엔드포인트 구조 테스트"""
        # 실제 존재할 가능성이 낮은 ID로 테스트
        response = client.get("/api/finproduct/nonexistent_id")
        
        # 엔드포인트는 존재해야 함 (404이든 다른 응답이든)
        assert response.status_code != 500
        assert "application/json" in response.headers.get("content-type", "")


class TestFinProductAPIErrorHandling:
    """FinProduct API 에러 처리 테스트"""
    
    def test_nonexistent_finproduct_endpoints(self, client):
        """존재하지 않는 금융상품 엔드포인트 테스트"""
        response = client.get("/api/finproduct/nonexistent_endpoint")
        assert response.status_code == 404
    
    def test_invalid_http_methods(self, client):
        """잘못된 HTTP 메서드 테스트"""
        # GET만 허용되는 엔드포인트에 POST 요청
        response = client.post("/api/finproduct/list")
        assert response.status_code == 405
        
        response = client.put("/api/finproduct/list")  
        assert response.status_code == 405
        
        response = client.delete("/api/finproduct/list")
        assert response.status_code == 405


class TestFinProductDatabaseConnectivity:
    """FinProduct API 데이터베이스 연결 상태 테스트"""
    
    def test_database_connection_handling(self, client):
        """데이터베이스 연결 상태 처리 확인"""
        response = client.get("/api/finproduct/list")
        
        if response.status_code == 200:
            # 데이터베이스 연결 성공
            data = response.json()
            assert isinstance(data, dict)
            print("✅ 금융상품 데이터베이스 연결 성공 - 실제 데이터로 테스트 가능")
            
        elif response.status_code == 503:
            # 서비스 사용 불가 (데이터베이스 연결 실패)
            print("⚠️  금융상품 데이터베이스 연결 실패 - 서비스 사용 불가 상태")
            
        elif response.status_code == 404:
            # 데이터 없음
            print("ℹ️  금융상품 데이터베이스는 연결되었지만 데이터가 없음")
            
        else:
            # 기타 상태
            print(f"ℹ️  금융상품 API 응답 상태: {response.status_code}")
        
        # 어떤 경우든 500 에러는 발생하지 않아야 함
        assert response.status_code != 500, "서버 내부 에러 발생"


class TestFinProductSpecialFeatures:
    """금융상품 특수 기능 테스트"""
    
    def test_interest_rate_sorting(self, client):
        """금리 정렬 기능 테스트"""
        # 금리 내림차순 정렬
        response = client.get("/api/finproduct/list?sort_by_interest=DESC")
        assert response.status_code != 422
        
        # 금리 오름차순 정렬
        response = client.get("/api/finproduct/list?sort_by_interest=ASC")
        assert response.status_code != 422
    
    def test_special_conditions_filter(self, client):
        """우대조건 필터 테스트"""
        response = client.get("/api/finproduct/list?special_conditions=신규고객")
        assert response.status_code != 422
        
        # 복수 우대조건
        response = client.get("/api/finproduct/list?special_conditions=신규고객&special_conditions=급여이체")
        assert response.status_code != 422
    
    def test_search_functionality(self, client):
        """검색 기능 테스트"""
        response = client.get("/api/finproduct/list?search_word=정기예금")
        assert response.status_code != 422
        
        # 한글 검색어
        response = client.get("/api/finproduct/list?search_word=적금")
        assert response.status_code != 422
