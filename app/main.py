from fastapi import FastAPI
from app.routers import health
from app.core.cors import setup_cors

# Policy
from app.routers.policy import filter as policy_filter
from app.routers.policy import list as policy_list
from app.routers.policy import id as policy_id

# FinProduct
from app.routers.finproduct import filter as finproduct_filter
from app.routers.finproduct import list as finproduct_list
from app.schemas.finproduct import finproduct_id
from app.routers.finproduct import id as finproduct_id

def create_app() -> FastAPI:
    
    openapi_tags = [
        # Health
        {
            "name": "[HEALTH] Health Check", 
            "description": "서비스 상태 확인 API"
        },
        # Policy
        {
            "name": "[청년정책] 필터 조회", 
            "description": "정책분야, 퍼스널 정보에서 표시할 필터 목록"
        },
        {
            "name": "[청년정책] 리스트 조회", 
            "description": "필터링된 청년정책 목록 리스트 표시"
        },
        {
            "name": "[청년정책] 상세페이지 조회", 
            "description": "id path parameter로 청년정책 상세페이지 반환"
        },
        # FinProduct
        {
            "name": "[금융상품] 필터 조회", 
            "description": "은행 목록, 우대조건"
        },
        {
            "name": "[금융상품] 리스트 조회", 
            "description": "필터링된 금융상품 목록 리스트 표시"
        },
        {
            "name": "[금융상품] 상세페이지 조회", 
            "description": "id path parameter로 금융상품 상세페이지 반환"
        },
    ]
    
    app = FastAPI(
        title="Policy Service", 
        version="1.0.0",
        openapi_tags=openapi_tags
    )

    setup_cors(app)
    
    # Health
    app.include_router(health.router, prefix="/api")

    # Policy
    app.include_router(policy_filter.router, prefix="/api/policy")
    app.include_router(policy_list.router, prefix="/api/policy")
    app.include_router(policy_id.router, prefix="/api/policy")

    # FinProduct
    app.include_router(finproduct_filter.router, prefix="/api/finproduct")
    app.include_router(finproduct_list.router, prefix="/api/finproduct")
    app.include_router(finproduct_id.router, prefix="/api/finproduct")
    
    return app

app = create_app()