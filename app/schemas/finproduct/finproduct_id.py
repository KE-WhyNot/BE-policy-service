from pydantic import BaseModel
from typing import Optional, List, Dict


# -----------------------------
# 🏦 1️⃣ 상단 정보 (기본 정보)
# -----------------------------
class FinProductTop(BaseModel):
    # 금융상품 ID
    finproduct_id: int
    
    # 상품명
    product_name: str
    
    # 은행명
    bank_name: str
    
    # 은행 id
    bank_id: int
    
    # 상품유형 Chip (방문없이 가입, 누구나 가입)
    product_type_chip: List[str] = []
    
    # 최고금리
    max_interest_rate: str
    
    # 기본금리
    min_interest_rate: str


# -----------------------------
# 📋 2️⃣ 하단 1 - 상품 안내
# -----------------------------
class FinProductBottom1(BaseModel):
    # 기간
    period: str
    
    # 금액
    amount: str
    
    # 가입방법
    subscription_method: str
    
    # 대상
    target: str
    
    # 우대조건
    special_conditions: str
    
    # 이자지급 (단리/복리)
    interest_payment: str


# -----------------------------
# 📊 3️⃣ 하단 2 - 금리 안내
# -----------------------------
class FinProductBottom2(BaseModel):
    # 기간별 금리 표 들어갈 데이터
    interest_rates: List[Dict] = []


# -----------------------------
# 🌟 최상위 응답 모델
# -----------------------------
class FinProductDetailResponse(BaseModel):
    """금융상품 상세 조회 응답 스키마"""
    top: FinProductTop
    bottom1: FinProductBottom1
    bottom2: FinProductBottom2
    
    class Config:
        from_attributes = True


# -----------------------------
# 🚫 예외 응답 모델
# -----------------------------
class FinProductNotFoundResponse(BaseModel):
    """금융상품을 찾을 수 없을 때 응답 스키마"""
    message: str = "FinProduct not found"
    finproduct_id: int