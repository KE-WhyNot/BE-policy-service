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
    # core.product의 kor_co_nm 컬럼값을 master.bank와 조인해서 nickname으로 변경
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
    # core.product_option에서 product_id로 조인 후 가장 짧은/긴 기간 파싱해 출력 (ex: 12 ~ 60개월)
    period: str
    
    # 금액
    # DB에 컬럼 없어서 반환 불가
    # amount: str
    
    # 가입방법
    # core.product.join_way 컬럼에서 파싱 (원본값 "인터넷,스마트폰" 이런 식이라 콤마 사이에 공백만 하나씩 추가)
    subscription_method: str
    
    # 대상
    # core.product.join_member 컬럼값 그대로 반환
    target: str
    
    # 우대조건
    # core.product에서 spcl_cnd 컬럼값 그대로 반환
    special_conditions: str
    
    # 상품 안내 (단리/복리)
    # core.product에서 etc_note 컬럼값 그대로 반환
    product_guide: str


# -----------------------------
# 📊 3️⃣ 하단 2 - 금리 안내
# -----------------------------
class FinProductBottom2(BaseModel):
    # 기간별 금리 표 들어갈 데이터
    # core.product_option에서 product_id로 조인 후 옵션별 금리 정보 파싱
    # 컬럼 1 : core.product_option.save_trm (저축기간) + "개월"
    # 컬럼 2 : core.product_option.intr_rate (기본금리) + "%"
    # 컬럼 3 : core.product_option.intr_rate2 (우대금리) + "%"
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