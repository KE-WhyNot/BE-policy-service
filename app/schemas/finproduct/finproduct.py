from pydantic import BaseModel
from typing import Optional, List

class FinProductListResponse(BaseModel):
    # 금융상품 ID
    finproduct_id: int
    
    # 은행 ID
    bank_id: int

    # 은행 로고 URL
    image_url: str

    # 상품명
    product_name: str

    # 은행명
    bank_name: str

    # 상품유형 Chip (방문없이 가입, 누구나 가입)
    product_type_chip: List[str] = []

    # 최고금리
    max_interest_rate: Optional[float] = None

    # 기본금리
    min_interest_rate: Optional[float] = None