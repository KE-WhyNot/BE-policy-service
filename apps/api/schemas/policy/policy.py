from pydantic import BaseModel

class PolicyListResponse(BaseModel):
    
    # 상시 / 마감 / 예정
    status: str

    # 카테고리 ("일자리")
    category_large: str

    # 🚨 제외 🚨 지역 ("전국 / 서울 / 경기 ...")
    # region_large: str

    # 정책명
    title: str
    
    # 요약
    summary_raw: str

    # 신청기간
    period_apply: str

    # 키워드 ("#교육지원")
    keyword: list[str] = []

class PolicyListNotFoundResponse(BaseModel):
    message: str = "No policies found matching the criteria"