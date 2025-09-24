from datetime import date
from pydantic import BaseModel


class PolicyDetailResponse(BaseModel):
    """정책 상세 조회 응답 스키마"""

# 기본 정보 (최상단)
    # 정책분야(카테고리) - ex) 복지문화
    category_large: str | None = None
    # 사업 신청기간 - ex) 상시
    status: str | None = None
    # 정책명
    title: str
    # 키워드 - ex) 교육지원, 맞춤형상담서비스
    keyword: list[str] = []
    # AI 한줄요약 - TODO: LLM api 처리 (elt)
    summary_ai: str | None = None 

# 한 눈에 보는 정책 요약 (Body 1)
    # 정책번호
    id: str 
    # 정책분야(카테고리) - 대분류 + 중분류
    category_full: str | None = None 
    # 정책 요약 (원본)
    summary_raw: str | None = None 
    # 지원 내용 (원본)
    description_raw: str | None = None
    # 사업 운영 기간
    period_start: date | None = None # TODO: DB 컬럼 추가 및 elt 구현
    period_end: date | None = None # TODO: DB 컬럼 추가 및 elt 구현
    # 사업 신청기간 - ex) "상시" or 날짜
    apply_type: str | None = None 
    apply_start: date | None = None
    apply_end: date | None = None

# 신청자격
    # 연령
    age_min: int | None = None 
    age_max: int | None = None 
    # 소득
    income_type: str | None = None
    income_min: int | None = None
    income_max: int | None = None
    # 학력
    education: list[str] = []
    # 전공
    major: list[str] = []
    # 취업상태
    job_status: list[str] = []
    # 특화분야
    specializtation: list[str] = []
    # 추가사항
    eligibility_raw: str | None = None # TODO: DB 컬럼 추가 및 elt 구현

# 신청방법
    # 신청절차 # TODO: DB 컬럼 추가 및 elt 구현
    # 심사 및 발표 # TODO: DB 컬럼 추가 및 elt 구현
    # 신청 사이트
    apply_url: str | None = None
    # 제출 서류 # TODO: DB 컬럼 추가 및 elt 구현

# 기타
    # 기타 정보 # TODO: DB 컬럼 추가 및 elt 구현
    # 주관 기관
    supervising_org: str | None = None
    # 운영 기관
    operating_org: str | None = None
    # 참고 사이트 1
    ref_url_1: str | None = None
    # 참고 사이트 2
    ref_url_2: str | None = None

# 정보 변경 내역
    # 최종 수정일
    last_external_modified: date | None = None
    # 최초 등록일
    first_external_created: date | None = None # TODO: DB 컬럼 추가 및 elt 구현
    
    
    class Config:
        from_attributes = True
        json_encoders = {
            date: lambda v: v.isoformat() if v else None
        }


class PolicyNotFoundResponse(BaseModel):
    """정책을 찾을 수 없을 때 응답 스키마"""
    message: str = "Policy not found"
    policy_id: str