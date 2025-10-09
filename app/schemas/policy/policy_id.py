from datetime import date, datetime
from pydantic import BaseModel, field_serializer
from typing import Optional, List, Dict
import pytz


# -----------------------------
# 🧩 1️⃣ 기본 정보 (최상단)
# -----------------------------
class PolicyTop(BaseModel):
    # 정책분야(카테고리) - ex) 복지문화 
    category_large: Optional[str] = None
    # core.policy_category.category_id
    # -> master.category.id 조인
    # -> if parent_id not null, master.category에서 id = parent_id 찾고 name 가져오기

    # 사업 신청기간 상태 - ex) 상시, 마감, 예정
    status: Optional[str] = None
    # core.policy.status 반환
    # ✅ apply_start, apply_end 값에 따라 update_policy_status.py 에서 동적으로 변경됨

    # 정책명
    title: str
    # core.policy.title 반환

    # 키워드 - ex) 교육지원, 맞춤형상담서비스
    keyword: List[str] = []
    # core.policy.id -> core.policy_keyword.policy_id 조인
    # -> master.keyword.id 조인 후 name 반환

    # AI 한줄요약
    summary_ai: Optional[str] = None
    # TODO: LLM API 처리 (ETL 단계 요약 생성)


# -----------------------------
# 🧾 2️⃣ 한 눈에 보는 정책 요약 (Body 1)
# -----------------------------
class PolicySummary(BaseModel):
    # 정책번호
    id: str
    # core.policy.id 반환

    # 정책분야(카테고리) - 대분류 + 중분류
    category_full: Optional[str] = None
    # "{대분류} - {소분류}" 형태로 반환

    # 정책 요약 (원본)
    summary_raw: Optional[str] = None
    # core.policy.summary_raw 반환

    # 지원 내용 (원본)
    description_raw: Optional[str] = None
    # core.policy.description_raw 반환

    # 사업 운영 기간
    period_biz: Optional[str] = None
    # 1. period_type 확인
    # 2. 특정기간 → {period_start} ~ {period_end}
    # 3. 상시 → "상시"
    # 4. period_etc 존재 시 → "{기간} ({period_etc})"

    # 사업 신청기간
    period_apply: Optional[str] = None
    # apply_type에 따라:
    # 1. 특정기간 → "{apply_start} ~ {apply_end}"
    # 2. 상시 → "상시"
    # 3. 마감 → "마감"

    # 제거된 중복 필드들 (가공된 값으로 통합됨):
    # apply_start, apply_end → period_apply로 통합
    # period_start, period_end, period_etc → period_biz로 통합
    # apply_type, period_type → 각각 period_apply, period_biz 생성 로직에 사용


# -----------------------------
# 🧍‍♀️ 3️⃣ 신청자격 (Eligibility)
# -----------------------------
class PolicyEligibility(BaseModel):
    # 연령
    age: Optional[str] = None
    # 1. age_min, age_max 확인
    # 2. "{min}세 ~ {max}세", "{min}세 이상", "{max}세 이하", 없으면 "제한없음"

    # 거주지역
    regions: Optional[str] = None
    # core.policy_region.region_id -> master.region.full_name 조인

    # 소득
    income: Optional[str] = None
    # income_type 따라 "무관" / "금액 범위" / "텍스트" / "신청 사이트 내 확인"

    # 학력
    education: Optional[str] = None
    # restrict_education=False → "제한없음"
    # restrict_education=True → 관련 master.education.name 리스트

    # 전공
    major: Optional[str] = None
    # restrict_major=False → "제한없음"
    # restrict_major=True → master.major.name 리스트

    # 취업상태
    job_status: Optional[str] = None
    # restrict_job_status=False → "제한없음"
    # restrict_job_status=True → master.job_status.name 리스트

    # 특화분야
    specialization: Optional[str] = None
    # restrict_specialization=False → "제한없음"
    # restrict_specialization=True → master.specialization.name 리스트

    # 추가사항
    eligibility_additional: Optional[str] = None
    # core.policy_eligibility.eligibility_additional (null→"없음")

    # 참여 제한 대상
    eligibility_restrictive: Optional[str] = None
    # core.policy_eligibility.eligibility_restrictive (null→"없음")


# -----------------------------
# 📝 4️⃣ 신청방법 (Application)
# -----------------------------
class PolicyApplication(BaseModel):
    # 신청절차
    application_process: Optional[str] = None
    # core.policy.application_process 반환

    # 심사 및 발표
    announcement: Optional[str] = None
    # core.policy.announcement 반환

    # 신청 사이트
    apply_url: Optional[str] = None
    # core.policy.apply_url 반환

    # 제출 서류
    required_documents: Optional[str] = None
    # core.policy.required_documents 반환


# -----------------------------
# 📎 5️⃣ 기타 (Etc)
# -----------------------------
class PolicyEtc(BaseModel):
    # 기타 정보
    info_etc: Optional[str] = None
    # core.policy.info_etc 반환

    # 주관 기관
    supervising_org: Optional[str] = None
    # core.policy.supervising_org 반환

    # 운영 기관
    operating_org: Optional[str] = None
    # core.policy.operating_org 반환

    # 참고 사이트 1
    ref_url_1: Optional[str] = None
    # core.policy.ref_url_1 반환

    # 참고 사이트 2
    ref_url_2: Optional[str] = None
    # core.policy.ref_url_2 반환


# -----------------------------
# 🧾 6️⃣ 추가 메타 정보 (Meta)
# -----------------------------
class PolicyMeta(BaseModel):
    # 외부 소스
    ext_source: Optional[str] = None
    # core.policy.ext_source 반환

    # 외부 ID
    ext_id: Optional[str] = None
    # core.policy.ext_id 반환

    # 조회수
    views: int = 0
    # core.policy.views 반환

    # 생성일시
    created_at: Optional[datetime] = None
    # core.policy.created_at 반환

    # 수정일시
    updated_at: Optional[datetime] = None
    # core.policy.updated_at 반환

    # 페이로드 (원본 JSON 데이터)
    payload: Optional[Dict] = None
    # core.policy.payload 반환

    # 컨텐츠 해시
    content_hash: Optional[str] = None
    # core.policy.content_hash 반환

    # 최종 수정일
    last_external_modified: Optional[datetime] = None
    # core.policy.last_external_modified 반환

    # 최초 등록일
    first_external_created: Optional[datetime] = None 
    # core.policy.first_external_created 반환

    @field_serializer('created_at', 'updated_at', 'last_external_modified', 'first_external_created')
    def serialize_datetime(self, dt: Optional[datetime]) -> Optional[str]:
        """datetime을 KST 시간대로 변환하고 분 단위까지만 표시"""
        if dt is None:
            return None
        
        # UTC timezone이 있는 경우 KST로 변환
        if dt.tzinfo is not None:
            kst = pytz.timezone('Asia/Seoul')
            dt_kst = dt.astimezone(kst)
        else:
            # timezone이 없는 경우 UTC로 가정하고 KST로 변환
            utc = pytz.timezone('UTC')
            dt_utc = utc.localize(dt)
            kst = pytz.timezone('Asia/Seoul')
            dt_kst = dt_utc.astimezone(kst)
        
        # 분 단위까지만 표시 (초, 마이크로초 제거)
        return dt_kst.strftime('%Y-%m-%d %H:%M')


# -----------------------------
# 🌟 최상위 응답 모델
# -----------------------------
class PolicyDetailResponse(BaseModel):
    """정책 상세 조회 응답 스키마"""
    top: PolicyTop
    summary: PolicySummary
    eligibility: PolicyEligibility
    application: PolicyApplication
    etc: PolicyEtc
    meta: PolicyMeta
    
    class Config:
        from_attributes = True


# -----------------------------
# 🚫 예외 응답 모델
# -----------------------------
class PolicyNotFoundResponse(BaseModel):
    """정책을 찾을 수 없을 때 응답 스키마"""
    message: str = "Policy not found"
    policy_id: str