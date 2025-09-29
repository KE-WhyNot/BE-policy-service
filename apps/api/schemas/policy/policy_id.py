from datetime import date
from pydantic import BaseModel


class PolicyDetailResponse(BaseModel):
    """정책 상세 조회 응답 스키마"""

# 기본 정보 (최상단)
    # 정책분야(카테고리) - ex) 복지문화 
    category_large: str | None = None
        # core.policy_category.category_id
        # -> master.category.id 조인
        # -> if parent_id not null, master.category에서 id = parent_id 찾고 name 가져오기
    
    # 사업 신청기간 - ex) 상시 
    status: str | None = None
        # core.policy.status 반환
        # TODO: apply_start, apply_end 값에 따라 동적으로 변경하는 로직 추가 필요 (elt)

    # 정책명
    title: str
        # core.policy.title 반환

    # 키워드 - ex) 교육지원, 맞춤형상담서비스
    keyword: list[str] = []
        # core.policy.id -> core.policy_keyword.policy_id 조인 (한 정책이 여러 키워드 가질 수 있음)
        # core.policy_keyword 에서 keyword_id -> master.keyword.id 조인

    # AI 한줄요약
    summary_ai: str | None = None 
        # TODO: LLM api 처리 (elt)


# 한 눈에 보는 정책 요약 (Body 1)
    # 정책번호
    id: str 
        # core.policy.id 반환

    # 정책분야(카테고리) - 대분류 + 중분류

    category_full: str | None = None 
        # 대분류 : category_large 재사용
        # 소분류 : core.policy_category.category_id -> master.category.id 조인 -> name 가져오기
        # category_full 은 "{대분류} - {소분류}" 형태로 반환
    
    # 정책 요약 (원본)
    summary_raw: str | None = None 
        # core.policy.summary_raw 반환

    # 지원 내용 (원본)
    description_raw: str | None = None
        # core.policy.description_raw 반환

    # 사업 운영 기간
    # TODO: 다음 항목들 구현 (core.policy) - period_type (bizPrdSecd), period_start (bizPrdBgngYmd), period_end (bizPrdEndYmd), period_etc (bizPrdEtcCn)
    period_biz: str | None = None
        # 1. period_type 확인
        # 2-1. 특정기간인 경우 -> {period_start} ~ {period_end} 반환
        # 2-2. 상시인 경우 -> "상시" 반환
        # 3. period_etc 가 있으면 -> "{위 2가지 결과} ({period_etc})" 형태로 반환
    
    # 사업 신청기간 - ex) "상시" or 날짜
    # TODO: 다음 항목 구현 - apply_type (aplyPrdSeCd)
    # TODO: ELT 로직 변경 (현재 aplyPrdSeCd, 신청기간 구분코드가 status로 들어가고 있음)
    period_apply: str | None = None
        # 1. apply_type 확인
        # 2-1. 특정기간인 경우 -> {apply_start} ~ {apply_end} 반환
        # 2-2. 상시인 경우 -> "상시" 반환
        # 2-3. 마감인 경우 -> "마감" 반환
    

# 신청자격
    # 연령
    age: str | None = None
        # 1. core.policy_eligibility.age_min 과 .age_max 확인
        # 2-1. 둘 다 있으면 -> "{age_min}세 ~ {age_max}세" 형태로 반환
        # 2-2. age_min만 있으면 -> "{age_min}세 이상" 형태
        # 2-3. age_max만 있으면 -> "{age_max}세 이하" 형태
        # 2-4. 둘 다 없으면 -> "제한없음" 반환

    # 거주지역
    regions: str | None = None
        # 1. core.policy.id -> core.policy_region.policy_id 조인 (한 정책이 여러 지역 가질 수 있음)
        # 2. core.policy_region.region_id -> master.region.id 조인
        # 3. master.region.full_name 들을 ", "로 연결한 문자열 반환

    # 소득
    income: str | None = None
        # 1. core.policy_eligibility.income_type 확인
        # 2-1. "ANY" -> "무관"
        # 2-2. "RANGE" -> core.policy_eligibility에서 "{income_min}만원 ~ {income_max}만원" 형태로 반환
        # 2-3. "TEXT" -> core.policy_eligibility.income_text 반환
        # 2-4. "UNKNOWN" -> "신청 사이트 내 확인" 반환

    # TODO: core.policy_eligibility에 다음 컬럼 추가 : 학력, 전공, 취업상태, 특화분야 제한여부 (restrict_education: true/false)
    # 학력
    education: str | None = None
        # 1. core.policy_eligibility.restrict_education 확인
        # 2-1. false : "제한없음" 반환
        # 2-2. true : core.policy.id -> core.policy_education.policy_id 조인 (한 정책이 여러 학력 가질 수 있음)
        #          core.policy_education.education_id -> master.education.id 조인
        #          master.education.name 들을 ", "로 연결한 문자열 반환

    # 전공
    major: str | None = None
        # 1. core.policy_eligibility.restrict_major 확인
        # 2-1. false : "제한없음" 반환
        # 2-2. true : core.policy.id -> core.policy_major.policy_id 조인 (한 정책이 여러 전공 가질 수 있음)
        #          core.policy_major.major_id -> master.major.id 조인
        #          master.major.name 들을 ", "로 연결한 문자열 반환

    # 취업상태
    job_status: str | None = None
        # 1. core.policy_eligibility.restrict_job_status 확인
        # 2-1. false : "제한없음" 반환
        # 2-2. true : core.policy.id -> core.policy_job_status.policy_id 조인 (한 정책이 여러 취업상태 가질 수 있음)
        #          core.policy_job_status.job_status_id -> master.job_status.id 조인
        #          master.job_status.name 들을 ", "로 연결한 문자열 반환

    # 특화분야
    specialization: str | None = None
        # 1. core.policy_eligibility.restrict_specialization 확인
        # 2-1. false : "제한없음" 반환
        # 2-2. true : core.policy.id -> core.policy_specialization.policy_id 조인 (한 정책이 여러 특화분야 가질 수 있음)
        #          core.policy_specialization.specialization_id -> master.specialization.id 조인
        #          master.specialization.name 들을 ", "로 연결한 문자열 반환

    # 추가사항
    # TODO: DB 컬럼 추가 및 elt 구현 (addAplyQlfcCndCn -> eligibility_additional)
    eligibility_additional: str | None = None 
        # core.policy_eligibility.eligibility_additional 반환
        # null 이면 "없음" 반환

    # 참여 제한 대상
    # TODO: DB 컬럼 추가 및 elt 구현 (ptcpPrpTrgtCn)
    eligibility_restrictive: str | None = None
        # core.policy_eligibility.eligibility_restrictive 반환
        # null 이면 "없음" 반환

# 신청방법
    # 신청절차
    # TODO: DB 컬럼 추가 및 elt 구현 (plcyAplyMthdCn -> application_process)
    application_process: str | None = None

    # 심사 및 발표
    # TODO: DB 컬럼 추가 및 elt 구현 (srngMthdCn -> announcement)
    announcement: str | None = None

    # 신청 사이트
    apply_url: str | None = None
        # core.policy.aplyUrl 반환

    # 제출 서류
    # TODO: DB 컬럼 추가 및 elt 구현 (sbmsnDcmntCn -> required_documents)
    required_documents: str | None = None
        # core.policy.required_documents 반환

# 기타
    # 기타 정보
    # TODO: DB 컬럼 추가 및 elt 구현 (etcMttrCn -> info_etc)
    info_etc: str | None = None
        # core.policy.info_etc 반환

    # 주관 기관
    supervising_org: str | None = None
        # core.policy.supervising_org 반환

    # 운영 기관
    operating_org: str | None = None
        # core.policy.operating_org 반환

    # 참고 사이트 1
    ref_url_1: str | None = None
        # core.policy.ref_url_1 반환

    # 참고 사이트 2
    ref_url_2: str | None = None
        # core.policy.ref_url_2 반환

# 정보 변경 내역
    # 최종 수정일
    last_external_modified: date | None = None
        # core.policy.last_external_modified 반환 (timestampz -> kst 적용)

    # 최초 등록일
    # TODO: DB 컬럼 추가 및 elt 구현 (frstRegDt -> first_external_created)
    first_external_created: date | None = None 
        # core.policy.first_external_created 반환 (timestampz -> kst 적용)
    
    
    class Config:
        from_attributes = True
        json_encoders = {
            date: lambda v: v.isoformat() if v else None
        }


class PolicyNotFoundResponse(BaseModel):
    """정책을 찾을 수 없을 때 응답 스키마"""
    message: str = "Policy not found"
    policy_id: str