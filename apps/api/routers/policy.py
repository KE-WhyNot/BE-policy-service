from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from apps.api.core.db import get_db
from apps.api.schemas.policy.policy import(
    PolicyListResponse,
    PolicyListNotFoundResponse
)

router = APIRouter(tags=["policy"])

@router.get(
    "/policy",
    responses = {
        200: {"description": "정책 리스트 조회 성공"},
        400: {"description": "잘못된 요청"},
        404: {"description": "정책을 찾을 수 없음"},
        500: {"description": "서버 오류"},
    }
)
async def get_policy_list(
# 페이지네이션
    page_num: int = Query(default=1, description="페이지 번호"),
    page_size: int = Query(default=10, description="페이지 크기 (0 입력 시 전체 출력)"),

# 검색어
    # TODO: full-text search 추후 구현
    search_word: str | None = Query(default=None, description="검색어 : ❌ full-text search 아직 미구현 ❌ "),

# 디버그용
    policy_id: str | None = Query(default=None, description="💻 디버그용 정책 ID"),

# 정책 분야
    # 카테고리(소분류) 체크박스
    # 받은 name값과 일치하는 master.category의 name으로 master.category의 id 조회 -> core.policy_category에서 category_id로 policy_id 조회
    category_small: str | None = Query(default=None, description="카테고리(소분류) : 한글 name값"),

# 퍼스널 정보
    # 지역 (checkbox list)
    # 받은 regions 리스트 내 region_id로 core.policy_region에서 region_id로 policy_id 조회
    regions: list[str] | None = Query(default=None, description="지역 : id 값 리스트"),

    # 혼인여부 (dropdown: 제한없음 / 기혼 / 미혼)
    # core.policy_eligibility에서 marital_status(ANY / MARRIED / SINGLE / UNKNOWN)로 policy_id 조회 - UNKNOWN은 ANY 취급
    marital_status: str | None = Query(default=None, description="혼인여부 : 제한없음 / 기혼 / 미혼"),

    # 연령 (textinput: numeric)
    # core.policy_eligibility에서 age_min, age_max 비교 -> core.policy_eligibility에서 policy_id 조회
    # db에서 age_min, age_max가 NULL인 경우 제한없음으로 간주
    age: int | None = Query(default=None, description="연령 : 숫자 입력"),

    # 연소득 (textinput: range min & max)
    # core.policy_eligibility에서 income_type(ANY / RANGE / TEXT / UNKNOWN)로 필터링
    # income_type이 RANGE인 경우 income_min, income_max로 와 비교 -> core.policy_eligibility에서 policy_id 조회
    # income_type이 ANY, TEXT, UNKNOWN인 경우 제한없음으로 간주
    income_min: int | None = Query(default=None, description="연소득 최소 : 숫자 입력"),
    income_max: int | None = Query(default=None, description="연소득 최대 : 숫자 입력"),

    # 학력 (multi-select chip)
    # 받은 name 값 -> master.education에서 name으로 id 조회 -> core.policy_eligibility_education에서 education_id로 policy_id 조회
    # core.policy_eligibility.restrict_education=True인 경우만 필터링, False인 경우 제한없음으로 간주
    education: list[str] | None = Query(default=None, description="학력 : 한글 name 값 리스트"),

    # 전공요건 (multi-select chip)
    # 받은 name 값 -> master.major에서 name으로 id 조회 -> core.policy_eligibility_major에서 major_id로 policy_id 조회
    # core.policy_eligibility.restrict_major=True인 경우만 필터링, False인 경우 제한없음으로 간주
    major: list[str] | None = Query(default=None, description="전공요건 : 한글 name 값 리스트"),

    # 취업상태 (multi-select chip)
    # 받은 name 값 -> master.job_status에서 name으로 id 조회 -> core.policy_eligibility_job_status에서 job_status_id로 policy_id 조회
    # core.policy_eligibility.restrict_job_status=True인 경우만 필터링, False인 경우 제한없음으로 간주
    job_status: list[str] | None = Query(default=None, description="취업상태 : 한글 name 값 리스트"),

    # 특화분야 (multi-select chip)
    # 받은 name 값 -> master.specialization에서 name으로 id 조회 -> core.policy_eligibility_specialization에서 specialization_id로 policy_id 조회
    # core.policy_eligibility_specialization 존재하는 경우만 필터링, 없는 경우 제한없음으로 간주
    specialization: list[str] | None = Query(default=None, description="특화분야 : 한글 name 값 리스트"),

# 키워드 ("검색결과에 포함된 #태그를 선택해 찾고싶은 정책을 조회해보세요.")
    # 키워드 (mutli-select chip)
    # 받은 name 값 -> master.keyword에서 name으로 id 조회 -> core.policy_keyword에서 keyword_id로 policy_id 조회
    keyword: list[str] | None = Query(default=None, description="키워드 : 한글 name 값 리스트"),

# DB session
    db: AsyncSession = Depends(get_db)
):
    # 동적 SQL 구성
    base_tables = """
    FROM core.policy p
    LEFT JOIN core.policy_category pc ON p.id = pc.policy_id
    LEFT JOIN master.category c ON pc.category_id = c.id
    LEFT JOIN master.category cl ON c.parent_id = cl.id
    LEFT JOIN core.policy_keyword pk ON p.id = pk.policy_id
    LEFT JOIN master.keyword k ON pk.keyword_id = k.id
    LEFT JOIN core.policy_region pr ON p.id = pr.policy_id
    LEFT JOIN core.policy_eligibility pe ON p.id = pe.policy_id
    LEFT JOIN core.policy_eligibility_education pee ON pe.policy_id = pee.policy_id
    LEFT JOIN master.education e ON pee.education_id = e.id
    LEFT JOIN core.policy_eligibility_major pem ON pe.policy_id = pem.policy_id
    LEFT JOIN master.major m ON pem.major_id = m.id
    LEFT JOIN core.policy_eligibility_job_status pejs ON pe.policy_id = pejs.policy_id
    LEFT JOIN master.job_status js ON pejs.job_status_id = js.id
    LEFT JOIN core.policy_eligibility_specialization pes ON pe.policy_id = pes.policy_id
    LEFT JOIN master.specialization s ON pes.specialization_id = s.id
    """
    
    joins = []
    where_conditions = []
    params = {}
    
    # 디버그용 policy_id 필터
    if policy_id:
        where_conditions.append("p.id = :policy_id")
        params["policy_id"] = policy_id
    
    # 키워드 필터
    if keyword:
        where_conditions.append("k.name = ANY(string_to_array(:keyword, ',')::text[])")
        params["keyword"] = ','.join(keyword)
    
    # 지역 필터
    if regions:
        joins.append("LEFT JOIN core.policy_region pr ON p.id = pr.policy_id")
        where_conditions.append("pr.region_id = ANY(string_to_array(:regions, ',')::int[])")
        params["regions"] = ','.join(regions)
    
    # 카테고리 필터
    if category_small:
        where_conditions.append("c.name = :category_small")
        params["category_small"] = category_small
    
    # 자격요건 관련 필터들이 있는 경우에만 policy_eligibility JOIN
    eligibility_needed = any([marital_status, age is not None, income_min is not None, income_max is not None, 
                             education, major, job_status, specialization])
    
    if eligibility_needed:
        joins.append("LEFT JOIN core.policy_eligibility pe ON p.id = pe.policy_id")
        
        # 혼인상태 필터
        if marital_status:
            if marital_status == '제한없음':
                where_conditions.append("(pe.marital_status IN ('ANY', 'UNKNOWN') OR pe.marital_status IS NULL)")
            elif marital_status == '기혼':
                where_conditions.append("pe.marital_status = 'MARRIED'")
            elif marital_status == '미혼':
                where_conditions.append("pe.marital_status = 'SINGLE'")
        
        # 연령 필터
        if age is not None:
            where_conditions.append("(pe.age_min IS NULL OR :age >= pe.age_min) AND (pe.age_max IS NULL OR :age <= pe.age_max)")
            params["age"] = age
        
        # 소득 필터
        if income_min is not None or income_max is not None:
            income_condition = "pe.income_type IN ('ANY', 'TEXT', 'UNKNOWN')"
            if income_min is not None and income_max is not None:
                income_condition += " OR (pe.income_type = 'RANGE' AND (pe.income_min IS NULL OR :income_min >= pe.income_min) AND (pe.income_max IS NULL OR :income_max <= pe.income_max))"
                params["income_min"] = income_min
                params["income_max"] = income_max
            where_conditions.append(f"({income_condition})")
        
        # 학력 필터
        if education:
            joins.append("LEFT JOIN core.policy_eligibility_education pee ON pe.policy_id = pee.policy_id")
            joins.append("LEFT JOIN master.education e ON pee.education_id = e.id")
            where_conditions.append("(pe.restrict_education = FALSE OR e.name = ANY(string_to_array(:education, ',')::text[]))")
            params["education"] = ','.join(education)
        
        # 전공 필터
        if major:
            joins.append("LEFT JOIN core.policy_eligibility_major pem ON pe.policy_id = pem.policy_id")
            joins.append("LEFT JOIN master.major m ON pem.major_id = m.id")
            where_conditions.append("(pe.restrict_major = FALSE OR m.name = ANY(string_to_array(:major, ',')::text[]))")
            params["major"] = ','.join(major)
        
        # 취업상태 필터
        if job_status:
            joins.append("LEFT JOIN core.policy_eligibility_job_status pejs ON pe.policy_id = pejs.policy_id")
            joins.append("LEFT JOIN master.job_status js ON pejs.job_status_id = js.id")
            where_conditions.append("(pe.restrict_job_status = FALSE OR js.name = ANY(string_to_array(:job_status, ',')::text[]))")
            params["job_status"] = ','.join(job_status)
        
        # 특화분야 필터
        if specialization:
            joins.append("LEFT JOIN core.policy_eligibility_specialization pes ON pe.policy_id = pes.policy_id")
            joins.append("LEFT JOIN master.specialization s ON pes.specialization_id = s.id")
            where_conditions.append("(pes.policy_id IS NOT NULL AND s.name = ANY(string_to_array(:specialization, ',')::text[]))")
            params["specialization"] = ','.join(specialization)

    # 동적 SQL 조합
    all_joins = base_tables + "\n" + "\n".join(set(joins))  # set으로 중복 제거
    where_clause = "WHERE 1=1" + ("\nAND " + "\nAND ".join(where_conditions) if where_conditions else "")

    # 전체 개수 조회용 SQL
    count_sql = f"""
    SELECT COUNT(DISTINCT p.id) as total_count
    {all_joins}
    {where_clause}
    """

    # 데이터 조회용 SQL - 응답에 포함될 모든 1:N 관계 필드에 STRING_AGG 적용
    data_sql = f"""
    SELECT DISTINCT 
        p.id,
        p.status,
        p.apply_type,
        p.apply_end,
        STRING_AGG(DISTINCT c.name, ', ') as category_small,
        (SELECT cl_parent.name 
         FROM master.category c_first 
         LEFT JOIN master.category cl_parent ON c_first.parent_id = cl_parent.id
         WHERE c_first.name = (
             SELECT TRIM(SPLIT_PART(STRING_AGG(DISTINCT c.name, ', '), ',', 1))
             FROM core.policy_category pc_sub
             LEFT JOIN master.category c_sub ON pc_sub.category_id = c_sub.id
             WHERE pc_sub.policy_id = p.id
             LIMIT 1
         )
         LIMIT 1
        ) as category_large,
        p.title,
        p.summary_raw,
        CASE 
            WHEN p.apply_type = 'ALWAYS_OPEN' THEN '상시'
            WHEN p.apply_type = 'CLOSED' THEN '마감'
            WHEN p.apply_type = 'PERIODIC' AND p.apply_start IS NOT NULL AND p.apply_end IS NOT NULL 
                THEN CONCAT(TO_CHAR(p.apply_start, 'YYYY-MM-DD'), ' ~ ', TO_CHAR(p.apply_end, 'YYYY-MM-DD'))
            WHEN p.apply_type = 'PERIODIC' AND p.apply_start IS NOT NULL AND p.apply_end IS NULL 
                THEN CONCAT(TO_CHAR(p.apply_start, 'YYYY-MM-DD'), ' ~ 별도공지')
            WHEN p.apply_type = 'PERIODIC' AND p.apply_start IS NULL AND p.apply_end IS NOT NULL 
                THEN CONCAT('별도공지 ~ ', TO_CHAR(p.apply_end, 'YYYY-MM-DD'))
            WHEN p.apply_type = 'PERIODIC' AND p.apply_start IS NULL AND p.apply_end IS NULL 
                THEN '별도공지'
            ELSE '미정'
        END as period_apply,
        STRING_AGG(DISTINCT k.name, ', ') as keyword,
        STRING_AGG(DISTINCT pr.region_id::text, ', ') as regions,
        STRING_AGG(DISTINCT e.name, ', ') as education,
        STRING_AGG(DISTINCT m.name, ', ') as major,
        STRING_AGG(DISTINCT js.name, ', ') as job_status,
        STRING_AGG(DISTINCT s.name, ', ') as specialization
    {all_joins}
    {where_clause}
    GROUP BY p.id, p.status, p.apply_type, p.apply_end, p.title, p.summary_raw, 
             p.apply_start
    ORDER BY p.id
    """

    # page_size가 0이 아니면 LIMIT/OFFSET 추가
    if page_size > 0:
        data_sql += "\nLIMIT :limit OFFSET :offset"
        params.update({
            "limit": page_size,
            "offset": (page_num - 1) * page_size
        })

    # 전체 개수 조회
    count_result = await db.execute(text(count_sql), params)
    total_count = count_result.scalar()

    # 데이터 조회
    result = await db.execute(text(data_sql), params)
    rows = result.mappings().all()

    # 정책이 존재하지 않는 경우 404 에러
    if not rows:
        raise HTTPException(
            status_code=404,
            detail={"message": "No policies found matching the criteria"}
        )
    
    policy_list = []
    for item in rows:
        # 문자열을 리스트로 변환하는 헬퍼 함수
        def str_to_list(value):
            if value:
                return [v.strip() for v in value.split(', ') if v.strip()]
            return []
        
        # status 파싱 로직
        def parse_status(status, apply_type, apply_end):
            if status == 'CLOSED':
                return "마감"
            elif status == 'OPEN':
                if apply_type == 'ALWAYS_OPEN':
                    return "상시"
                elif apply_type == 'PERIODIC' and apply_end:
                    from datetime import datetime
                    today = datetime.now().date()
                    d_day = (apply_end - today).days
                    return f"마감 D-{d_day}"
                else:
                    return "상시"  # apply_end가 없는 경우 상시로 처리
            elif status == 'UNKNOWN':
                return "UNKNOWN"
            elif status == 'UPCOMING':
                return "오픈예정"
            else:
                return status  # 기본값
        
        policy_list_response = PolicyListResponse(
            policy_id=item["id"],
            status=parse_status(item["status"], item["apply_type"], item["apply_end"]),
            category_large=item["category_large"] or "",
            title=item["title"],
            summary_raw=item["summary_raw"],
            period_apply=item["period_apply"],
            keyword=str_to_list(item["keyword"])
        )
        policy_list.append(policy_list_response)

    return {
        "result": {
            "pagging": {
                "total_count": total_count,
                "page_num": page_num,
                "page_size": page_size if page_size > 0 else total_count
            },
            "youthPolicyList": policy_list
        }
    }