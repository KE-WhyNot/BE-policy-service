# app/routers/policy/list.py

"""
청년정책 리스트 조회 API (고성능 버전)

핵심 아이디어
1) 필터는 모두 EXISTS 기반으로 먼저 policy id를 좁히고(filtered_p), 
2) 각 1:N 관계는 policy_id 기준으로 CTE에서 사전 집계하여 붙입니다.
3) 정렬은 '마감은 항상 마지막'을 1차 키로, deadline/newest/oldest 2차 키로 정렬합니다.

정렬 파라미터(sort_by):
- deadline: 마감 임박순 (apply_end 오름차순, 상시/무기한은 뒤로, CLOSED는 항상 마지막)
- newest:   최신순 (created_at DESC, CLOSED는 항상 마지막)
- oldest:   오래된순 (created_at ASC, CLOSED는 항상 마지막)

주의:
- p.created_at 컬럼이 존재해야 newest/oldest 정렬이 의미 있습니다.
  (없다면 created_at을 다른 기준으로 교체하세요.)
"""

from __future__ import annotations

from datetime import datetime, date
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_db
from app.schemas.policy.policy import (
    PolicyListResponse,
    PolicyListNotFoundResponse,
)

router = APIRouter(tags=["[청년정책] 리스트 조회"])

DEBUG = True


@router.get(
    "/list",
    responses={
        200: {"description": "정책 리스트 조회 성공"},
        400: {"description": "잘못된 요청"},
        404: {"description": "정책을 찾을 수 없음"},
        500: {"description": "서버 오류"},
    },
)
async def get_policy_list(
    # 페이지네이션
    page_num: int = Query(default=1, ge=1, description="페이지 번호"),
    page_size: int = Query(default=10, ge=0, description="페이지 크기 (0 입력 시 전체 출력)"),

    # 검색어 (간단 ILIKE — FTS 미구현)
    search_word: Optional[str] = Query(default=None, description="검색어 : ❌ full-text search 아직 미구현 ❌ "),

    # 디버그용
    policy_id: Optional[str] = Query(default=None, description="💻 디버그용 정책 ID"),

    # 정책 분야
    category_small: Optional[List[str]] = Query(default=None, description="카테고리(소분류) 한글 name 리스트"),

    # 퍼스널 정보
    regions: Optional[List[str]] = Query(default=None, description="지역 id 리스트"),
    marital_status: Optional[str] = Query(default=None, description="혼인여부: 제한없음/기혼/미혼"),
    age: Optional[int] = Query(default=None, description="연령 숫자"),
    income_min: Optional[int] = Query(default=None, description="연소득 최소"),
    income_max: Optional[int] = Query(default=None, description="연소득 최대"),
    education: Optional[List[str]] = Query(default=None, description="학력 한글 name 리스트"),
    major: Optional[List[str]] = Query(default=None, description="전공 한글 name 리스트"),
    job_status: Optional[List[str]] = Query(default=None, description="취업상태 한글 name 리스트"),
    specialization: Optional[List[str]] = Query(default=None, description="특화분야 한글 name 리스트"),

    # 키워드
    keyword: Optional[List[str]] = Query(default=None, description="키워드 한글 name 리스트"),

    # 정렬
    sort_by: str = Query(default="deadline", pattern="^(deadline|newest|oldest)$",
                         description="정렬: **deadline(마감임박순), newest(최신순), oldest(오래된순)**"),

    # DB
    db: AsyncSession = Depends(get_db),
):
    # ------------------------------------------------------
    # 0) 파라미터 전처리
    # ------------------------------------------------------
    params: dict = {"sort_by": sort_by}

    def as_list(v: Optional[List[str]]) -> Optional[List[str]]:
        return v if (v and len(v) > 0) else None

    # 숫자 id (policy_id) 캐스팅
    if policy_id is not None:
        try:
            params["policy_id"] = int(policy_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="policy_id는 숫자여야 합니다.")

    if search_word:
        params["search_like"] = f"%{search_word}%"

    if as_list(keyword):
        params["keyword"] = keyword
    if as_list(regions):
        # regions는 정수형 리스트로 전달되는 것을 기대
        try:
            params["regions"] = [int(x) for x in regions]  # 안전 캐스팅
        except ValueError:
            raise HTTPException(status_code=400, detail="regions는 정수 리스트여야 합니다.")
    if as_list(category_small):
        params["category_small"] = category_small
    if marital_status:
        params["marital_status"] = marital_status
    if age is not None:
        params["age"] = age
    if income_min is not None:
        params["income_min"] = income_min
    if income_max is not None:
        params["income_max"] = income_max
    if as_list(education):
        params["education"] = education
    if as_list(major):
        params["major"] = major
    if as_list(job_status):
        params["job_status"] = job_status
    if as_list(specialization):
        params["specialization"] = specialization

    # 어떤 필터가 실제로 쓰였는지에 따라 EXISTS 블록을 선택적으로 추가
    where_blocks: List[str] = []

    if "policy_id" in params:
        where_blocks.append("p.id = :policy_id")

    if "search_like" in params:
        where_blocks.append("(p.title ILIKE :search_like OR p.summary_raw ILIKE :search_like)")

    if "keyword" in params:
        where_blocks.append(
            "EXISTS (SELECT 1 FROM core.policy_keyword pk "
            "JOIN master.keyword k ON k.id = pk.keyword_id "
            "WHERE pk.policy_id = p.id AND k.name = ANY(:keyword))"
        )

    if "regions" in params:
        where_blocks.append(
            "EXISTS (SELECT 1 FROM core.policy_region pr "
            "WHERE pr.policy_id = p.id AND pr.region_id = ANY(:regions))"
        )

    if "category_small" in params:
        where_blocks.append(
            "EXISTS (SELECT 1 FROM core.policy_category pc "
            "JOIN master.category c ON c.id = pc.category_id "
            "WHERE pc.policy_id = p.id AND c.name = ANY(:category_small))"
        )

    # 자격요건 — 필요한 것만 EXISTS 조합
    eligibility_blocks: List[str] = []
    if "marital_status" in params:
        eligibility_blocks.append(
            "("
            "(:marital_status = '제한없음' AND EXISTS ("
            "  SELECT 1 FROM core.policy_eligibility pe "
            "  WHERE pe.policy_id = p.id "
            "    AND (pe.marital_status IN ('ANY','UNKNOWN') OR pe.marital_status IS NULL)"
            "))"
            " OR (:marital_status = '기혼' AND EXISTS ("
            "  SELECT 1 FROM core.policy_eligibility pe "
            "  WHERE pe.policy_id = p.id AND pe.marital_status = 'MARRIED'"
            "))"
            " OR (:marital_status = '미혼' AND EXISTS ("
            "  SELECT 1 FROM core.policy_eligibility pe "
            "  WHERE pe.policy_id = p.id AND pe.marital_status = 'SINGLE'"
            "))"
            ")"
        )

    if "age" in params:
        eligibility_blocks.append(
            "EXISTS (SELECT 1 FROM core.policy_eligibility pe "
            "WHERE pe.policy_id = p.id "
            "AND (pe.age_min IS NULL OR :age >= pe.age_min) "
            "AND (pe.age_max IS NULL OR :age <= pe.age_max))"
        )

    if ("income_min" in params) or ("income_max" in params):
        # ANY/TEXT/UNKNOWN은 통과, RANGE는 min/max 비교
        eligibility_blocks.append(
            "EXISTS (SELECT 1 FROM core.policy_eligibility pe "
            "WHERE pe.policy_id = p.id AND ("
            "  pe.income_type IN ('ANY','TEXT','UNKNOWN') "
            "  OR (pe.income_type='RANGE' "
            "      AND (pe.income_min IS NULL OR :income_min >= pe.income_min) "
            "      AND (pe.income_max IS NULL OR :income_max <= pe.income_max)"
            "  )"
            "))"
        )
        # income_min/max가 없으면 NULL 전달되어도 비교식은 안전 (IS NULL 허용)

    if "education" in params:
        eligibility_blocks.append(
            "EXISTS (SELECT 1 "
            "FROM core.policy_eligibility pe "
            "JOIN core.policy_eligibility_education pee ON pee.policy_id = pe.policy_id "
            "JOIN master.education e ON e.id = pee.education_id "
            "WHERE pe.policy_id = p.id "
            "  AND pe.restrict_education = TRUE "
            "  AND e.name = ANY(:education))"
        )

    if "major" in params:
        eligibility_blocks.append(
            "EXISTS (SELECT 1 "
            "FROM core.policy_eligibility pe "
            "JOIN core.policy_eligibility_major pem ON pem.policy_id = pe.policy_id "
            "JOIN master.major m ON m.id = pem.major_id "
            "WHERE pe.policy_id = p.id "
            "  AND pe.restrict_major = TRUE "
            "  AND m.name = ANY(:major))"
        )

    if "job_status" in params:
        eligibility_blocks.append(
            "EXISTS (SELECT 1 "
            "FROM core.policy_eligibility pe "
            "JOIN core.policy_eligibility_job_status pejs ON pejs.policy_id = pe.policy_id "
            "JOIN master.job_status js ON js.id = pejs.job_status_id "
            "WHERE pe.policy_id = p.id "
            "  AND pe.restrict_job_status = TRUE "
            "  AND js.name = ANY(:job_status))"
        )

    if "specialization" in params:
        eligibility_blocks.append(
            "EXISTS (SELECT 1 "
            "FROM core.policy_eligibility_specialization pes "
            "JOIN master.specialization s ON s.id = pes.specialization_id "
            "WHERE pes.policy_id = p.id "
            "  AND s.name = ANY(:specialization))"
        )

    if eligibility_blocks:
        where_blocks.append("(" + " AND ".join(eligibility_blocks) + ")")

    where_sql = "WHERE 1=1" + ((" AND " + " AND ".join(where_blocks)) if where_blocks else "")

    # ------------------------------------------------------
    # 1) COUNT SQL
    # ------------------------------------------------------
    count_sql = f"""
    WITH filtered_p AS (
      SELECT p.id
      FROM core.policy p
      {where_sql}
    )
    SELECT COUNT(*) AS total_count
    FROM filtered_p;
    """

    if DEBUG:
        print("=== COUNT SQL ===")
        print(count_sql)
        print("PARAMS:", params)

    count_result = await db.execute(text(count_sql), params)
    total_count = count_result.scalar() or 0

    # 페이지 계산용
    limit_clause = ""
    if page_size > 0:
        params["limit"] = page_size
        params["offset"] = (page_num - 1) * page_size
        limit_clause = "\nLIMIT :limit OFFSET :offset"

    # ------------------------------------------------------
    # 2) DATA SQL (사전집계 CTE + 정렬키)
    # ------------------------------------------------------
    data_sql = f"""
    WITH filtered_p AS (
      SELECT p.id, p.status, p.apply_type, p.apply_start, p.apply_end, p.title, p.summary_raw, p.created_at
      FROM core.policy p
      {where_sql}
    ),
    cat AS (
      SELECT
        pc.policy_id,
        STRING_AGG(DISTINCT c.name, ', ') AS category_small,
        (SELECT cl2.name
         FROM master.category c2
         LEFT JOIN master.category cl2 ON cl2.id = c2.parent_id
         JOIN core.policy_category pc2 ON pc2.category_id = c2.id
         WHERE pc2.policy_id = pc.policy_id
         GROUP BY cl2.name
         ORDER BY COUNT(*) DESC, cl2.name
         LIMIT 1
        ) AS category_large
      FROM core.policy_category pc
      JOIN master.category c ON c.id = pc.category_id
      GROUP BY pc.policy_id
    ),
    kw AS (
      SELECT pk.policy_id, STRING_AGG(DISTINCT k.name, ', ') AS keyword
      FROM core.policy_keyword pk
      JOIN master.keyword k ON k.id = pk.keyword_id
      GROUP BY pk.policy_id
    ),
    rg AS (
      SELECT pr.policy_id, STRING_AGG(DISTINCT pr.region_id::text, ', ') AS regions
      FROM core.policy_region pr
      GROUP BY pr.policy_id
    ),
    edu AS (
      SELECT pee.policy_id, STRING_AGG(DISTINCT e.name, ', ') AS education
      FROM core.policy_eligibility_education pee
      JOIN master.education e ON e.id = pee.education_id
      GROUP BY pee.policy_id
    ),
    maj AS (
      SELECT pem.policy_id, STRING_AGG(DISTINCT m.name, ', ') AS major
      FROM core.policy_eligibility_major pem
      JOIN master.major m ON m.id = pem.major_id
      GROUP BY pem.policy_id
    ),
    job AS (
      SELECT pejs.policy_id, STRING_AGG(DISTINCT js.name, ', ') AS job_status
      FROM core.policy_eligibility_job_status pejs
      JOIN master.job_status js ON js.id = pejs.job_status_id
      GROUP BY pejs.policy_id
    ),
    spec AS (
      SELECT pes.policy_id, STRING_AGG(DISTINCT s.name, ', ') AS specialization
      FROM core.policy_eligibility_specialization pes
      JOIN master.specialization s ON s.id = pes.specialization_id
      GROUP BY pes.policy_id
    ),
    sort_keys AS (
      SELECT
        p.id,
        CASE WHEN p.status = 'CLOSED' THEN 1 ELSE 0 END AS closed_last,
        CASE
          WHEN p.status = 'CLOSED' THEN DATE '9999-12-31'
          WHEN p.apply_type='PERIODIC' AND p.apply_end IS NOT NULL THEN p.apply_end
          ELSE DATE '9999-12-30'
        END AS sort_deadline,
        p.created_at
      FROM filtered_p p
    )
    SELECT
      p.id,
      p.status,
      p.apply_type,
      p.apply_end,
      COALESCE(cat.category_small,'') AS category_small,
      COALESCE(cat.category_large,'') AS category_large,
      p.title,
      p.summary_raw,
      CASE
        WHEN p.apply_type='ALWAYS_OPEN' THEN '상시'
        WHEN p.apply_type='CLOSED' THEN '마감'
        WHEN p.apply_type='PERIODIC' AND p.apply_start IS NOT NULL AND p.apply_end IS NOT NULL
          THEN CONCAT(TO_CHAR(p.apply_start,'YYYY-MM-DD'),' ~ ',TO_CHAR(p.apply_end,'YYYY-MM-DD'))
        WHEN p.apply_type='PERIODIC' AND p.apply_start IS NOT NULL AND p.apply_end IS NULL
          THEN CONCAT(TO_CHAR(p.apply_start,'YYYY-MM-DD'),' ~ 별도공지')
        WHEN p.apply_type='PERIODIC' AND p.apply_start IS NULL AND p.apply_end IS NOT NULL
          THEN CONCAT('별도공지 ~ ',TO_CHAR(p.apply_end,'YYYY-MM-DD'))
        WHEN p.apply_type='PERIODIC' AND p.apply_start IS NULL AND p.apply_end IS NULL
          THEN '별도공지'
        ELSE '미정'
      END AS period_apply,
      COALESCE(kw.keyword,'') AS keyword,
      COALESCE(rg.regions,'') AS regions,
      COALESCE(edu.education,'') AS education,
      COALESCE(maj.major,'') AS major,
      COALESCE(job.job_status,'') AS job_status,
      COALESCE(spec.specialization,'') AS specialization
    FROM filtered_p p
    LEFT JOIN cat  ON cat.policy_id  = p.id
    LEFT JOIN kw   ON kw.policy_id   = p.id
    LEFT JOIN rg   ON rg.policy_id   = p.id
    LEFT JOIN edu  ON edu.policy_id  = p.id
    LEFT JOIN maj  ON maj.policy_id  = p.id
    LEFT JOIN job  ON job.policy_id  = p.id
    LEFT JOIN spec ON spec.policy_id = p.id
    JOIN sort_keys sk ON sk.id = p.id
    ORDER BY
      sk.closed_last ASC,
      CASE WHEN :sort_by = 'deadline' THEN sk.sort_deadline END ASC NULLS LAST,
      CASE WHEN :sort_by = 'newest'   THEN sk.created_at   END DESC NULLS LAST,
      CASE WHEN :sort_by = 'oldest'   THEN sk.created_at   END ASC  NULLS LAST,
      p.id
    {limit_clause}
    ;
    """

    if DEBUG:
        print("=== DATA SQL ===")
        print(data_sql)
        print("PARAMS:", params)

    result = await db.execute(text(data_sql), params)
    rows = result.mappings().all()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=PolicyListNotFoundResponse(message="No policies found matching the criteria").model_dump(),
        )

    # ------------------------------------------------------
    # 3) 응답 직렬화
    # ------------------------------------------------------
    def str_to_list(value: Optional[str]) -> List[str]:
        if value:
            return [v.strip() for v in value.split(", ") if v.strip()]
        return []

    def parse_status(status: str, apply_type: str, apply_end: Optional[date]) -> str:
        if status == "CLOSED":
            return "마감"
        if status == "OPEN":
            if apply_type == "ALWAYS_OPEN":
                return "상시"
            if apply_type == "PERIODIC" and apply_end:
                today = date.today()
                d_day = (apply_end - today).days
                # 음수면 이미 마감됐을 수 있으나, 정렬에서 CLOSED 마지막으로 이미 처리됨
                return f"마감 D-{d_day}"
            return "상시"
        if status == "UPCOMING":
            return "오픈예정"
        if status == "UNKNOWN":
            return "UNKNOWN"
        return status

    policy_list: List[PolicyListResponse] = []
    for item in rows:
        policy_list.append(
            PolicyListResponse(
                policy_id=item["id"],
                status=parse_status(item["status"], item["apply_type"], item["apply_end"]),
                category_large=item["category_large"] or "",
                title=item["title"],
                summary_raw=item["summary_raw"],
                period_apply=item["period_apply"],
                keyword=str_to_list(item["keyword"]),
            )
        )

    return {
        "result": {
            "pagging": {
                "total_count": total_count,
                "page_num": page_num,
                "page_size": page_size if page_size > 0 else total_count,
            },
            "youthPolicyList": policy_list,
        }
    }