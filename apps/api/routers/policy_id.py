from datetime import date
from fastapi import APIRouter, HTTPException, Depends, Path
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from apps.api.core.db import get_db
from apps.api.schemas.policy.policy_id import (
    PolicyDetailResponse, 
    PolicyNotFoundResponse,
    PolicyTop,
    PolicySummary,
    PolicyEligibility,
    PolicyApplication,
    PolicyEtc,
    PolicyMeta
)

router = APIRouter(tags=["policy"])


@router.get(
    "/policy/{policy_id}", 
    response_model=PolicyDetailResponse,
    responses={
        200: {"description": "정책 상세 조회 성공"},
        400: {"description": "잘못된 요청"},
        404: {"model": PolicyNotFoundResponse, "description": "정책을 찾을 수 없음"},
        500: {"description": "서버 오류"}
    }
)
async def get_policy_detail(
    policy_id: str = Path(..., description="조회할 정책의 ID"),
    db: AsyncSession = Depends(get_db),
):
    """
    정책 상세 정보 조회
    """
    # 메인 정책 정보 + 조인 필요한 데이터 모두 조회
    sql = """
        SELECT 
            p.id,
            p.ext_source,
            p.ext_id,
            p.title,
            p.summary_raw,
            p.description_raw,
            p.summary_ai,
            p.status,
            p.apply_start,
            p.apply_end,
            p.last_external_modified,
            p.views,
            p.supervising_org,
            p.operating_org,
            p.apply_url,
            p.ref_url_1,
            p.ref_url_2,
            p.created_at,
            p.updated_at,
            p.payload,
            p.content_hash,
            p.period_start,
            p.period_etc,
            p.period_end,
            p.apply_type,
            p.period_type,
            p.announcement,
            p.info_etc,
            p.first_external_created,
            p.application_process,
            p.required_documents,
            
            -- 카테고리 정보
            c_parent.name as category_large,
            c.name as category_small,
            CASE 
                WHEN c_parent.name IS NOT NULL AND c.name IS NOT NULL 
                THEN c_parent.name || ' - ' || c.name
                WHEN c_parent.name IS NOT NULL 
                THEN c_parent.name
                WHEN c.name IS NOT NULL 
                THEN c.name
                ELSE NULL
            END as category_full,
            
            -- 키워드 정보 (서브쿼리로 배열 생성)
            COALESCE(
                (SELECT array_agg(k.name ORDER BY k.name) 
                 FROM core.policy_keyword pk 
                 JOIN master.keyword k ON pk.keyword_id = k.id 
                 WHERE pk.policy_id = p.id), 
                ARRAY[]::text[]
            ) as keyword,
            
            -- 지역 정보
            (SELECT string_agg(r.full_name, ', ' ORDER BY r.full_name) 
             FROM core.policy_region pr 
             JOIN master.region r ON pr.region_id = r.id 
             WHERE pr.policy_id = p.id) as regions,
            
            -- 연령 처리
            CASE 
                WHEN pe.age_min IS NOT NULL AND pe.age_max IS NOT NULL 
                THEN pe.age_min::text || '세 ~ ' || pe.age_max::text || '세'
                WHEN pe.age_min IS NOT NULL 
                THEN pe.age_min::text || '세 이상'
                WHEN pe.age_max IS NOT NULL 
                THEN pe.age_max::text || '세 이하'
                ELSE '제한없음'
            END as age,
            
            -- 소득 처리
            CASE 
                WHEN pe.income_type = 'ANY' THEN '무관'
                WHEN pe.income_type = 'RANGE' AND pe.income_min IS NOT NULL AND pe.income_max IS NOT NULL 
                THEN pe.income_min::text || '만원 ~ ' || pe.income_max::text || '만원'
                WHEN pe.income_type = 'TEXT' THEN pe.income_text
                WHEN pe.income_type = 'UNKNOWN' THEN '신청 사이트 내 확인'
                ELSE NULL
            END as income,
            
            -- 학력 처리
            CASE 
                WHEN pe.restrict_education = false THEN '제한없음'
                ELSE COALESCE(
                    (SELECT string_agg(e.name, ', ' ORDER BY e.name) 
                     FROM core.policy_eligibility_education pee 
                     JOIN master.education e ON pee.education_id = e.id 
                     WHERE pee.policy_id = p.id), 
                    '제한없음'
                )
            END as education,
            
            -- 전공 처리
            CASE 
                WHEN pe.restrict_major = false THEN '제한없음'
                ELSE COALESCE(
                    (SELECT string_agg(m.name, ', ' ORDER BY m.name) 
                     FROM core.policy_eligibility_major pem 
                     JOIN master.major m ON pem.major_id = m.id 
                     WHERE pem.policy_id = p.id), 
                    '제한없음'
                )
            END as major,
            
            -- 취업상태 처리
            CASE 
                WHEN pe.restrict_job_status = false THEN '제한없음'
                ELSE COALESCE(
                    (SELECT string_agg(js.name, ', ' ORDER BY js.name) 
                     FROM core.policy_eligibility_job_status pejs 
                     JOIN master.job_status js ON pejs.job_status_id = js.id 
                     WHERE pejs.policy_id = p.id), 
                    '제한없음'
                )
            END as job_status,
            
            -- 특화분야 처리
            CASE 
                WHEN pe.restrict_specialization = false THEN '제한없음'
                ELSE COALESCE(
                    (SELECT string_agg(s.name, ', ' ORDER BY s.name) 
                     FROM core.policy_eligibility_specialization pes 
                     JOIN master.specialization s ON pes.specialization_id = s.id 
                     WHERE pes.policy_id = p.id), 
                    '제한없음'
                )
            END as specialization,
            
            -- 자격 추가사항 처리
            COALESCE(pe.eligibility_additional, '없음') as eligibility_additional,
            COALESCE(pe.eligibility_restrictive, '없음') as eligibility_restrictive,
            
            -- 사업 운영 기간 처리
            CASE 
                WHEN p.period_type = 'ALWAYS' OR p.period_type = '상시' THEN 
                    CASE 
                        WHEN p.period_etc IS NOT NULL THEN '상시 (' || p.period_etc || ')'
                        ELSE '상시'
                    END
                WHEN p.period_type = 'ETC' AND p.period_etc IS NOT NULL THEN p.period_etc
                WHEN p.period_start IS NOT NULL AND p.period_end IS NOT NULL THEN
                    CASE 
                        WHEN p.period_etc IS NOT NULL THEN p.period_start::text || ' ~ ' || p.period_end::text || ' (' || p.period_etc || ')'
                        ELSE p.period_start::text || ' ~ ' || p.period_end::text
                    END
                ELSE NULL
            END as period_biz,
            
            -- 사업 신청기간 처리
            CASE 
                WHEN p.apply_type = 'ALWAYS_OPEN' OR p.apply_type = '상시' THEN '상시'
                WHEN p.apply_type = 'CLOSED' OR p.apply_type = '마감' THEN '마감'
                WHEN p.apply_start IS NOT NULL AND p.apply_end IS NOT NULL THEN 
                    p.apply_start::text || ' ~ ' || p.apply_end::text
                ELSE NULL
            END as period_apply
            
        FROM core.policy p
        LEFT JOIN core.policy_category pc ON p.id = pc.policy_id
        LEFT JOIN master.category c ON pc.category_id = c.id
        LEFT JOIN master.category c_parent ON c.parent_id = c_parent.id
        LEFT JOIN core.policy_eligibility pe ON p.id = pe.policy_id
        WHERE p.id = :policy_id
    """
    
    result = await db.execute(text(sql), {"policy_id": policy_id})
    row = result.mappings().first()
    
    # 정책이 존재하지 않는 경우 404 에러
    if not row:
        raise HTTPException(
            status_code=404,
            detail={"message": "Policy not found", "policy_id": policy_id}
        )
    
    # 중첩된 구조로 데이터 매핑 (중복 제거된 버전)
    policy_response = PolicyDetailResponse(
        top=PolicyTop(
            category_large=row["category_large"],
            status=row["status"],
            title=row["title"],
            keyword=list(row["keyword"]) if row["keyword"] else [],
            summary_ai=row["summary_ai"]
        ),
        summary=PolicySummary(
            id=row["id"],
            category_full=row["category_full"],
            summary_raw=row["summary_raw"],
            description_raw=row["description_raw"],
            period_biz=row["period_biz"],
            period_apply=row["period_apply"]
            # 제거된 필드들: apply_start, apply_end, period_start, period_end, 
            # period_etc, apply_type, period_type (가공된 값으로 통합됨)
        ),
        eligibility=PolicyEligibility(
            age=row["age"],
            regions=row["regions"],
            income=row["income"],
            education=row["education"],
            major=row["major"],
            job_status=row["job_status"],
            specialization=row["specialization"],
            eligibility_additional=row["eligibility_additional"],
            eligibility_restrictive=row["eligibility_restrictive"]
        ),
        application=PolicyApplication(
            application_process=row["application_process"],
            announcement=row["announcement"],
            apply_url=row["apply_url"],
            required_documents=row["required_documents"]
        ),
        etc=PolicyEtc(
            info_etc=row["info_etc"],
            supervising_org=row["supervising_org"],
            operating_org=row["operating_org"],
            ref_url_1=row["ref_url_1"],
            ref_url_2=row["ref_url_2"]
        ),
        meta=PolicyMeta(
            ext_source=row["ext_source"],
            ext_id=row["ext_id"],
            views=row["views"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            payload=row["payload"],
            content_hash=row["content_hash"],
            last_external_modified=row["last_external_modified"],
            first_external_created=row["first_external_created"]
        )
    )
    
    return policy_response