from datetime import date
from fastapi import APIRouter, HTTPException, Depends, Path
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from apps.api.core.db import get_db
from apps.api.schemas.policy.policy_id import PolicyDetailResponse, PolicyNotFoundResponse

router = APIRouter(tags=["policy"])


@router.get(
    "/policy/{policy_id}", 
    response_model=PolicyDetailResponse,
    responses={
        404: {"model": PolicyNotFoundResponse, "description": "Policy not found"}
    }
)
async def get_policy_detail(
    policy_id: str = Path(..., description="조회할 정책의 ID"),
    db: AsyncSession = Depends(get_db),
):
    """
    정책 상세 정보 조회
    """
    # DB에서 정책 상세 정보 조회
    sql = """
        SELECT id, title, summary_raw, description_raw, summary_ai, status,
               apply_start, apply_end, views, supervising_org, operating_org,
               ref_url_1, ref_url_2
        FROM core.policy 
        WHERE id = :policy_id
    """
    
    result = await db.execute(text(sql), {"policy_id": policy_id})
    policy_row = result.mappings().first()
    
    # 정책이 존재하지 않는 경우 404 에러
    if not policy_row:
        raise HTTPException(
            status_code=404,
            detail={"message": "Policy not found", "policy_id": policy_id}
        )
    
    await db.commit()
    
    # PolicyDetailResponse 객체로 변환하여 반환
    return PolicyDetailResponse(**policy_row)