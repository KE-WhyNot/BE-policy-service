from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.db import get_fin_db

from app.schemas.finproduct.bank import Bank

router = APIRouter(prefix="/filter", tags=["[FINPRODUCT] Filters"])

# /api/finproduct/filter/bank
@router.get("/bank", response_model=list[Bank])
async def list_bank(
    type: int = Query(..., description="0: 전체, 1:은행, 2:저축은행"),
    db: AsyncSession = Depends(get_fin_db),
):
    """
    은행 목록 조회
    """
    if type == 0:
        where_condition = "top_fin_grp_no IN ('0200000', '0303000')"
    elif type == 1:
        where_condition = "top_fin_grp_no = '0200000'"
    elif type == 2:
        where_condition = "top_fin_grp_no = '0303000'" 

    sql = f"""
        SELECT id, top_fin_grp_no, fin_co_no, kor_co_nm, nickname
        FROM master.bank
        WHERE {where_condition}
        ORDER BY id
    """
    
    rows = (await db.execute(text(sql))).mappings().all()
    return [Bank(**r) for r in rows]


# /api/finproduct/filter/special_condition

