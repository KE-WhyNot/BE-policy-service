from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from apps.api.core.db import get_db
from apps.api.schemas.master.education import Education

router = APIRouter(prefix="/master", tags=["master"])

@router.get("/education", response_model=list[Education])
async def list_education(
    include_inactive: bool = Query(False, description="true면 비활성 포함"),
    db: AsyncSession = Depends(get_db),
):

    sql = """
        SELECT id, name, code, is_active
        FROM master.education
        {where}
        ORDER BY id
    """.format(where="" if include_inactive else "WHERE is_active = true")

    rows = (await db.execute(text(sql))).mappings().all()
    # mappings() 결과는 dict-like → Pydantic 모델로 바로 캐스팅 가능
    return [Education(**r) for r in rows]