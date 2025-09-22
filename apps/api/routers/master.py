from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from apps.api.core.db import get_db

from apps.api.schemas.master.education import Education
from apps.api.schemas.master.job_status import JobStatus
from apps.api.schemas.master.keyword import Keyword
from apps.api.schemas.master.major import Major
from apps.api.schemas.master.specialization import Specialization

router = APIRouter(prefix="/master", tags=["master"])

# /api/master/education
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

# /api/master/job_status
@router.get("/job_status", response_model=list[JobStatus])
async def list_job_status(
    include_inactive: bool = Query(False, description="true면 비활성 포함"),
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT id, name, code, is_active
        FROM master.job_status
        {where}
        ORDER BY id
    """.format(where="" if include_inactive else "WHERE is_active = true")

    rows = (await db.execute(text(sql))).mappings().all()
    return [JobStatus(**r) for r in rows]

# /api/master/keyword
@router.get("/keyword", response_model=list[Keyword])
async def list_keyword(
    include_inactive: bool = Query(False, description="true면 비활성 포함"),
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT id, name, is_active
        FROM master.keyword
        {where}
        ORDER BY id
    """.format(where="" if include_inactive else "WHERE is_active = true")

    rows = (await db.execute(text(sql))).mappings().all()
    return [Keyword(**r) for r in rows]

# /api/master/major
@router.get("/major", response_model=list[Major])
async def list_major(
    include_inactive: bool = Query(False, description="true면 비활성 포함"),
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT id, name, code, is_active
        FROM master.major
        {where}
        ORDER BY id
    """.format(where="" if include_inactive else "WHERE is_active = true")

    rows = (await db.execute(text(sql))).mappings().all()
    return [Major(**r) for r in rows]

# /api/master/specialization
@router.get("/specialization", response_model=list[Specialization])
async def list_specialization(
    include_inactive: bool = Query(False, description="true면 비활성 포함"),
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT id, name, code, is_active
        FROM master.specialization
        {where}
        ORDER BY id
    """.format(where="" if include_inactive else "WHERE is_active = true")

    rows = (await db.execute(text(sql))).mappings().all()
    return [Specialization(**r) for r in rows]
