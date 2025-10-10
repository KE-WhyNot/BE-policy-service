from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.db import get_db

from app.schemas.policy.category import Category
from app.schemas.policy.education import Education
from app.schemas.policy.job_status import JobStatus
from app.schemas.policy.keyword import Keyword
from app.schemas.policy.major import Major
from app.schemas.policy.region import Region
from app.schemas.policy.specialization import Specialization
from app.schemas.policy.response import ListResponse, Meta

router = APIRouter(prefix="/filter", tags=["[청년정책] 필터 조회"])


# /api/policy/filter/category
@router.get(
        "/category/{parent_id}", 
        response_model=list[Category],
        summary="정책분야 - 카테고리 목록")
async def list_category(
    parent_id: int = Path(..., description="0이면 최상위 카테고리, 실제 ID면 해당 카테고리의 하위 카테고리"),
    include_inactive: bool = Query(False, description="true면 비활성 포함"),
    db: AsyncSession = Depends(get_db),
):
    """
    카테고리 목록 조회
    - parent_id=0: 최상위(Large) 카테고리 조회 **(예: 일자리, 주거, 교육, 복지문화, 참여권리)**
    - parent_id={실제ID}: **최상위 카테고리의 id를 넣으면,** 해당 카테고리의 하위(Medium) 카테고리 조회 **(예: 일자리 → 취업, 재직자, 창업)**
    """
    # parent_id가 0이면 최상위 카테고리 (parent_id IS NULL)
    # 그렇지 않으면 해당 parent_id의 하위 카테고리
    if parent_id == 0:
        where_condition = "parent_id IS NULL"
    else:
        where_condition = f"parent_id = {parent_id}"
    
    if not include_inactive:
        where_condition += " AND is_active = true"
    
    sql = f"""
        SELECT id, code, name, parent_id, level, is_active
        FROM master.category
        WHERE {where_condition}
        ORDER BY id
    """
    
    rows = (await db.execute(text(sql))).mappings().all()
    return [Category(**r) for r in rows]

# /api/policy/filter/region
@router.get("/region/{parent_id}",
            response_model=ListResponse[Region],
            summary="퍼스널 정보 - 지역 목록")
async def list_region(
    parent_id: int = Path(..., description="2이면 최상위 지역(시/도), 실제 ID면 해당 지역의 하위 지역(시/군/구)"),
    include_inactive: bool = Query(False, description="true면 비활성 포함"),
    db: AsyncSession = Depends(get_db),
):
    """
    지역 목록 조회
    - parent_id=0: 최상위(시/도) 지역 조회 **(서울특별시, 부산광역시, ... 제주특별자치도)**
    - parent_id=실제ID: 해당 지역의 하위(시/군/구) 지역 조회 **(강남구, 강동구, ...)**
    """
    # parent_id가 0이면 최상위 지역 (parent_id IS 2)
    # 그렇지 않으면 해당 parent_id의 하위 지역
    if parent_id == 0:
        where_condition = "parent_id IS 2"
    else:
        where_condition = f"parent_id = {parent_id}"
    
    if not include_inactive:
        where_condition += " AND is_active = true"
    
    sql = f"""
        SELECT id, code, name, parent_id, kind, zip_code, is_active
        FROM master.region
        WHERE {where_condition}
        ORDER BY id
    """
    
    rows = (await db.execute(text(sql))).mappings().all()
    
    # 순서 번호(no)를 추가하여 Region 객체 생성
    regions = []
    for idx, row in enumerate(rows, 1):
        region_data = dict(row)
        region_data['no'] = idx
        regions.append(Region(**region_data))
    
    return ListResponse[Region](
        data=regions,
        meta=Meta(count=len(regions))
    )

# /api/policy/filter/education
@router.get("/education",
            response_model=list[Education],
            summary="퍼스널 정보 - 학력 목록")
async def list_education(
    include_inactive: bool = Query(False, description="true면 비활성 포함"),
    db: AsyncSession = Depends(get_db),
):
    """
    **고졸 미만, 고교 재학, 고졸 예정, 고교 졸업, ...**

    """
    

    sql = """
        SELECT id, name, code, is_active
        FROM master.education
        {where}
        ORDER BY id
    """.format(where="" if include_inactive else "WHERE is_active = true")

    rows = (await db.execute(text(sql))).mappings().all()
    # mappings() 결과는 dict-like → Pydantic 모델로 바로 캐스팅 가능
    return [Education(**r) for r in rows]

# /api/policy/filter/major
@router.get("/major",
            response_model=list[Major],
            summary="퍼스널 정보 - 전공요건 목록")
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

# /api/policy/filter/job_status
@router.get("/job_status",
            response_model=list[JobStatus],
            summary="퍼스널 정보 - 취업상태 목록")
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

# /api/policy/filter/specialization
@router.get("/specialization",
            response_model=list[Specialization],
            summary="퍼스널 정보 - 특화분야 목록")
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

# /api/policy/filter/keyword
@router.get("/keyword",
            response_model=list[Keyword],
            summary="태그 선택 - 키워드 목록")
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
