from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.db import get_fin_db

from app.schemas.finproduct.bank import Bank
from app.schemas.finproduct.special_condition import SpecialCondition
from app.schemas.finproduct.special_type import SpecialType

router = APIRouter(prefix="/filter", tags=["[금융상품] 필터 조회"])

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
        SELECT id, top_fin_grp_no, fin_co_no, kor_co_nm, nickname, image_url
        FROM master.bank
        WHERE {where_condition}
        ORDER BY id
    """
    
    rows = (await db.execute(text(sql))).mappings().all()
    return [Bank(**r) for r in rows]


# /api/finproduct/filter/special_condition
@router.get("/special_condition", response_model=list[SpecialCondition])
async def list_special_condition(
    type: int = Query(..., description="1:예금, 2:적금")
):
    """
    우대조건 chip 필터 목록 조회\n
    ⚠️ 예금일 때와 적금일 때 다름에 주의\n
    type: 1(예금), 2(적금)
    """
    if type == 1:
        conditions = SPECIAL_CONDITIONS_DEPOSIT
    elif type == 2:
        conditions = SPECIAL_CONDITIONS_SAVING

    rows = conditions.values()
    return [SpecialCondition(**r) for r in rows]

# /api/finproduct/filter/special_type
@router.get("/special_type", response_model=list[SpecialType])
async def list_special_type(
    type: int = Query(..., description="1:예금, 2:적금")
):
    """
    상세유형 chip 필터 목록 조회\n
    ⚠️ 예금일 때와 적금일 때 다름에 주의\n
    type: 1(예금), 2(적금)
    * 예금 : 방문없이 가입, 누구나 가입
    * 적금 : 방문없이 가입, 청년적금, 군인적금, 주택청약, 자유적금, 정기적금, 청년도약계좌

    """
    if type == 1:
        rows = SPECIAL_TYPES_DEPOSIT.values()
    elif type == 2:
        rows = SPECIAL_TYPES_SAVING.values()
    return [SpecialType(**r) for r in rows]


SPECIAL_CONDITIONS_DEPOSIT = {
    1: {"id": 1, "name": "비대면가입", "db_row_name": "is_non_face_to_face"},
    2: {"id": 2, "name": "은행앱사용", "db_row_name": "is_bank_app"},
    3: {"id": 3, "name": "급여연동", "db_row_name": "is_salary_linked"},
    4: {"id": 4, "name": "연금", "db_row_name": "is_pension_linked"},
    5: {"id": 5, "name": "공과금연동", "db_row_name": "is_utility_linked"},
    6: {"id": 6, "name": "카드사용", "db_row_name": "is_card_usage"},
    7: {"id": 7, "name": "첫거래", "db_row_name": "is_first_transaction"},
    8: {"id": 8, "name": "입출금통장", "db_row_name": "is_checking_account"},
    9: {"id": 9, "name": "재예치", "db_row_name": "is_redeposit"},
}
SPECIAL_CONDITIONS_SAVING = {
    1: {"id": 1, "name": "비대면가입", "db_row_name": "is_non_face_to_face"},
    2: {"id": 2, "name": "은행앱사용", "db_row_name": "is_bank_app"},
    3: {"id": 3, "name": "급여연동", "db_row_name": "is_salary_linked"},
    5: {"id": 5, "name": "공과금연동", "db_row_name": "is_utility_linked"},
    6: {"id": 6, "name": "카드사용", "db_row_name": "is_card_usage"},
    7: {"id": 7, "name": "첫거래", "db_row_name": "is_first_transaction"},
    8: {"id": 8, "name": "입출금통장", "db_row_name": "is_checking_account"},
    10: {"id": 10, "name": "청약보유", "db_row_name": "is_subscription_linked"},
    11: {"id": 11, "name": "추천,쿠폰", "db_row_name": "is_recommend_coupon"},
    12: {"id": 12, "name": "자동이체/달성", "db_row_name": "is_auto_transfer"},
}

SPECIAL_TYPES_DEPOSIT = {
    1: {"id": 1, "name": "방문없이가입", "db_row_name": "is_no_visit"},
    2: {"id": 2, "name": "누구나가입", "db_row_name": "is_anyone_join"},
    
}

SPECIAL_TYPES_SAVING = {
    1: {"id": 1, "name": "방문없이가입", "db_row_name": "is_no_visit"},
    2: {"id": 2, "name": "청년적금", "db_row_name": "is_youth_saving"},    
    3: {"id": 3, "name": "군인적금", "db_row_name": "is_military_saving"},
    4: {"id": 4, "name": "주택청약", "db_row_name": "is_housing_saving"},
    5: {"id": 5, "name": "자유적금", "db_row_name": "is_flexible_saving"},
    6: {"id": 6, "name": "정기적금", "db_row_name": "is_fixed_saving"},
    7: {"id": 7, "name": "청년도약계좌", "db_row_name": "is_youth_dream"},
}
