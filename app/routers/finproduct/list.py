# app/routers/finproduct/list.py
# ==========================================================
# [FINPRODUCT] 금융상품 리스트 조회 API
# (조건 필터링 + 금리 정렬 + 상품유형 chip 생성)
# ==========================================================

from __future__ import annotations
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.db import get_fin_db
from app.schemas.finproduct.finproduct import FinProductListResponse


router = APIRouter(tags=["[FINPRODUCT] Financial Product List and Detail"])
DEBUG = False

# ----------------------------------------------------------
# [상수 정의] '누구나 가입' 판별 키워드
# ----------------------------------------------------------
UNLIMITED_JOIN_KEYWORDS = [
    "개인",
    "없음",
    "모든고객",
    "제한없음",
    "누구나가입가능",
    "개인사업자, 조합(비영리법인 포함), 법인",
    "제한없음 (단,비거주 외국인 제외)",
    "제한없음.",
    "실명의\n개인 및 \n개인사업자",
    "실명의 개인",
    "누구나 가입가능",
    "실명의 개인\n또는 개인사업자",
    "실명의 개인 및 개인사업자",
    "실명의 \n 개인 및 \n 개인사업자",
    "제한 없음",
    "개인 및 개인사업자",
    "개인 및 법인(단,국가 지자체 및 금융기관 제외)",
    "개인(개인사업자 포함)",
    "개인,개인사업자,법인",
    "개인, 개인사업자, 임의단체",
    "내·외국인 대상",
    "-제한없음",
    "실명의 개인\n또는 개인사업자(1인 다계좌 가능)"
]

# ----------------------------------------------------------
# [엔드포인트] /api/finproduct/list
# ----------------------------------------------------------
@router.get(
    "/list",
    responses={
        200: {"description": "금융상품 리스트 조회 성공"},
        400: {"description": "잘못된 요청"},
        404: {"description": "금융상품을 찾을 수 없음"},
        500: {"description": "서버 오류"},
    },
)
async def get_finproduct_list(
    page_num: int = Query(default=1, description="페이지 번호"),
    page_size: int = Query(default=10, description="페이지 크기 (0 입력 시 전체 출력)"),

    # 디버그용
    finproduct_id: int | None = Query(default=None, description="💻 디버그용 금융상품 ID"),

    # 필터
    banks: list[int] | None = Query(default=None, description="은행 ID 리스트"),
    periods: list[int] | None = Query(default=None, description="기간 필터 (6, 12, 24개월)"),
    special_conditions: list[str] | None = Query(default=None, description="우대조건 필터 (여러개 가능)"),
    interest_rate_sort: str = Query(default="include_bonus", description="금리 정렬 (최고금리순 : include_bonus / 기본금리순 : base_only)"),

    db: AsyncSession = Depends(get_fin_db)
):
    # ----------------------------------------------------------
    # [1] 기본 FROM / JOIN 절
    # ----------------------------------------------------------
    base_tables = """
    FROM core.product p
    LEFT JOIN master.bank b ON p.kor_co_nm = b.kor_co_nm
    LEFT JOIN core.product_special_condition psc ON p.id = psc.product_id
    LEFT JOIN core.product_option po ON p.id = po.product_id
    LEFT JOIN core.product_join_way pjw ON p.id = pjw.product_id
    """

    where_conditions = []
    params = {}

    # ----------------------------------------------------------
    # [2] 필터 조건
    # ----------------------------------------------------------
    
    # 디버그용 finproduct_id 필터
    if finproduct_id:
        where_conditions.append("p.id = :finproduct_id")
        params["finproduct_id"] = finproduct_id

    if banks:
        where_conditions.append("b.id = ANY(string_to_array(:banks, ',')::int[])")
        params["banks"] = ",".join(map(str, banks))

    if periods:
        period_conditions = [f"po.save_trm = {p}" for p in periods]
        where_conditions.append("(" + " OR ".join(period_conditions) + ")")

    if special_conditions:
        # TODO: 우대조건별 매핑 dict 구성 필요
        where_conditions.append("psc.is_non_face_to_face = TRUE")

    where_clause = "WHERE 1=1 " + ("AND " + " AND ".join(where_conditions) if where_conditions else "")

    # ----------------------------------------------------------
    # [3] 정렬 조건 (금리 통합기준)
    # ----------------------------------------------------------
    if interest_rate_sort == "include_bonus":
        order_clause = """
        ORDER BY MAX(
            GREATEST(
                COALESCE(NULLIF(po.intr_rate, 0), 0),
                COALESCE(NULLIF(po.intr_rate2, 0), 0)
            )
        ) DESC
        """
    else:
        order_clause = """
        ORDER BY MIN(
            LEAST(
                NULLIF(po.intr_rate, 0),
                NULLIF(po.intr_rate2, 0)
            )
        ) DESC
        """

    # ----------------------------------------------------------
    # [4] COUNT SQL
    # ----------------------------------------------------------
    count_sql = f"""
    SELECT COUNT(DISTINCT p.id) AS total_count
    {base_tables}
    {where_clause}
    """

    # ----------------------------------------------------------
    # [5] DATA SQL (intr_rate / intr_rate2 함께 고려)
    # ----------------------------------------------------------
    data_sql = f"""
    SELECT DISTINCT
        p.id,
        b.id AS bank_id,
        p.kor_co_nm AS bank_name,
        p.fin_prdt_nm AS product_name,
        p.join_member,
        p.etc_note,

        MIN(
            LEAST(
                NULLIF(po.intr_rate, 0),
                NULLIF(po.intr_rate2, 0)
            )
        ) AS min_interest_rate,
        MAX(
            GREATEST(
                COALESCE(NULLIF(po.intr_rate, 0), 0),
                COALESCE(NULLIF(po.intr_rate2, 0), 0)
            )
        ) AS max_interest_rate,

        ARRAY_AGG(DISTINCT pjw.join_way) AS join_ways,

        psc.is_non_face_to_face,
        psc.is_bank_app,
        psc.is_salary_linked,
        psc.is_utility_linked,
        psc.is_card_usage,
        psc.is_first_transaction,
        psc.is_checking_account,
        psc.is_pension_linked,
        psc.is_redeposit,
        psc.is_subscription_linked,
        psc.is_recommend_coupon,
        psc.is_auto_transfer

    {base_tables}
    {where_clause}
    GROUP BY
        p.id, b.id, p.kor_co_nm, p.fin_prdt_nm,
        p.join_member, p.etc_note,
        psc.is_non_face_to_face, psc.is_bank_app, psc.is_salary_linked,
        psc.is_utility_linked, psc.is_card_usage, psc.is_first_transaction,
        psc.is_checking_account, psc.is_pension_linked, psc.is_redeposit,
        psc.is_subscription_linked, psc.is_recommend_coupon, psc.is_auto_transfer
    {order_clause}
    """

    if page_size > 0:
        data_sql += "\nLIMIT :limit OFFSET :offset"
        params.update({
            "limit": page_size,
            "offset": (page_num - 1) * page_size,
        })

    # ----------------------------------------------------------
    # [6] SQL 실행
    # ----------------------------------------------------------
    if DEBUG:
        print("=== COUNT SQL ===")
        print(count_sql)
        print("=== DATA SQL ===")
        print(data_sql)
        print("=== PARAMS ===")
        print(params)

    count_result = await db.execute(text(count_sql), params)
    total_count = count_result.scalar()

    result = await db.execute(text(data_sql), params)
    rows = result.mappings().all()

    if not rows:
        raise HTTPException(status_code=404, detail={"message": "No financial products found"})

    # ----------------------------------------------------------
    # [7] 후처리: 상품유형 chip 생성
    # ----------------------------------------------------------
    finproduct_list = []
    for item in rows:
        product_type_chip = []

        # ① 방문없이 가입: join_ways 중 '영업점'이 없을 경우
        join_ways = item.get("join_ways") or []
        if not any("영업점" in (way or "") for way in join_ways):
            product_type_chip.append("방문없이 가입")

        # ② 누구나 가입: join_member 문자열 검사
        join_member = (item.get("join_member") or "").replace(" ", "")
        if any(keyword in join_member for keyword in UNLIMITED_JOIN_KEYWORDS):
            product_type_chip.append("누구나 가입")

        finproduct_list.append(
            FinProductListResponse(
                finproduct_id=item["id"],
                bank_id=item["bank_id"],
                product_name=item["product_name"],
                bank_name=item["bank_name"],
                product_type_chip=product_type_chip,
                max_interest_rate=float(item["max_interest_rate"] or 0),
                min_interest_rate=float(item["min_interest_rate"] or 0),
            )
        )

    # ----------------------------------------------------------
    # [8] 응답
    # ----------------------------------------------------------
    return {
        "result": {
            "pagging": {
                "total_count": total_count,
                "page_num": page_num,
                "page_size": page_size if page_size > 0 else total_count,
            },
            "finProductList": finproduct_list,
        }
    }