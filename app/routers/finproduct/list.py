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


router = APIRouter(tags=["[금융상품] 리스트 조회"])
DEBUG = False

# ----------------------------------------------------------
# [상수 정의] '누구나 가입' 판별 키워드
#   - join_member 문자열(개행/공백 제거 후) 안에 아래 키워드 중
#     하나라도 포함되면 "누구나 가입" chip 추가
# ----------------------------------------------------------
UNLIMITED_JOIN_KEYWORDS = [
    "개인",
    "없음",
    "모든고객",
    "제한없음",
    "누구나가입가능",
    "개인사업자,조합(비영리법인포함),법인",
    "제한없음(단,비거주외국인제외)",
    "제한없음.",
    "실명의개인및개인사업자",
    "실명의개인",
    "누구나가입가능",
    "실명의개인또는개인사업자",
    "제한없음",
    "개인및개인사업자",
    "개인및법인(단,국가지자체및금융기관제외)",
    "개인(개인사업자포함)",
    "개인,개인사업자,법인",
    "개인,개인사업자,임의단체",
    "내·외국인대상",
    "-제한없음",
    "실명의개인또는개인사업자(1인다계좌가능)",
]

# ----------------------------------------------------------
# [상수 정의] 우대조건 문자열 → 컬럼 매핑
#   - special_conditions 파라미터로 들어오는 값과 매핑
#   - OR 조건으로 결합됨 (예: 비대면가입 OR 카드사용)
# ----------------------------------------------------------
SPECIAL_CONDITION_MAP = {
    "비대면가입": "psc.is_non_face_to_face",
    "은행앱사용": "psc.is_bank_app",
    "급여연동": "psc.is_salary_linked",
    "공과금연동": "psc.is_utility_linked",
    "카드사용": "psc.is_card_usage",
    "첫거래": "psc.is_first_transaction",
    "입출금통장": "psc.is_checking_account",
    "연금": "psc.is_pension_linked",
    "재예치": "psc.is_redeposit",
    "청약보유": "psc.is_subscription_linked",
    "추천/쿠폰": "psc.is_recommend_coupon",
    "자동이체": "psc.is_auto_transfer",
}

# ----------------------------------------------------------
# [상수 정의] 상품유형 문자열 → 내부 코드 매핑
#   - 프론트/테스트에서 들어오는 다양한 표현을 허용
# ----------------------------------------------------------
PRODUCT_TYPE_ALIASES = {
    "예금": "DEPOSIT",
    "적금": "SAVING",
    "deposit": "DEPOSIT",
    "deposits": "DEPOSIT",
    "saving": "SAVING",
    "savings": "SAVING",
}
PRODUCT_TYPE_ALL_KEYWORDS = {"0", "all", "전체", "전체보기"}


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

    # 디버그용 (정확히 해당 상품만 조회)
    finproduct_id: int | None = Query(default=None, description="💻 디버그용 금융상품 ID"),

    # 필터
    banks: list[str] | None = Query(default=None, description="은행 식별자 (ID 또는 이름). 다중 선택 시 ?banks=1&banks=국민은행"),
    product_type: str | int | None = Query(default=None, description="상품 유형: 0/전체, 1/예금, 2/적금, 텍스트(예금/적금) 허용"),
    periods: int | None = Query(default=None, description="기간 필터: 해당 개월수 이상의 옵션 보유 상품만 (예: 6/12/24)"),
    special_conditions: list[str] | None = Query(
        default=None,
        description="우대조건 필터 (여러개 가능) -> 비대면가입, 은행앱사용, 급여연동, 공과금연동, 카드사용, 첫거래, 입출금통장, 연금, 재예치, 청약보유, 추천/쿠폰, 자동이체"
    ),
    interest_rate_sort: str = Query(default="include_bonus", description="정렬: include_bonus(최고금리순) / base_only(기본금리순)"),

    db: AsyncSession = Depends(get_fin_db),
):
    # ----------------------------------------------------------
    # [1] 기본 FROM / JOIN 절
    #   - bank_name은 master.bank.nickname 사용
    # ----------------------------------------------------------
    base_tables = """
    FROM core.product p
    LEFT JOIN master.bank b ON p.kor_co_nm = b.kor_co_nm
    LEFT JOIN core.product_special_condition psc ON p.id = psc.product_id
    LEFT JOIN core.product_option po ON p.id = po.product_id
    LEFT JOIN core.product_join_way pjw ON p.id = pjw.product_id
    """

    where_conditions: list[str] = []
    params: dict[str, object] = {}

    # ----------------------------------------------------------
    # [2] 필터 조건
    # ----------------------------------------------------------
    # 2-1) finproduct_id
    if finproduct_id is not None:
        where_conditions.append("p.id = :finproduct_id")
        params["finproduct_id"] = finproduct_id

    # 2-2) banks (PostgreSQL ANY 배열 바인딩)
    if banks:
        bank_ids: list[int] = []
        bank_names: list[str] = []
        for raw_bank in banks:
            value = (raw_bank or "").strip()
            if not value:
                continue
            if value.isdigit():
                bank_ids.append(int(value))
            else:
                bank_names.append(value)

        if bank_ids and bank_names:
            where_conditions.append("(b.id = ANY(:bank_ids) OR b.nickname = ANY(:bank_names))")
            params["bank_ids"] = bank_ids
            params["bank_names"] = bank_names
        elif bank_ids:
            where_conditions.append("b.id = ANY(:bank_ids)")
            params["bank_ids"] = bank_ids
        elif bank_names:
            where_conditions.append("b.nickname = ANY(:bank_names)")
            params["bank_names"] = bank_names

    # 2-3) product_type (0: 전체, 1: 예금만, 2: 적금만)
    product_type_filter = None
    if product_type is not None:
        if isinstance(product_type, int):
            product_type_filter = {1: "DEPOSIT", 2: "SAVING"}.get(product_type)
        else:
            raw_type = str(product_type).strip()
            if raw_type:
                if raw_type.isdigit():
                    product_type_filter = {1: "DEPOSIT", 2: "SAVING"}.get(int(raw_type))
                elif raw_type in PRODUCT_TYPE_ALL_KEYWORDS or raw_type.lower() in PRODUCT_TYPE_ALL_KEYWORDS:
                    product_type_filter = None
                else:
                    alias = PRODUCT_TYPE_ALIASES.get(raw_type) or PRODUCT_TYPE_ALIASES.get(raw_type.lower())
                    product_type_filter = alias

    if product_type_filter == "DEPOSIT":
        where_conditions.append("p.product_type = 'DEPOSIT'")
    elif product_type_filter == "SAVING":
        where_conditions.append("p.product_type = 'SAVING'")
    # product_type == 전체 또는 None인 경우 조건 추가 안함 (전체 조회)

    # 2-4) periods (해당 개월수 이상인 옵션이 하나라도 있는 상품)
    if periods is not None:
        where_conditions.append("po.save_trm >= :periods")
        params["periods"] = periods

    # 2-5) special_conditions (매핑되는 항목만 AND로 결합)
    if special_conditions is not None and len(special_conditions) > 0:
        matched_cols = [SPECIAL_CONDITION_MAP[s] for s in special_conditions if s in SPECIAL_CONDITION_MAP]
        if matched_cols:
            where_conditions.append("(" + " AND ".join(f"{c} = TRUE" for c in matched_cols) + ")")
        # 매칭되는 항목이 하나도 없으면 조건을 추가하지 않음
        # (원한다면 400으로 에러 처리하도록 변경 가능)

    where_clause = "WHERE 1=1 " + ("AND " + " AND ".join(where_conditions) if where_conditions else "")

    # ----------------------------------------------------------
    # [3] 공통 금리 계산식 (0과 NULL은 제외하고 intr_rate/intr_rate2 통합)
    #   - min_interest_rate: 두 컬럼의 유효값 중 최소 → 그 중 전체 MIN
    #   - max_interest_rate: 두 컬럼의 유효값 중 최대 → 그 중 전체 MAX
    # ----------------------------------------------------------
    min_rate_sql = """
        MIN(
            CASE
                WHEN NULLIF(po.intr_rate, 0) IS NULL AND NULLIF(po.intr_rate2, 0) IS NULL THEN NULL
                WHEN NULLIF(po.intr_rate, 0) IS NULL THEN NULLIF(po.intr_rate2, 0)
                WHEN NULLIF(po.intr_rate2, 0) IS NULL THEN NULLIF(po.intr_rate, 0)
                ELSE LEAST(NULLIF(po.intr_rate, 0), NULLIF(po.intr_rate2, 0))
            END
        )
    """
    max_rate_sql = """
        MAX(
            CASE
                WHEN NULLIF(po.intr_rate, 0) IS NULL AND NULLIF(po.intr_rate2, 0) IS NULL THEN NULL
                WHEN NULLIF(po.intr_rate, 0) IS NULL THEN NULLIF(po.intr_rate2, 0)
                WHEN NULLIF(po.intr_rate2, 0) IS NULL THEN NULLIF(po.intr_rate, 0)
                ELSE GREATEST(NULLIF(po.intr_rate, 0), NULLIF(po.intr_rate2, 0))
            END
        )
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
    # [5] DATA SQL
    #   - bank_name: b.nickname (마스터 테이블 별칭)
    #   - join_ways: DISTINCT + NULL 제외
    # ----------------------------------------------------------
    data_sql = f"""
    SELECT DISTINCT
        p.id,
        b.id AS bank_id,
        b.nickname AS bank_name,
        p.fin_prdt_nm AS product_name,
        p.join_member,
        p.etc_note,

        {min_rate_sql} AS min_interest_rate,
        {max_rate_sql} AS max_interest_rate,

        ARRAY_AGG(DISTINCT pjw.join_way) FILTER (WHERE pjw.join_way IS NOT NULL) AS join_ways,

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
        p.id, b.id, b.nickname, p.fin_prdt_nm,
        p.join_member, p.etc_note,
        psc.is_non_face_to_face, psc.is_bank_app, psc.is_salary_linked,
        psc.is_utility_linked, psc.is_card_usage, psc.is_first_transaction,
        psc.is_checking_account, psc.is_pension_linked, psc.is_redeposit,
        psc.is_subscription_linked, psc.is_recommend_coupon, psc.is_auto_transfer
    """

    # ----------------------------------------------------------
    # [6] 정렬 (SELECT 별칭 활용)
    # ----------------------------------------------------------
    if interest_rate_sort == "include_bonus":  # 최고금리순
        data_sql += "\nORDER BY max_interest_rate DESC NULLS LAST"
    else:  # base_only: 기본금리순
        data_sql += "\nORDER BY min_interest_rate DESC NULLS LAST"

    if page_size > 0:
        data_sql += "\nLIMIT :limit OFFSET :offset"
        params["limit"] = page_size
        params["offset"] = (page_num - 1) * page_size

    # ----------------------------------------------------------
    # [7] SQL 실행
    # ----------------------------------------------------------
    if DEBUG:
        print("=== COUNT SQL ==="); print(count_sql)
        print("=== DATA SQL ==="); print(data_sql)
        print("=== PARAMS ==="); print(params)

    count_result = await db.execute(text(count_sql), params)
    total_count = count_result.scalar()

    result = await db.execute(text(data_sql), params)
    rows = result.mappings().all()

    if not rows:
        raise HTTPException(status_code=404, detail={"message": "No financial products found"})

    # ----------------------------------------------------------
    # [8] 후처리: 상품유형 chip 생성
    #   - '방문없이 가입': join_ways에 '영업점'이 없으면 추가
    #   - '누구나 가입'  : join_member에 키워드 존재하면 추가
    # ----------------------------------------------------------
    finproduct_list = []
    for item in rows:
        product_type_chip: list[str] = []

        # ① 방문없이 가입
        join_ways = item.get("join_ways") or []
        if not any("영업점" in (way or "") for way in join_ways):
            product_type_chip.append("방문없이 가입")

        # ② 누구나 가입
        join_member_raw = (item.get("join_member") or "")
        # 공백/개행 제거 후 키워드 검사
        join_member_norm = join_member_raw.replace(" ", "").replace("\n", "")
        if any(keyword in join_member_norm for keyword in UNLIMITED_JOIN_KEYWORDS):
            product_type_chip.append("누구나 가입")

        finproduct_list.append(
            FinProductListResponse(
                finproduct_id=item["id"],
                bank_id=item["bank_id"],
                product_name=item["product_name"],
                bank_name=item["bank_name"],  # master.bank.nickname
                product_type_chip=product_type_chip,
                max_interest_rate=float(item["max_interest_rate"] or 0),
                min_interest_rate=float(item["min_interest_rate"] or 0),
            )
        )

    # ----------------------------------------------------------
    # [9] 응답
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
