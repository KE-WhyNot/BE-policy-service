# app/routers/finproduct/id.py
# ==========================================================
# [FINPRODUCT] 금융상품 상세 조회 API
# (core.product_option의 금리유형/적립유형 정보를 포함)
# ==========================================================

from fastapi import APIRouter, HTTPException, Depends, Path
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.db import get_fin_db
from app.schemas.finproduct.finproduct_id import (
    FinProductDetailResponse,
    FinProductNotFoundResponse,
    FinProductTop,
    FinProductBottom1,
    FinProductBottom2
)

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


@router.get(
    "/{finproduct_id}",
    response_model=FinProductDetailResponse,
    responses={
        200: {"description": "금융상품 상세 조회 성공"},
        400: {"description": "잘못된 요청"},
        404: {"model": FinProductNotFoundResponse, "description": "금융상품을 찾을 수 없음"},
        500: {"description": "서버 오류"},
    },
)
async def get_finproduct_detail(
    finproduct_id: int = Path(..., description="조회할 금융상품의 ID"),
    db: AsyncSession = Depends(get_fin_db),
):
    """
    금융상품 상세 정보 조회
    """

    # ----------------------------------------------------------
    # [1] 메인 금융상품 정보 (core.product + bank + join_way + special_condition + option)
    # ----------------------------------------------------------
    main_sql = """
        SELECT 
            p.id,
            p.fin_prdt_nm AS product_name,
            p.kor_co_nm AS bank_name_from_product,
            p.join_member,
            p.join_way,
            p.etc_note,
            p.spcl_cnd,

            -- 은행 정보
            b.id AS bank_id,
            b.nickname AS bank_name,

            -- 가입방법 정보
            ARRAY_AGG(DISTINCT pjw.join_way) AS join_ways,

            -- 대표 옵션정보 (core.product_option 기준)
            MAX(po.intr_rate_type_nm) AS intr_rate_type_nm,
            MAX(po.rsrv_type_nm) AS rsrv_type_nm,

            -- 우대조건 정보
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

        FROM core.product p
        LEFT JOIN master.bank b ON p.kor_co_nm = b.kor_co_nm
        LEFT JOIN core.product_join_way pjw ON p.id = pjw.product_id
        LEFT JOIN core.product_special_condition psc ON p.id = psc.product_id
        LEFT JOIN core.product_option po ON p.id = po.product_id

        WHERE p.id = :finproduct_id
        GROUP BY 
            p.id, p.fin_prdt_nm, p.kor_co_nm, p.join_member, p.join_way, 
            p.etc_note, p.spcl_cnd, b.id, b.nickname,
            psc.is_non_face_to_face, psc.is_bank_app, psc.is_salary_linked,
            psc.is_utility_linked, psc.is_card_usage, psc.is_first_transaction,
            psc.is_checking_account, psc.is_pension_linked, psc.is_redeposit,
            psc.is_subscription_linked, psc.is_recommend_coupon, psc.is_auto_transfer
    """

    # ----------------------------------------------------------
    # [2] 금리 옵션 목록 조회 (core.product_option)
    # ----------------------------------------------------------
    options_sql = """
        SELECT 
            po.save_trm,
            po.intr_rate,
            po.intr_rate2,
            po.intr_rate_type_nm,
            po.rsrv_type_nm
        FROM core.product_option po
        WHERE po.product_id = :finproduct_id
        ORDER BY po.save_trm
    """

    if DEBUG:
        print("=== MAIN SQL ===")
        print(main_sql)
        print("=== OPTIONS SQL ===")
        print(options_sql)
        print("=== PARAMS ===")
        print({"finproduct_id": finproduct_id})

    # ----------------------------------------------------------
    # [3] DB 실행
    # ----------------------------------------------------------
    main_result = await db.execute(text(main_sql), {"finproduct_id": finproduct_id})
    main_row = main_result.mappings().first()

    if not main_row:
        raise HTTPException(
            status_code=404,
            detail={"message": "FinProduct not found", "finproduct_id": finproduct_id},
        )

    options_result = await db.execute(text(options_sql), {"finproduct_id": finproduct_id})
    options_rows = options_result.mappings().all()

    # ----------------------------------------------------------
    # [4] TOP 섹션 데이터 구성
    # ----------------------------------------------------------
    product_type_chip = []

    # 방문없이 가입
    join_ways = main_row["join_ways"] or []
    if not any("영업점" in (way or "") for way in join_ways):
        product_type_chip.append("방문없이 가입")

    # 누구나 가입
    join_member = (main_row["join_member"] or "").replace(" ", "")
    if any(keyword in join_member for keyword in UNLIMITED_JOIN_KEYWORDS):
        product_type_chip.append("누구나 가입")

    # 금리 계산
    if options_rows:
        all_rates = []
        for opt in options_rows:
            if opt["intr_rate"]:
                all_rates.append(float(opt["intr_rate"]))
            if opt["intr_rate2"]:
                all_rates.append(float(opt["intr_rate2"]))
        min_rate = min(all_rates) if all_rates else 0
        max_rate = max(all_rates) if all_rates else 0
    else:
        min_rate = 0
        max_rate = 0

    top_data = FinProductTop(
        finproduct_id=main_row["id"],
        product_name=main_row["product_name"],
        bank_name=main_row["bank_name"] or main_row["bank_name_from_product"],
        bank_id=main_row["bank_id"] or 0,
        product_type_chip=product_type_chip,
        max_interest_rate=str(max_rate),
        min_interest_rate=str(min_rate),
    )

    # ----------------------------------------------------------
    # [5] BOTTOM1 섹션 (상품 안내)
    # ----------------------------------------------------------
    
    # 기간: 최소 ~ 최대 범위 형태
    if options_rows:
        periods_nums = [opt["save_trm"] for opt in options_rows if opt["save_trm"]]
        if periods_nums:
            min_period = min(periods_nums)
            max_period = max(periods_nums)
            if min_period == max_period:
                period_str = f"{min_period}개월"
            else:
                period_str = f"{min_period} ~ {max_period}개월"
        else:
            period_str = "정보 없음"
    else:
        period_str = "정보 없음"

    # 가입방법: core.product.join_way 직접 사용 (콤마 뒤 공백 추가)
    join_way_raw = main_row["join_way"] or ""
    if join_way_raw:
        # "인터넷,스마트폰" → "인터넷, 스마트폰"
        subscription_method = join_way_raw.replace(",", ", ")
    else:
        subscription_method = "정보 없음"

    # 대상: core.product.join_member 그대로
    target = main_row["join_member"] or "정보 없음"

    # 우대조건: core.product.spcl_cnd 그대로 사용 (\n → <br> 변환)
    special_conditions_raw = main_row["spcl_cnd"] or "없음"
    special_conditions_str = special_conditions_raw.replace("\n", "<br>")

    # 상품 안내: core.product.etc_note 그대로 사용 (\n → <br> 변환)
    product_guide_raw = main_row["etc_note"] or "정보 없음"
    product_guide = product_guide_raw.replace("\n", "<br>")

    bottom1_data = FinProductBottom1(
        period=period_str,
        subscription_method=subscription_method,
        target=target,
        special_conditions=special_conditions_str,
        product_guide=product_guide,
    )

    # ----------------------------------------------------------
    # [6] BOTTOM2 섹션 (금리 안내)
    # ----------------------------------------------------------
    interest_rates = []
    for opt in options_rows:
        rate_info = {
            "period": f"{opt['save_trm']}개월" if opt["save_trm"] else "정보없음",
            "base_rate": str(opt["intr_rate"]) if opt["intr_rate"] is not None else "-",
            "bonus_rate": str(opt["intr_rate2"]) if opt["intr_rate2"] is not None else "-",
        }
        interest_rates.append(rate_info)

    bottom2_data = FinProductBottom2(interest_rates=interest_rates)

    # ----------------------------------------------------------
    # [7] 최종 응답
    # ----------------------------------------------------------
    return FinProductDetailResponse(
        top=top_data, bottom1=bottom1_data, bottom2=bottom2_data
    )