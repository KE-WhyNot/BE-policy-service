# stg_to_core.py
from __future__ import annotations
"""
STG(통합) -> CORE 업서트 + 옵션 세트 해시 계산 + 우대조건 분석

전제:
  - STG(base):   stg.finproduct_base_landing
  - STG(option): stg.finproduct_option_landing
  - product_type: 'DEPOSIT' | 'SAVING'
  - is_current로 현재본 1건 보장 (SCD2 스타일)

환경:
  PG_DSN_FIN = postgresql+psycopg://user:pass@host:5432/db
실행:
  python stg_to_core.py [all|deposit|saving] [--skip-gemini]
"""

import os, sys, logging, json
from typing import Tuple, List, Optional
from dataclasses import dataclass

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Gemini 관련 import (선택적)
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    genai = None

load_dotenv()
log = logging.getLogger("stg_to_core")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s :: %(message)s")

def env_str(name: str, default: str | None=None) -> str:
    v = os.getenv(name, default)
    if v in (None, ""):
        raise RuntimeError(f"Missing env: {name}")
    return v

PG_DSN_FIN = env_str("PG_DSN_FIN")

# Gemini 설정 (선택적)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if GEMINI_AVAILABLE and GEMINI_API_KEY and GEMINI_API_KEY != "your_gemini_api_key_here":
    genai.configure(api_key=GEMINI_API_KEY)

PRODUCT_TYPES = {
    "deposit": "DEPOSIT",
    "saving":  "SAVING",
}

@dataclass
class SpecialCondition:
    is_non_face_to_face: bool = False
    is_bank_app: bool = False
    is_salary_linked: bool = False
    is_pension_linked: bool = False
    is_utility_linked: bool = False
    is_card_usage: bool = False
    is_first_transaction: bool = False
    is_checking_account: bool = False
    is_redeposit: bool = False

# ------------------------
# BOOTSTRAP (멱등)
# ------------------------
BOOTSTRAP_SQL = """
CREATE SCHEMA IF NOT EXISTS core;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS core.product (
  id              BIGSERIAL PRIMARY KEY,
  product_type    TEXT NOT NULL CHECK (product_type IN ('DEPOSIT','SAVING')),
  ext_source      TEXT NOT NULL,
  ext_id          TEXT NOT NULL,            -- fin_prdt_cd
  payload         JSONB NOT NULL,
  content_hash    TEXT  NOT NULL,

  -- 기본 생성 컬럼
  dcls_month      TEXT GENERATED ALWAYS AS ((payload->>'dcls_month')) STORED,
  fin_prdt_nm     TEXT GENERATED ALWAYS AS ((payload->>'fin_prdt_nm')) STORED,
  fin_co_no       TEXT GENERATED ALWAYS AS ((payload->>'fin_co_no')) STORED,
  kor_co_nm       TEXT GENERATED ALWAYS AS ((payload->>'kor_co_nm')) STORED,

  -- SCD2
  is_current      BOOLEAN     NOT NULL DEFAULT TRUE,
  valid_from_ts   TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_to_ts     TIMESTAMPTZ NULL,

  -- 옵션 세트
  options_set_hash TEXT NULL,
  options_count    INT  NULL,

  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 멱등: 부족한 생성 컬럼 보강
ALTER TABLE core.product
  ADD COLUMN IF NOT EXISTS fin_prdt_cd     TEXT    GENERATED ALWAYS AS ((payload->>'fin_prdt_cd')) STORED,
  ADD COLUMN IF NOT EXISTS join_way        TEXT    GENERATED ALWAYS AS ((payload->>'join_way')) STORED,
  ADD COLUMN IF NOT EXISTS mtrt_int        TEXT    GENERATED ALWAYS AS ((payload->>'mtrt_int')) STORED,
  ADD COLUMN IF NOT EXISTS spcl_cnd        TEXT    GENERATED ALWAYS AS ((payload->>'spcl_cnd')) STORED,
  ADD COLUMN IF NOT EXISTS join_deny       TEXT    GENERATED ALWAYS AS ((payload->>'join_deny')) STORED,
  ADD COLUMN IF NOT EXISTS join_member     TEXT    GENERATED ALWAYS AS ((payload->>'join_member')) STORED,
  ADD COLUMN IF NOT EXISTS etc_note        TEXT    GENERATED ALWAYS AS ((payload->>'etc_note')) STORED,
  ADD COLUMN IF NOT EXISTS max_limit       NUMERIC GENERATED ALWAYS AS (NULLIF(payload->>'max_limit','')::numeric) STORED,
  ADD COLUMN IF NOT EXISTS dcls_strt_day   TEXT    GENERATED ALWAYS AS ((payload->>'dcls_strt_day')) STORED,
  ADD COLUMN IF NOT EXISTS dcls_end_day    TEXT    GENERATED ALWAYS AS ((payload->>'dcls_end_day')) STORED,
  ADD COLUMN IF NOT EXISTS fin_co_subm_day TEXT    GENERATED ALWAYS AS ((payload->>'fin_co_subm_day')) STORED;

-- 유니크/조회 인덱스
CREATE UNIQUE INDEX IF NOT EXISTS uq_product_current
  ON core.product(ext_source, ext_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS idx_product_lookup
  ON core.product(ext_source, ext_id, is_current);
CREATE INDEX IF NOT EXISTS idx_product_dcls_month
  ON core.product(dcls_month) WHERE is_current;
CREATE INDEX IF NOT EXISTS idx_product_fin_prdt_cd
  ON core.product(fin_prdt_cd) WHERE is_current;
CREATE INDEX IF NOT EXISTS idx_product_fin_co_no
  ON core.product(fin_co_no) WHERE is_current;
CREATE INDEX IF NOT EXISTS idx_product_kor_co_nm
  ON core.product(kor_co_nm) WHERE is_current;
CREATE INDEX IF NOT EXISTS idx_product_fin_prdt_nm
  ON core.product(fin_prdt_nm) WHERE is_current;

-- option 테이블(동일)
CREATE TABLE IF NOT EXISTS core.product_option (
  id                BIGSERIAL PRIMARY KEY,
  product_id        BIGINT NOT NULL REFERENCES core.product(id) ON DELETE CASCADE,
  payload           JSONB  NOT NULL,
  content_hash      TEXT   NOT NULL,

  dcls_month        TEXT    GENERATED ALWAYS AS ((payload->>'dcls_month')) STORED,
  save_trm          INTEGER GENERATED ALWAYS AS (NULLIF(payload->>'save_trm','')::int) STORED,
  intr_rate_type    TEXT    GENERATED ALWAYS AS ((payload->>'intr_rate_type')) STORED,
  intr_rate_type_nm TEXT    GENERATED ALWAYS AS ((payload->>'intr_rate_type_nm')) STORED,
  intr_rate         NUMERIC GENERATED ALWAYS AS (NULLIF(payload->>'intr_rate','')::numeric) STORED,
  intr_rate2        NUMERIC GENERATED ALWAYS AS (NULLIF(payload->>'intr_rate2','')::numeric) STORED,
  rsrv_type         TEXT    GENERATED ALWAYS AS ((payload->>'rsrv_type')) STORED,
  rsrv_type_nm      TEXT    GENERATED ALWAYS AS ((payload->>'rsrv_type_nm')) STORED,

  is_current        BOOLEAN     NOT NULL DEFAULT TRUE,
  valid_from_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_to_ts       TIMESTAMPTZ NULL,

  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_option_current
  ON core.product_option(product_id,
                          COALESCE(save_trm,-1),
                          COALESCE(intr_rate_type,''),
                          COALESCE(rsrv_type,''))
  WHERE is_current;

CREATE INDEX IF NOT EXISTS idx_option_product
  ON core.product_option(product_id, is_current);

-- 우대조건 테이블
CREATE TABLE IF NOT EXISTS core.product_special_condition (
    id                   SERIAL PRIMARY KEY,
    product_id           BIGINT NOT NULL REFERENCES core.product(id) ON DELETE CASCADE,
    is_non_face_to_face  BOOLEAN DEFAULT FALSE,
    is_bank_app          BOOLEAN DEFAULT FALSE,
    is_salary_linked     BOOLEAN DEFAULT FALSE,
    is_pension_linked    BOOLEAN DEFAULT FALSE,
    is_utility_linked    BOOLEAN DEFAULT FALSE,
    is_card_usage        BOOLEAN DEFAULT FALSE,
    is_first_transaction BOOLEAN DEFAULT FALSE,
    is_checking_account  BOOLEAN DEFAULT FALSE,
    is_redeposit         BOOLEAN DEFAULT FALSE,
    created_at           TIMESTAMPTZ DEFAULT now(),
    updated_at           TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE core.product_special_condition IS '예·적금 상품 우대조건 테이블';
COMMENT ON COLUMN core.product_special_condition.is_non_face_to_face IS '비대면가입';
COMMENT ON COLUMN core.product_special_condition.is_bank_app IS '은행앱사용';
COMMENT ON COLUMN core.product_special_condition.is_salary_linked IS '급여연동';
COMMENT ON COLUMN core.product_special_condition.is_pension_linked IS '연금';
COMMENT ON COLUMN core.product_special_condition.is_utility_linked IS '공과금연동';
COMMENT ON COLUMN core.product_special_condition.is_card_usage IS '카드사용';
COMMENT ON COLUMN core.product_special_condition.is_first_transaction IS '첫거래';
COMMENT ON COLUMN core.product_special_condition.is_checking_account IS '입출금통장';
COMMENT ON COLUMN core.product_special_condition.is_redeposit IS '재예치';

CREATE UNIQUE INDEX IF NOT EXISTS uq_product_special_condition
    ON core.product_special_condition(product_id);
"""

# ------------------------
# BASE 업서트 (변경 추적 포함)
# ------------------------
BASE_CLOSE_SQL = """
WITH candidates AS (
  SELECT
    b.product_type, b.ext_source, b.fin_prdt_cd AS ext_id, b.payload, b.content_hash, b.run_ts, b.dcls_month,
    ROW_NUMBER() OVER (
      PARTITION BY b.product_type, b.ext_source, b.fin_prdt_cd
      ORDER BY to_date(COALESCE(b.dcls_month,'190001'),'YYYYMM') DESC, b.run_ts DESC
    ) AS rn
  FROM stg.finproduct_base_landing b
  WHERE b.product_type = :product_type
),
to_close AS (
  SELECT p.id, p.ext_id, p.fin_prdt_nm
  FROM core.product p
  JOIN candidates c
    ON c.rn = 1
   AND p.is_current = TRUE
   AND p.product_type = c.product_type
   AND p.ext_source  = c.ext_source
   AND p.ext_id      = c.ext_id
   AND p.content_hash <> c.content_hash
)
UPDATE core.product p
SET is_current = FALSE, valid_to_ts = now(), updated_at = now()
FROM to_close x
WHERE p.id = x.id
RETURNING p.id, p.ext_id, p.fin_prdt_nm;
"""

BASE_INSERT_SQL = """
WITH candidates AS (
  SELECT
    b.product_type, b.ext_source, b.fin_prdt_cd AS ext_id, b.payload, b.content_hash, b.run_ts, b.dcls_month,
    ROW_NUMBER() OVER (
      PARTITION BY b.product_type, b.ext_source, b.fin_prdt_cd
      ORDER BY to_date(COALESCE(b.dcls_month,'190001'),'YYYYMM') DESC, b.run_ts DESC
    ) AS rn
  FROM stg.finproduct_base_landing b
  WHERE b.product_type = :product_type
),
need_insert AS (
  SELECT c.*
  FROM candidates c
  LEFT JOIN core.product p
    ON p.is_current = TRUE
   AND p.product_type = c.product_type
   AND p.ext_source  = c.ext_source
   AND p.ext_id      = c.ext_id
  WHERE c.rn = 1 AND (p.id IS NULL OR p.content_hash <> c.content_hash)
)
INSERT INTO core.product (
  product_type, ext_source, ext_id, payload, content_hash,
  is_current, valid_from_ts, created_at, updated_at
)
SELECT
  product_type, ext_source, ext_id, payload, content_hash,
  TRUE, now(), now(), now()
FROM need_insert
RETURNING id, ext_id, fin_prdt_nm;
"""

BASE_TOUCH_SQL = """
WITH candidates AS (
  SELECT
    b.product_type, b.ext_source, b.fin_prdt_cd AS ext_id, b.payload, b.content_hash, b.run_ts, b.dcls_month,
    ROW_NUMBER() OVER (
      PARTITION BY b.product_type, b.ext_source, b.fin_prdt_cd
      ORDER BY to_date(COALESCE(b.dcls_month,'190001'),'YYYYMM') DESC, b.run_ts DESC
    ) AS rn
  FROM stg.finproduct_base_landing b
  WHERE b.product_type = :product_type
)
UPDATE core.product p
SET updated_at = now()
FROM candidates c
WHERE c.rn = 1
  AND p.is_current = TRUE
  AND p.product_type = c.product_type
  AND p.ext_source  = c.ext_source
  AND p.ext_id      = c.ext_id
  AND p.content_hash = c.content_hash
RETURNING p.id;
"""

# ------------------------
# OPTION 업서트 (동일)
# ------------------------
OPTION_UPSERT_SQL = """
-- 현재본 product들과 매칭되는 옵션 "원천" 후보
WITH base_now AS (
  SELECT p.id AS product_id, p.ext_source, p.ext_id, p.dcls_month
  FROM core.product p
  WHERE p.is_current = TRUE
    AND p.product_type = :product_type
),

-- 0) 원천 후보 + 옵션키/유효월 파생
opt_raw AS (
  SELECT
    o.*,
    bn.product_id,
    bn.dcls_month AS base_month,
    /* 옵션키(정규화) */
    COALESCE(NULLIF(o.payload->>'save_trm','')::int, -1) AS k_save_trm,
    COALESCE(o.payload->>'intr_rate_type','')           AS k_intr_rate_type,
    COALESCE(o.payload->>'rsrv_type','')                AS k_rsrv_type,
    /* 유효 비교용 공시월: 옵션에 없으면 base월 사용 */
    COALESCE(o.dcls_month, bn.dcls_month)               AS eff_month
  FROM stg.finproduct_option_landing o
  JOIN base_now bn
    ON bn.ext_source = o.ext_source
   AND bn.ext_id     = o.fin_prdt_cd
   AND (o.dcls_month IS NULL OR o.dcls_month = bn.dcls_month)
  WHERE o.product_type = :product_type
),

-- 1) 동일 (product_id, 옵션키) 내에서 '최신 1건'만 남김
--    기준: eff_month DESC → run_ts DESC → content_hash DESC
opt_canon AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY product_id, k_save_trm, k_intr_rate_type, k_rsrv_type
      ORDER BY to_date(COALESCE(eff_month,'190001'), 'YYYYMM') DESC,
               run_ts DESC,
               content_hash DESC
    ) AS rn
  FROM opt_raw
),

opt_candidates AS (
  SELECT * FROM opt_canon WHERE rn = 1
),

-- 2) 변경된 현재본 옵션 해제
closed AS (
  UPDATE core.product_option po
  SET is_current = FALSE, valid_to_ts = now(), updated_at = now()
  FROM opt_candidates oc
  WHERE po.is_current = TRUE
    AND po.product_id = oc.product_id
    AND COALESCE(po.save_trm,-1)       = oc.k_save_trm
    AND COALESCE(po.intr_rate_type,'') = oc.k_intr_rate_type
    AND COALESCE(po.rsrv_type,'')      = oc.k_rsrv_type
    AND po.content_hash <> oc.content_hash
  RETURNING po.product_id
),

-- 3) 신규 현재본 옵션 INSERT (없거나 바뀐 경우)
inserted AS (
  INSERT INTO core.product_option (
    product_id, payload, content_hash,
    is_current, valid_from_ts, created_at, updated_at
  )
  SELECT
    oc.product_id, oc.payload, oc.content_hash,
    TRUE, now(), now(), now()
  FROM opt_candidates oc
  LEFT JOIN core.product_option cur
    ON cur.is_current = TRUE
   AND cur.product_id = oc.product_id
   AND COALESCE(cur.save_trm,-1)       = oc.k_save_trm
   AND COALESCE(cur.intr_rate_type,'') = oc.k_intr_rate_type
   AND COALESCE(cur.rsrv_type,'')      = oc.k_rsrv_type
  WHERE cur.id IS NULL OR cur.content_hash <> oc.content_hash
  RETURNING product_id
),

-- 4) 동일 내용 옵션 touch
touched AS (
  UPDATE core.product_option po
  SET updated_at = now()
  FROM opt_candidates oc
  WHERE po.is_current = TRUE
    AND po.product_id = oc.product_id
    AND COALESCE(po.save_trm,-1)       = oc.k_save_trm
    AND COALESCE(po.intr_rate_type,'') = oc.k_intr_rate_type
    AND COALESCE(po.rsrv_type,'')      = oc.k_rsrv_type
    AND po.content_hash = oc.content_hash
  RETURNING po.product_id
)

SELECT
  (SELECT count(*) FROM closed)   AS closed_cnt,
  (SELECT count(*) FROM inserted) AS inserted_cnt,
  (SELECT count(*) FROM touched)  AS touched_cnt;
"""

# ------------------------
# 옵션 세트 해시/개수 갱신 (동일)
# ------------------------
RECOMPUTE_OPTION_SET_HASH_SQL = """
WITH base_now AS (
  SELECT p.id AS product_id
  FROM core.product p
  WHERE p.is_current = TRUE
    AND p.product_type = :product_type
),
affected AS (  -- 이번 배치에서 고려된 상품만(최적화 목적)
  SELECT DISTINCT bn.product_id
  FROM base_now bn
),
agg AS (
  SELECT
    po.product_id,
    COUNT(*) AS options_count,
    encode(
      digest(
        string_agg(
          format(
            '%s|%s|%s|%s|%s',
            COALESCE(po.save_trm, -1),
            COALESCE(po.intr_rate_type, ''),
            COALESCE(po.rsrv_type, ''),
            COALESCE(TO_CHAR(po.intr_rate,  'FM999999999.00000'), ''),
            COALESCE(TO_CHAR(po.intr_rate2, 'FM999999999.00000'), '')
          ),
          ';'  -- 구분자
          ORDER BY po.save_trm, po.intr_rate_type, po.rsrv_type, po.intr_rate, po.intr_rate2
        ),
        'sha256'
      ),
      'hex'
    ) AS options_set_hash
  FROM core.product_option po
  WHERE po.is_current = TRUE
    AND po.product_id IN (SELECT product_id FROM affected)
  GROUP BY po.product_id
)
UPDATE core.product p
SET options_set_hash = a.options_set_hash,
    options_count    = a.options_count,
    updated_at       = now()
FROM agg a
WHERE p.id = a.product_id
  AND p.is_current = TRUE
  AND p.product_type = :product_type;

-- 옵션이 0개인 경우 처리
WITH base_now AS (
  SELECT p.id AS product_id
  FROM core.product p
  WHERE p.is_current = TRUE
    AND p.product_type = :product_type
),
zero AS (
  SELECT bn.product_id
  FROM base_now bn
  WHERE NOT EXISTS (
    SELECT 1 FROM core.product_option po
    WHERE po.product_id = bn.product_id AND po.is_current = TRUE
  )
)
UPDATE core.product p
SET options_set_hash = NULL,
    options_count    = 0,
    updated_at       = now()
FROM zero z
WHERE p.id = z.product_id
  AND p.is_current = TRUE
  AND p.product_type = :product_type;
"""

# ------------------------
# Gemini 우대조건 분석
# ------------------------
ANALYSIS_PROMPT = """
다음은 은행 예금/적금 상품의 우대조건 텍스트입니다. 
이 텍스트를 분석하여 아래 9개 카테고리 각각에 해당하는지 true/false로 판단해주세요.

카테고리 정의:
1. is_non_face_to_face: 비대면 가입 (인터넷, 모바일, 온라인 등)
2. is_bank_app: 은행 앱 사용 (모바일뱅킹, 앱 이용 등)
3. is_salary_linked: 급여 연동 (급여이체, 급여통장 등)
4. is_pension_linked: 연금 관련 (국민연금, 공무원연금, 사학연금, 연금통장 등)
5. is_utility_linked: 공과금 연동 (공과금 자동이체, 공공요금 등)
6. is_card_usage: 카드 사용 (결제계좌, 체크카드, 신용카드 등)
7. is_first_transaction: 첫 거래 (신규고객, 첫거래, 신규가입 등)
8. is_checking_account: 입출금통장 (입출금계좌, 자유적금, 적립식예금 등)
9. is_redeposit: 재예치 (재예치, 만기연장, 자동연장 등)

분석할 텍스트:
{spcl_cnd}

응답 형식 (JSON):
{{
    "is_non_face_to_face": true/false,
    "is_bank_app": true/false,
    "is_salary_linked": true/false,
    "is_pension_linked": true/false,
    "is_utility_linked": true/false,
    "is_card_usage": true/false,
    "is_first_transaction": true/false,
    "is_checking_account": true/false,
    "is_redeposit": true/false
}}

JSON 형태로만 응답해주세요.
"""

def analyze_special_condition(spcl_cnd: str) -> Optional[SpecialCondition]:
    """Gemini API를 사용하여 우대조건 텍스트 분석"""
    if not GEMINI_AVAILABLE or not GEMINI_API_KEY or GEMINI_API_KEY == "your_gemini_api_key_here":
        log.warning("Gemini API not available - skipping special condition analysis")
        return None
        
    if not spcl_cnd or spcl_cnd.strip() == "":
        return SpecialCondition()
    
    try:
        model = genai.GenerativeModel('gemini-2.5-flash-lite')
        prompt = ANALYSIS_PROMPT.format(spcl_cnd=spcl_cnd)
        
        response = model.generate_content(prompt)
        result_text = response.text.strip()
        
        # JSON 파싱
        if result_text.startswith('```json'):
            result_text = result_text[7:-3].strip()
        elif result_text.startswith('```'):
            result_text = result_text[3:-3].strip()
            
        result_dict = json.loads(result_text)
        
        return SpecialCondition(
            is_non_face_to_face=result_dict.get('is_non_face_to_face', False),
            is_bank_app=result_dict.get('is_bank_app', False),
            is_salary_linked=result_dict.get('is_salary_linked', False),
            is_pension_linked=result_dict.get('is_pension_linked', False),
            is_utility_linked=result_dict.get('is_utility_linked', False),
            is_card_usage=result_dict.get('is_card_usage', False),
            is_first_transaction=result_dict.get('is_first_transaction', False),
            is_checking_account=result_dict.get('is_checking_account', False),
            is_redeposit=result_dict.get('is_redeposit', False)
        )
        
    except Exception as e:
        log.error(f"Gemini API 분석 실패: {e}")
        log.error(f"텍스트: {spcl_cnd[:100]}...")
        return None

def save_special_condition(conn, product_id: int, condition: SpecialCondition) -> None:
    """우대조건 분석 결과 저장"""
    query = """
    INSERT INTO core.product_special_condition (
        product_id, is_non_face_to_face, is_bank_app, is_salary_linked,
        is_pension_linked, is_utility_linked, is_card_usage,
        is_first_transaction, is_checking_account, is_redeposit,
        created_at, updated_at
    ) VALUES (
        :product_id, :is_non_face_to_face, :is_bank_app, :is_salary_linked,
        :is_pension_linked, :is_utility_linked, :is_card_usage,
        :is_first_transaction, :is_checking_account, :is_redeposit,
        now(), now()
    )
    ON CONFLICT (product_id) DO UPDATE SET
        is_non_face_to_face = EXCLUDED.is_non_face_to_face,
        is_bank_app = EXCLUDED.is_bank_app,
        is_salary_linked = EXCLUDED.is_salary_linked,
        is_pension_linked = EXCLUDED.is_pension_linked,
        is_utility_linked = EXCLUDED.is_utility_linked,
        is_card_usage = EXCLUDED.is_card_usage,
        is_first_transaction = EXCLUDED.is_first_transaction,
        is_checking_account = EXCLUDED.is_checking_account,
        is_redeposit = EXCLUDED.is_redeposit,
        updated_at = now()
    """
    
    conn.execute(text(query), {
        "product_id": product_id,
        "is_non_face_to_face": condition.is_non_face_to_face,
        "is_bank_app": condition.is_bank_app,
        "is_salary_linked": condition.is_salary_linked,
        "is_pension_linked": condition.is_pension_linked,
        "is_utility_linked": condition.is_utility_linked,
        "is_card_usage": condition.is_card_usage,
        "is_first_transaction": condition.is_first_transaction,
        "is_checking_account": condition.is_checking_account,
        "is_redeposit": condition.is_redeposit
    })

def analyze_changed_products(conn, changed_product_ids: List[int], skip_gemini: bool = False) -> Tuple[int, int]:
    """변경된 상품들의 우대조건 분석"""
    if skip_gemini or not changed_product_ids:
        return 0, 0
    
    # 우대조건이 있는 상품만 조회
    query = """
    SELECT id, fin_prdt_nm, spcl_cnd
    FROM core.product 
    WHERE id = ANY(:product_ids) 
      AND is_current = TRUE 
      AND spcl_cnd IS NOT NULL 
      AND spcl_cnd != ''
    """
    
    result = conn.execute(text(query), {"product_ids": changed_product_ids})
    products = [dict(row._mapping) for row in result]
    
    success_count = 0
    failure_count = 0
    
    for product in products:
        product_id = product['id']
        fin_prdt_nm = product['fin_prdt_nm']
        spcl_cnd = product['spcl_cnd']
        
        log.info(f"우대조건 분석 중: {fin_prdt_nm}")
        
        condition = analyze_special_condition(spcl_cnd)
        
        if condition is not None:
            save_special_condition(conn, product_id, condition)
            success_count += 1
            log.info(f"✓ 우대조건 분석 완료: {product_id}")
        else:
            failure_count += 1
            log.error(f"✗ 우대조건 분석 실패: {product_id}")
    
    return success_count, failure_count

def upsert_for_type(conn, product_type: str, skip_gemini: bool = False) -> Tuple[int, int, int, int, int]:
    """타입별 업서트 + 우대조건 분석"""
    changed_product_ids = []
    
    # 1) BASE: close / insert / touch (변경 추적)
    close_result = conn.execute(text(BASE_CLOSE_SQL), {"product_type": product_type})
    closed_products = [dict(row._mapping) for row in close_result]
    
    insert_result = conn.execute(text(BASE_INSERT_SQL), {"product_type": product_type})
    inserted_products = [dict(row._mapping) for row in insert_result]
    
    conn.execute(text(BASE_TOUCH_SQL), {"product_type": product_type})
    
    # 변경된 상품 ID 수집
    changed_product_ids.extend([p['id'] for p in inserted_products])
    
    # 2) OPTION: upsert
    res = conn.execute(text(OPTION_UPSERT_SQL), {"product_type": product_type}).mappings().first()
    closed_cnt = int(res["closed_cnt"])
    inserted_cnt = int(res["inserted_cnt"])
    touched_cnt = int(res["touched_cnt"])
    
    # 3) 옵션 세트 해시/개수 재계산
    conn.execute(text(RECOMPUTE_OPTION_SET_HASH_SQL), {"product_type": product_type})
    
    # 4) 변경된 상품의 우대조건 분석
    gemini_success, gemini_failure = analyze_changed_products(conn, changed_product_ids, skip_gemini)
    
    return closed_cnt, inserted_cnt, touched_cnt, gemini_success, gemini_failure

def run(which: str, skip_gemini: bool = False) -> None:
    if which not in ("all", "deposit", "saving"):
        raise SystemExit("Usage: python stg_to_core.py [all|deposit|saving] [--skip-gemini]")

    engine: Engine = create_engine(PG_DSN_FIN, future=True)

    # 부트스트랩
    with engine.begin() as conn:
        conn.execute(text(BOOTSTRAP_SQL))

    types = ("deposit", "saving") if which == "all" else (which,)
    total_ins = total_cls = total_tch = total_gem_suc = total_gem_fail = 0

    for t in types:
        pt = PRODUCT_TYPES[t]
        log.info("UPSERT start | product_type=%s", pt)
        with engine.begin() as conn:
            c, i, h, gs, gf = upsert_for_type(conn, pt, skip_gemini)
        total_cls += c
        total_ins += i
        total_tch += h
        total_gem_suc += gs
        total_gem_fail += gf
        log.info("UPSERT done | type=%s | opt_closed=%d, opt_inserted=%d, opt_touched=%d | gemini_success=%d, gemini_failed=%d", 
                 pt, c, i, h, gs, gf)

    log.info("ALL DONE | opt_closed=%d, opt_inserted=%d, opt_touched=%d | gemini_success=%d, gemini_failed=%d", 
             total_cls, total_ins, total_tch, total_gem_suc, total_gem_fail)

if __name__ == "__main__":
    args = sys.argv[1:]
    arg = args[0] if args else "all"
    skip_gemini = "--skip-gemini" in args
    
    if skip_gemini:
        log.info("Gemini 분석을 건너뜁니다")
    
    run(arg, skip_gemini)