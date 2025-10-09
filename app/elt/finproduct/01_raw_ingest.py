# raw_ingest.py
from __future__ import annotations
from typing import Any, Dict
import time
import json
import os
import logging

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type
import httpx

# -------------------------
# 환경 변수
# -------------------------
load_dotenv()

def env_str(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing environment variable: {name}")
    return v

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v not in (None, "") else default

PG_DSN_FIN           = env_str("PG_DSN_FIN")
API_KEY              = env_str("API_KEY_FIN")
BASE_URL_DEPOSIT     = env_str("BASE_URL_DEPOSIT")
BASE_URL_SAVING      = env_str("BASE_URL_SAVING")

START_PAGE   = env_int("START_PAGE", 1)
END_PAGE     = env_int("END_PAGE", 0)         # 0이면 끝까지
HTTP_TIMEOUT = env_int("HTTP_TIMEOUT", 10)
RETRY_MAX    = env_int("RETRY_MAX", 5)
LOG_LEVEL    = env_str("LOG_LEVEL", "INFO")

# -------------------------
# 로깅
# -------------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(name)s %(levelname)s :: %(message)s"
)
log = logging.getLogger("raw_ingest")

# -------------------------
# DB 연결
# -------------------------
def get_engine() -> Engine:
    return create_engine(PG_DSN_FIN, future=True)

def test_connection(engine: Engine) -> None:
    try:
        with engine.connect() as conn:
            conn.execute(text("select 1"))
        log.info("Database connection successful")
    except SQLAlchemyError as e:
        log.error("Database connection failed", exc_info=e)
        raise

engine = get_engine()
test_connection(engine)

# -------------------------
# HTTP 호출
# -------------------------
class ApiError(Exception):
    pass

@retry(
    stop=stop_after_attempt(RETRY_MAX),
    wait=wait_exponential_jitter(initial=0.5, max=5),
    retry=retry_if_exception_type((httpx.HTTPError, ApiError)),
)
def fetch_page(
    client: httpx.Client,
    base_url: str,
    top_fin_grp_no: str | int,
    page_no: int | str
) -> Dict[str, Any]:
    params = {
        "auth": API_KEY,
        "topFinGrpNo": top_fin_grp_no,
        "pageNo": page_no,
    }
    resp = client.get(str(base_url), params=params, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()

    try:
        js = resp.json()
    except Exception as e:
        raise ApiError(f"Invalid JSON response (page={page_no})") from e

    # 참고: 일부 API는 빈 페이지에서 리스트 타입이 아닐 수 있음 (경고만)
    result = js.get("result", js)
    items = result.get("baseList", result.get("optionList", []))
    if not isinstance(items, list):
        log.warning("Unexpected items type on page %s: %s", page_no, type(items).__name__)

    return {
        "http_status": resp.status_code,
        "json": js,
        "params": params,
    }

def extract_paging_meta(payload: Dict[str, Any]) -> tuple[int, int, int]:
    result = payload.get("result", payload)
    page_no     = int(result.get("now_page_no", 0) or START_PAGE)
    max_page_no = int(result.get("max_page_no", 0))
    total_count = int(result.get("total_count", 0))
    return page_no, max_page_no, total_count

# -------------------------
# 메인 루프
# -------------------------
def ingest_one(
    engine: Engine,
    product_type: str,      # 'DEPOSIT' | 'SAVING'
    base_url: str,
    top_fin_grp_no: str,
    label: str | None = None
) -> int:
    """
    base_url × top_fin_grp_no 조합 한 번을 수집.
    """
    log.info("Starting raw ingest [%s] → %s", label or f"{top_fin_grp_no}", base_url)
    inserted_rows = 0

    with httpx.Client() as client, engine.connect() as conn:
        page = max(1, START_PAGE)
        last_page_seen = 0

        while True:
            response = fetch_page(client, base_url, top_fin_grp_no, page)
            status  = response["http_status"]
            payload = response["json"]
            params  = response["params"]

            # 페이징 메타 파싱
            now_page_no, max_page_no, total_count = extract_paging_meta(payload)
            if last_page_seen == 0 and max_page_no:
                last_page_seen = max_page_no
                log.info("[%s] Paging detected: max_page_no=%s", label or top_fin_grp_no, last_page_seen)

            # RAW 저장 (product_type 포함)
            sql = text("""
                INSERT INTO raw.finproduct_pages
                (product_type, now_page_no, max_page_no, base_url, query_params, http_status, payload)
                VALUES
                (:product_type, :now_page_no, :max_page_no, :base_url, :query_params, :http_status, :payload)
            """)
            conn.execute(sql, {
                "product_type": product_type,
                "now_page_no": int(page),
                "max_page_no": int(max_page_no) if max_page_no else None,
                "base_url": str(base_url),
                "query_params": json.dumps(params),
                "http_status": int(status),
                "payload": json.dumps(payload),
            })
            conn.commit()
            inserted_rows += 1
            log.info("[%s] Inserted RAW page: now_page_no=%s status=%s", label or top_fin_grp_no, now_page_no or page, status)

            # 종료 조건
            if END_PAGE and page >= END_PAGE:
                log.info("[%s] END_PAGE reached: %s", label or top_fin_grp_no, END_PAGE)
                break

            if last_page_seen and page >= last_page_seen:
                log.info("[%s] Reached last page: %s", label or top_fin_grp_no, last_page_seen)
                break

            # 메타 없으면 items 비어있는지로 종료
            result = payload.get("result", payload)
            items = result.get("baseList", result.get("optionList", []))
            if isinstance(items, list) and len(items) == 0 and not max_page_no:
                log.info("[%s] Empty items; stopping at page=%s", label or top_fin_grp_no, page)
                break

            page += 1
            time.sleep(0.2)

    log.info("[OK] RAW ingest done [%s]. pages inserted=%s", label or top_fin_grp_no, inserted_rows)
    return inserted_rows

def main() -> None:
    engine = get_engine()

    # (product_type, url, top_fin_grp_no)
    combos = [
        ("DEPOSIT", BASE_URL_DEPOSIT, "020000"),
        ("DEPOSIT", BASE_URL_DEPOSIT, "030300"),
        ("SAVING",  BASE_URL_SAVING,  "020000"),
        ("SAVING",  BASE_URL_SAVING,  "030300"),
    ]

    total = 0
    for kind, url, grp in combos:
        total += ingest_one(engine, kind, url, grp, label=f"{kind}/{grp}")

    log.info("[OK] All combos done. total pages inserted=%s", total)

if __name__ == "__main__":
    main()