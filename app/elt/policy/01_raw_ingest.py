#!/usr/bin/env python3
"""
raw_ingest.py
- 청년정책 API의 페이지 응답을 RAW 스키마에 그대로 적재합니다.
- STG/CORE는 만들지 않습니다. (후속 파이프라인에서 처리)

필요 ENV (.env 권장):
  PG_DSN=postgresql://<user>:<pass>@<host>:<port>/<db>
  BASE_URL=https://www.youthcenter.go.kr/go/ythip/getPlcy
  API_KEY=<your_api_key>
  PAGE_SIZE=100
  START_PAGE=1         # 선택
  END_PAGE=0           # 선택(0 또는 미지정=모든 페이지)
  HTTP_TIMEOUT=20      # 선택(초)
  RETRY_MAX=5          # 선택(기본 5회)
  LOG_LEVEL=INFO       # 선택(DEBUG/INFO/WARN/ERROR)
"""

import os
import math
import time
import logging
from typing import Any, Dict

import httpx
import orjson
import psycopg
from psycopg.rows import tuple_row
from psycopg.types.json import Json
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

try:
    # 로컬 실행 편의: .env 자동 로드 (없어도 무방)
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# -------------------------
# 환경 변수
# -------------------------
def env_str(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing environment variable: {name}")
    return v

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v not in (None, "") else default

PG_DSN     = env_str("PG_DSN")
BASE_URL   = env_str("BASE_URL")
API_KEY    = env_str("API_KEY")
PAGE_SIZE  = env_int("PAGE_SIZE", 100)
START_PAGE = env_int("START_PAGE", 1)
END_PAGE   = env_int("END_PAGE", 0)  # 0 이면 끝까지
HTTP_TIMEOUT = env_int("HTTP_TIMEOUT", 20)
RETRY_MAX    = env_int("RETRY_MAX", 5)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s"
)
log = logging.getLogger("raw_ingest")

# -------------------------
# DB 부트스트랩 (idempotent)
# -------------------------
BOOTSTRAP_SQL = """
create extension if not exists pgcrypto;

create schema if not exists raw;

create table if not exists raw.youthpolicy_pages (
  ingest_id    uuid primary key default gen_random_uuid(),
  ingested_at  timestamptz not null default now(),
  page_no      int not null,
  page_size    int,
  base_url     text not null,
  query_params jsonb not null,
  http_status  int not null,
  payload      jsonb not null
);

-- 조회 편의 인덱스
create index if not exists idx_raw_yp_pages_time on raw.youthpolicy_pages(ingested_at desc);
create index if not exists idx_raw_yp_pages_page on raw.youthpolicy_pages(page_no);
"""

def bootstrap(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(BOOTSTRAP_SQL)
    conn.commit()
    log.info("DB bootstrap completed (raw.youthpolicy_pages ready)")

# -------------------------
# HTTP 호출
# -------------------------
class ApiError(Exception):
    pass

@retry(
    stop=stop_after_attempt(RETRY_MAX),
    wait=wait_exponential_jitter(initial=0.5, max=5),
    retry=retry_if_exception_type((httpx.HTTPError, ApiError))
)
def fetch_page(cli: httpx.Client, page_no: int, page_size: int) -> Dict[str, Any]:
    """
    페이지 요청.
    - youthcenter API는 page 파라미터 명이 변경될 수 있어, 두 가지를 모두 전달합니다.
    - 서버는 인지 가능한 파라미터 하나만 사용합니다.
    """
    params = {
        "apiKeyNm": API_KEY,
        "pageNum": page_no,
        "pageSize": page_size,
    }
    r = cli.get(BASE_URL, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()

    try:
        js = r.json()
    except Exception as e:
        raise ApiError(f"Invalid JSON response (page={page_no})") from e

    # 응답 유효성 간단 체크
    # 보편 구조: {"result":{"paging":{...}, "youthPolicyList":[...]}} 또는 flat
    result = js.get("result", js)
    items = result.get("youthPolicyList", result.get("items", []))
    if not isinstance(items, list):
        # 페이지가 비어있는 경우엔 list가 아닐 수 있음 → 그대로 통과시키되 로그만 남김
        log.warning("Unexpected items type on page %s: %s", page_no, type(items).__name__)

    return {
        "http_status": r.status_code,
        "json": js,
        "params": params,
    }

def extract_paging_meta(js: Dict[str, Any]) -> tuple[int, int, int]:
    """
    응답에서 페이징 메타데이터 추출
    반환: (pageNum, pageSize, totPage)
    일부 필드가 없을 수 있어 안전 계산합니다.
    """
    result = js.get("result", js)
    paging = result.get("paging", {})
    page_num  = int(paging.get("pageNum", paging.get("page", 0) or START_PAGE))
    page_size = int(paging.get("pageSize", PAGE_SIZE))
    tot_count = int(paging.get("totCount", 0))

    tot_page = 0
    if tot_count and page_size:
        tot_page = math.ceil(tot_count / page_size)

    # totPage가 명시되면 그 값을 우선
    tot_page = int(paging.get("totPage", tot_page or 0))

    return page_num, page_size, tot_page

# -------------------------
# 메인 루프
# -------------------------
def main() -> None:
    log.info("Starting RAW ingest → %s", BASE_URL)
    inserted_rows = 0

    with psycopg.connect(PG_DSN, row_factory=tuple_row) as conn:
        bootstrap(conn)

        # 트랜잭션: 각 페이지 단위로 커밋(대용량에서도 메모리 안정)
        with httpx.Client() as cli:
            page = max(1, START_PAGE)
            last_page_seen = 0

            while True:
                resp = fetch_page(cli, page, PAGE_SIZE)
                status = resp["http_status"]
                js = resp["json"]
                params = resp["params"]

                # 페이징 메타 파싱
                page_num, page_size, tot_page = extract_paging_meta(js)
                if last_page_seen == 0 and tot_page:
                    last_page_seen = tot_page
                    log.info("Paging detected: total_pages=%s page_size=%s", last_page_seen, page_size)

                # RAW 저장
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        insert into raw.youthpolicy_pages
                        (page_no, page_size, base_url, query_params, http_status, payload)
                        values (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            page,
                            page_size,
                            BASE_URL,
                            Json(params),
                            status,
                            Json(js),
                        )
                    )
                conn.commit()
                inserted_rows += 1
                log.info("Inserted RAW page: page=%s status=%s", page_num or page, status)

                # 종료 조건 계산
                if END_PAGE and page >= END_PAGE:
                    log.info("END_PAGE reached: %s", END_PAGE)
                    break

                # tot_page 기반 종료
                if last_page_seen and page >= last_page_seen:
                    log.info("Reached last page: %s", last_page_seen)
                    break

                # items 길이 기반(메타 없을 때)
                result = js.get("result", js)
                items = result.get("youthPolicyList", result.get("items", []))
                if isinstance(items, list) and len(items) == 0:
                    log.info("Empty items; stopping at page=%s", page)
                    break

                page += 1
                time.sleep(0.2)  # 과한 요청 방지(레이트리밋 여유)

    log.info("[OK] RAW ingest done. pages inserted=%s", inserted_rows)

if __name__ == "__main__":
    main()