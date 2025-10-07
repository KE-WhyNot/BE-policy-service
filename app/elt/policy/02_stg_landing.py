#!/usr/bin/env python3
"""
stg_landing_from_raw.py
- RAW (raw.youthpolicy_pages.payload) 의 페이지 단위 JSON을
  정책 단위(row)로 풀어 stg.youthpolicy_landing에 적재합니다.
- current 갱신은 하지 않습니다.

ENV (.env 권장):
  PG_DSN=postgresql://<user>:<pass>@<host>:<port>/<db>
  LOOKBACK_HOURS=0           # 0 = 전체 처리, >0 = 최근 N시간 RAW만
  PROCESS_ONLY_UNSEEN=1      # 1 = 이미 처리한 RAW 페이지(ingest_id) 건너뜀
  BATCH_SIZE=1000
  LOG_LEVEL=INFO
"""

import os
import hashlib
import logging
from typing import Any, Dict, Iterable, List, Tuple

import orjson
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

try:
    from dotenv import load_dotenv  # optional
    load_dotenv()
except Exception:
    pass

# ---------- ENV ----------
def env_str(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing environment variable: {name}")
    return v

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v not in (None, "") else default

PG_DSN = env_str("PG_DSN")
LOOKBACK_HOURS = env_int("LOOKBACK_HOURS", 0)
PROCESS_ONLY_UNSEEN = env_int("PROCESS_ONLY_UNSEEN", 1)
BATCH_SIZE = env_int("BATCH_SIZE", 1000)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s stg_landing :: %(message)s",
)
log = logging.getLogger("stg_landing_from_raw")

# ---------- Bootstrap DDL (idempotent) ----------
BOOTSTRAP_SQL = """
create schema if not exists stg;

create table if not exists stg.youthpolicy_landing (
  policy_id     text        not null,
  record_hash   char(64)    not null,
  raw_json      jsonb       not null,
  ingested_at   timestamptz not null default now(),
  raw_ingest_id uuid        not null,
  page_no       int         not null,
  primary key (policy_id, record_hash)
);
create index if not exists idx_stg_landing_raw on stg.youthpolicy_landing(raw_ingest_id);
create index if not exists idx_stg_landing_policy on stg.youthpolicy_landing(policy_id);
"""

def bootstrap(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(BOOTSTRAP_SQL)
    conn.commit()
    log.info("STG bootstrap complete (landing ready)")

# ---------- Helpers ----------

# page 메타(혹시 item에 섞여 들어와도 방어)
PAGE_META = {
    "totCount","pageNo","pageNum","pageIndex","pageSize",
    "nowTs","requestId","timestamp"
}

# 정책 레코드 내부의 운영성/비콘텐츠 필드
POLICY_NOISE = {
    "inqCnt",            # 조회수: 콘텐츠 무관, 호출마다 달라짐
    # "frstRegDt",         # 최초등록일시: 감사용 메타
    # "lastMdfcnDt",       # 최종수정일시: 내용 동일해도 갱신될 수 있음
    # # (선택) 등록자 기관 메타: 화면/도메인에 불필요하면 제외
    # "rgtrInstCd","rgtrInstCdNm",
    # "rgtrUpInstCd","rgtrUpInstCdNm",
    # "rgtrHghrkInstCd","rgtrHghrkInstCdNm",
}

# 해시 계산 시 제외할(휘발 가능) top-level 필드
DROP_FIELDS = PAGE_META | POLICY_NOISE

ID_CANDIDATE_KEYS = (
    # 실제 고유키 후보 (서비스에 맞게 우선순위 조정)
    # "plcyNo", "policyId", "bizId", "pblntfNo",
    "plcyNo"
)

TITLE_KEYS = ("plcyTitl", "title")
END_KEYS = ("rceptEndDe", "applyEndYmd", "rcptEndDt")

def extract_items_from_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """RAW payload에서 정책 배열만 추출."""
    result = payload.get("result", payload)
    arr = result.get("youthPolicyList") or result.get("items") or []
    if isinstance(arr, list):
        return [x for x in arr if isinstance(x, dict)]
    return []

def make_surrogate_id(item: Dict[str, Any]) -> str:
    """업스트림 ID가 없을 때 안전한 서러겟 키 생성."""
    title = next((item.get(k) for k in TITLE_KEYS if item.get(k)), "")
    end_dt = next((item.get(k) for k in END_KEYS if item.get(k)), "")
    # provider/기관 추정 필드 몇 개 더 섞기
    provider = item.get("jurMnnm") or item.get("fndnInsttNm") or item.get("cnsgNmor") or ""
    basis = f"{title}|{provider}|{end_dt}"
    return "SURR::" + hashlib.md5(basis.encode("utf-8")).hexdigest()

def pick_policy_id(item: Dict[str, Any]) -> str:
    # for k in ID_CANDIDATE_KEYS:
    #     v = item.get(k)
    #     if v:
    #         return str(v)
    # return make_surrogate_id(item)
    return str(item.get("plcyNo"))

def canonical_bytes(item: Dict[str, Any]) -> bytes:
    """DROP_FIELDS 제거 후 key-sort하여 안정적 JSON 바이트 생성."""
    pruned = {k: v for k, v in item.items() if k not in DROP_FIELDS}
    return orjson.dumps(pruned, option=orjson.OPT_SORT_KEYS)

def record_hash(item: Dict[str, Any]) -> str:
    return hashlib.sha256(canonical_bytes(item)).hexdigest()

def chunked(it: Iterable, size: int) -> Iterable[List]:
    buf: List = []
    for x in it:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

# ---------- Core ----------
def load_raw_pages(conn: psycopg.Connection) -> List[Dict[str, Any]]:
    """처리할 RAW 페이지들을 로드."""
    with conn.cursor(row_factory=dict_row) as cur:
        if PROCESS_ONLY_UNSEEN:
            if LOOKBACK_HOURS > 0:
                cur.execute(
                    """
                    select p.ingest_id, p.page_no, p.payload
                      from raw.youthpolicy_pages p
                     where p.ingested_at >= now() - interval '%s hours'
                       and not exists (
                           select 1 from stg.youthpolicy_landing l
                            where l.raw_ingest_id = p.ingest_id
                       )
                     order by p.ingested_at asc, p.page_no asc
                    """,
                    (LOOKBACK_HOURS,),
                )
            else:
                cur.execute(
                    """
                    select p.ingest_id, p.page_no, p.payload
                      from raw.youthpolicy_pages p
                     where not exists (
                           select 1 from stg.youthpolicy_landing l
                            where l.raw_ingest_id = p.ingest_id
                       )
                     order by p.ingested_at asc, p.page_no asc
                    """
                )
        else:
            if LOOKBACK_HOURS > 0:
                cur.execute(
                    """
                    select ingest_id, page_no, payload
                      from raw.youthpolicy_pages
                     where ingested_at >= now() - interval '%s hours'
                     order by ingested_at asc, page_no asc
                    """,
                    (LOOKBACK_HOURS,),
                )
            else:
                cur.execute(
                    "select ingest_id, page_no, payload from raw.youthpolicy_pages order by ingested_at asc, page_no asc"
                )
        rows = cur.fetchall()
    log.info("Loaded RAW pages: %s (lookback=%sh, only_unseen=%s)", len(rows), LOOKBACK_HOURS, bool(PROCESS_ONLY_UNSEEN))
    return rows

def upsert_landing(conn: psycopg.Connection, pages: List[Dict[str, Any]]) -> None:
    """RAW 페이지들을 landing에 적재."""
    total_items = 0
    surrogate_used = 0

    with conn.cursor() as cur:
        for r in pages:
            ingest_id = r["ingest_id"]
            page_no = int(r["page_no"])
            payload = r["payload"]

            items = extract_items_from_payload(payload)
            if not items:
                continue

            prepared: List[Tuple[str, str, Json, str, int]] = []
            for it in items:
                pid = pick_policy_id(it)
                if pid.startswith("SURR::"):
                    surrogate_used += 1
                h = record_hash(it)
                prepared.append((pid, h, Json(it), str(ingest_id), page_no))

            total_items += len(prepared)

            for batch in chunked(prepared, BATCH_SIZE):
                cur.executemany(
                    """
                    insert into stg.youthpolicy_landing
                        (policy_id, record_hash, raw_json, raw_ingest_id, page_no)
                    values (%s, %s, %s, %s, %s)
                    on conflict do nothing
                    """,
                    batch,
                )
            conn.commit()

    log.info("Landing upsert complete. items=%s, surrogate_used=%s", total_items, surrogate_used)

def main() -> None:
    log.info("STG landing transform start")
    with psycopg.connect(PG_DSN) as conn:
        bootstrap(conn)
        pages = load_raw_pages(conn)
        if not pages:
            log.info("No RAW pages to process. Done.")
            return
        upsert_landing(conn, pages)
    log.info("STG landing transform done")

if __name__ == "__main__":
    main()