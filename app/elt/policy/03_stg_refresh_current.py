#!/usr/bin/env python3
"""
stg_current_refresh.py
- STG landing -> STG current 갱신
- 정책별 최신 해시(record_hash)를 반영하고, 관측 시각/활성 플래그를 관리합니다.

ENV (.env 권장):
  PG_DSN=postgresql://<user>:<pass>@<host>:<port>/<db>
  LOOKBACK_HOURS=24         # 최근 N시간 landing만 보고 최신 선택(0이면 전체 스캔)
  INACTIVE_AFTER_DAYS=14    # N일 이상 관측 안 되면 is_active=false (0이면 미적용)
  LOG_LEVEL=INFO
"""

import os
import logging
from datetime import datetime, timezone, timedelta

import psycopg
from psycopg.rows import dict_row

try:
    from dotenv import load_dotenv  # optional
    load_dotenv()
except Exception:
    pass

# ------------ ENV ------------
def env_str(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing environment variable: {name}")
    return v

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v not in (None, "") else default

PG_DSN = env_str("PG_DSN")
LOOKBACK_HOURS = env_int("LOOKBACK_HOURS", 24)
INACTIVE_AFTER_DAYS = env_int("INACTIVE_AFTER_DAYS", 14)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s stg_current :: %(message)s",
)
log = logging.getLogger("stg_current_refresh")

# ------------ Bootstrap ------------
BOOTSTRAP_SQL = """
create schema if not exists stg;

create table if not exists stg.youthpolicy_current (
  policy_id     text        primary key,
  record_hash   char(64)    not null,
  first_seen_at timestamptz not null default now(),
  last_seen_at  timestamptz not null default now(),
  is_active     boolean     not null default true
);
create index if not exists idx_stg_current_hash on stg.youthpolicy_current(record_hash);
"""

def bootstrap(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(BOOTSTRAP_SQL)
    conn.commit()
    log.info("Bootstrap: stg.youthpolicy_current ready")

# ------------ Core ------------
def refresh_current(conn: psycopg.Connection) -> None:
    cutoff_ts = None
    if LOOKBACK_HOURS > 0:
        cutoff_ts = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

    with conn.cursor(row_factory=dict_row) as cur:
        # 1) 최신 후보 집합(tmp_latest) 구성
        if cutoff_ts is not None:
            cur.execute("""
                create temporary table tmp_latest on commit drop as
                select distinct on (l.policy_id)
                  l.policy_id, l.record_hash, l.ingested_at
                from stg.youthpolicy_landing l
                where l.ingested_at >= %s
                order by l.policy_id, l.ingested_at desc;
            """, (cutoff_ts,))
        else:
            cur.execute("""
                create temporary table tmp_latest on commit drop as
                select distinct on (l.policy_id)
                  l.policy_id, l.record_hash, l.ingested_at
                from stg.youthpolicy_landing l
                order by l.policy_id, l.ingested_at desc;
            """)

        # 2) 최초 관측 시각(전 구간) 집계
        cur.execute("""
            create temporary table tmp_first_seen on commit drop as
            select policy_id, min(ingested_at) as first_seen_at
            from stg.youthpolicy_landing
            group by policy_id;
        """)

        # 3) 변경 건수 확인(디버깅용)
        cur.execute("""
            select count(*) as diff_count
            from tmp_latest tl
            join stg.youthpolicy_current c on c.policy_id = tl.policy_id
            where c.record_hash <> tl.record_hash;
        """)
        diff_count = cur.fetchone()["diff_count"]
        log.info("Diff (hash changed) in window: %s", diff_count)

        # 4) upsert 적용: 최신 해시 반영 + last_seen_at 갱신 + first_seen_at 최소값 유지
        cur.execute("""
            insert into stg.youthpolicy_current
                (policy_id, record_hash, first_seen_at, last_seen_at, is_active)
            select
                tl.policy_id,
                tl.record_hash,
                fs.first_seen_at,
                now(),
                true
            from tmp_latest tl
            join tmp_first_seen fs using (policy_id)
            on conflict (policy_id) do update
              set last_seen_at = excluded.last_seen_at,
                  is_active    = true,
                  record_hash  = case
                                   when stg.youthpolicy_current.record_hash <> excluded.record_hash
                                   then excluded.record_hash
                                   else stg.youthpolicy_current.record_hash
                                 end,
                  first_seen_at = least(stg.youthpolicy_current.first_seen_at, excluded.first_seen_at);
        """)

        # 5) 비활성 스윕(옵션) - interval 파라미터 대신 컷오프 타임스탬프 사용
        if INACTIVE_AFTER_DAYS > 0:
            inactive_cutoff = datetime.now(timezone.utc) - timedelta(days=INACTIVE_AFTER_DAYS)
            cur.execute("""
                update stg.youthpolicy_current
                   set is_active = false
                 where last_seen_at < %s;
            """, (inactive_cutoff,))

        # 6) 현황 로그
        cur.execute("select count(*) as seen_policies from tmp_latest;")
        seen_cnt = cur.fetchone()["seen_policies"]
        cur.execute("select count(*) as current_rows from stg.youthpolicy_current;")
        cur_cnt = cur.fetchone()["current_rows"]

    conn.commit()
    log.info("Upsert applied. seen_in_window=%s, current_total=%s, inactive_threshold=%sd",
             seen_cnt, cur_cnt, INACTIVE_AFTER_DAYS)

def main() -> None:
    log.info("STG current refresh start (lookback=%sh, inactive_after=%sd)", LOOKBACK_HOURS, INACTIVE_AFTER_DAYS)
    with psycopg.connect(PG_DSN) as conn:
        bootstrap(conn)
        refresh_current(conn)
    log.info("STG current refresh complete")

if __name__ == "__main__":
    main()