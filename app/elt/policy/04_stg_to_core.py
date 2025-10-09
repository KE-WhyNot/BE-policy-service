DEBUG = False

import os
import unicodedata
from dotenv import load_dotenv

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine, Connection

from typing import Iterable, List, Dict, Any, Optional, Set

from dataclasses import dataclass
from datetime import datetime, date, timezone, timedelta
KST = timezone(timedelta(hours=9))

from pprint import pprint

import json

from tqdm.auto import tqdm

def _chunked(seq: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
ETL_SOURCE = os.getenv("ETL_SOURCE")

def get_engine() -> Engine:
    return create_engine(DATABASE_URL, future=True)

def test_connection(engine: Engine) -> None:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Database connection successful.")
    except SQLAlchemyError as e:
        print(f"❌ Database connection failed: {e}")

# stg.youthpolicy_current와 core.policy에서 해시값 비교 후
# 신규/변경 정책에 대해서만 stg.youthpolicy_landing에서 데이터 가져오기
def fetch_changed_rows(conn: Connection) -> List[Dict[str, Any]]:
    sql = text(
        """
        SELECT  stg_c.policy_id,
                stg_c.record_hash,
                stg_l.raw_json
        FROM    stg.youthpolicy_current AS stg_c
        LEFT JOIN core.policy AS core_p
        ON stg_c.policy_id = core_p.id
        JOIN (
            SELECT DISTINCT ON (policy_id) policy_id, raw_json
            FROM stg.youthpolicy_landing
            ORDER BY policy_id, ingested_at DESC
        ) AS stg_l
        ON stg_c.policy_id = stg_l.policy_id
        WHERE   core_p.id IS NULL
            OR  stg_c.record_hash <> core_p.content_hash;
        """
    )
    rows = conn.execute(sql).mappings().all()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "policy_id": r["policy_id"],
            "record_hash": r["record_hash"],
            "raw_json": r["raw_json"],
        })
    return out


# TODO: 실제 AI 요약 API 호출 로직 구현 (파라미터, 로직, 반환값)
def ai_summary(text: str) -> str:
    return None

@dataclass
class NormalizedPolicy:
    id: str
    ext_id: str
    ext_source: str
    title: str
    summary_raw: str
    description_raw: str
    summary_ai: str
    # status: str # status는 update_policy_status.py에서 별도 처리
    apply_start: date
    apply_end: date
    last_external_modified: datetime
    views: int
    supervising_org: str
    operating_org: str
    apply_url: str
    ref_url_1: str
    ref_url_2: str
    # TODO: update 트리거 작동 검토
    # created_at: datetime
    # updated_at: datetime
    payload: Dict[str, Any]
    content_hash: str

    marital_status: str
    age_min: int
    age_max: int
    income_type: str
    income_min: int
    income_max: int
    income_text: str

    period_type: str
    period_start: date
    period_end: date
    period_etc: str

    apply_type: str
    announcement: str
    info_etc: str
    first_external_created: datetime
    required_documents: str

    application_process: str

    eligibility_additional: str
    eligibility_restrictive: str
    restrict_education: bool
    restrict_major: bool
    restrict_job_status: bool
    restrict_specialization: bool

    # 기본값 가지는 필드들은 dataclass 마지막 부분에 작성할 것
    subcategories: List[str] = None
    educations: List[str] = None
    job_status: List[str] = None
    majors: List[str] = None
    specializations: List[str] = None

    keywords: List[str] = None
    regions: List[str] = None

def normalize_row(row: Dict[str, Any]) -> NormalizedPolicy:
    raw_json = row["raw_json"]

    # core.policy
    id = row["policy_id"]
    ext_id = row["policy_id"]
    ext_source = ETL_SOURCE
    title = raw_json.get("plcyNm", "")
    summary_raw = raw_json.get("plcyCn", "")
    description_raw = raw_json.get("plcySprtCn", "")
    summary_ai = ai_summary(raw_json)
    # status = set_policy_status(raw_json.get("aplyPrdSeCd", ""), raw_json.get("aplyYmd", "")) # status는 update_policy_status.py에서 별도 처리
    apply_start, apply_end = parse_period_field(raw_json.get("aplyYmd", ""))
    last_external_modified = parse_modified_datetime(raw_json.get("lastMdfcnDt"))
    views = int(raw_json.get("inqCnt", 0))
    supervising_org = raw_json.get("sprvsnInstCdNm") or None
    operating_org = raw_json.get("operInstCdNm") or None
    apply_url = raw_json.get("aplyUrlAddr") or None
    ref_url_1 = raw_json.get("refUrlAddr1") or None
    ref_url_2 = raw_json.get("refUrlAddr2") or None
    payload = raw_json
    content_hash = row["record_hash"]

    # core.policy_eligibility_* (다대다 관계)
    subcategories = extract_list_from_payload(raw_json, "mclsfNm")
    educations = extract_list_from_payload(raw_json, "schoolCd")
    job_status = extract_list_from_payload(raw_json, "jobCd")
    majors = extract_list_from_payload(raw_json, "plcyMajorCd")
    specializations = extract_list_from_payload(raw_json, "sbizCd")

    # core.policy_keyword, core.policy_region
    keywords = extract_list_from_payload(raw_json, "plcyKywdNm")
    regions = extract_list_from_payload(raw_json, "zipCd")

    # core.policy_eligibility
    marital_status = set_marital_status(raw_json.get("mrgSttsCd", ""))
    age_min = to_int_or_none(raw_json.get("sprtTrgtMinAge", 0))
    age_max = to_int_or_none(raw_json.get("sprtTrgtMaxAge", 0))
    income_type = set_income_type(raw_json.get("earnCndSeCd", ""))
    income_min = to_int_or_none(raw_json.get("earnMinAmt", 0))
    income_max = to_int_or_none(raw_json.get("earnMaxAmt", 0))
    income_text = raw_json.get("earnEtcCn") or None

    # core.policy (추가 필드)
    period_type = set_period_type(raw_json.get("bizPrdSeCd", ""))
    period_start = parse_date(raw_json.get("bizPrdBgngYmd", ""))
    period_end = parse_date(raw_json.get("bizPrdEndYmd", ""))
    period_etc = clean_dash_to_null(raw_json.get("bizPrdEtcCn"))
    apply_type = set_apply_type(raw_json.get("aplyPrdSeCd", ""))
    announcement = clean_dash_to_null(raw_json.get("srngMthdCn"))
    info_etc = clean_dash_to_null(raw_json.get("etcMttrCn"))
    first_external_created = parse_modified_datetime(raw_json.get("frstRegDt", ""))
    required_documents = clean_dash_to_null(raw_json.get("sbmsnDcmntCn"))
    application_process = clean_dash_to_null(raw_json.get("plcyAplyMthdCn"))

    # core.policy_eligibility (추가 필드)
    eligibility_additional = clean_dash_to_null(raw_json.get("addAplyQlfcCndCn"))
    eligibility_restrictive = clean_dash_to_null(raw_json.get("ptcpPrpTrgtCn"))
    restrict_education = set_education_restriction(raw_json.get("schoolCd", ""))
    restrict_major = set_major_restriction(raw_json.get("plcyMajorCd", ""))
    restrict_job_status = set_job_status_restriction(raw_json.get("jobCd", ""))
    restrict_specialization = set_specialization_restriction(raw_json.get("sbizCd", ""))

    


    return NormalizedPolicy(
        id=id,
        ext_id=ext_id,
        ext_source=ext_source,
        title=title,
        summary_raw=summary_raw,
        description_raw=description_raw,
        summary_ai=summary_ai,
        # status=status, # status는 update_policy_status.py에서 별도 처리
        apply_start=apply_start,
        apply_end=apply_end,
        last_external_modified=last_external_modified,
        views=views,
        supervising_org=supervising_org,
        operating_org=operating_org,
        apply_url=apply_url,
        ref_url_1=ref_url_1,
        ref_url_2=ref_url_2,
        payload=payload,
        content_hash=content_hash,

        subcategories=subcategories,
        educations=educations,
        job_status=job_status,
        majors=majors,
        specializations=specializations,

        keywords=keywords,
        regions=regions,

        marital_status=marital_status,
        age_min=age_min,
        age_max=age_max,
        income_type=income_type,
        income_min=income_min,
        income_max=income_max,
        income_text=income_text,

        period_type=period_type,
        period_start=period_start,
        period_end=period_end,
        period_etc=period_etc,

        apply_type=apply_type,
        announcement=announcement,
        info_etc=info_etc,
        first_external_created=first_external_created,
        required_documents=required_documents,

        application_process=application_process,

        eligibility_additional=eligibility_additional,
        eligibility_restrictive=eligibility_restrictive,
        restrict_education=restrict_education,
        restrict_major=restrict_major,
        restrict_job_status=restrict_job_status,
        restrict_specialization=restrict_specialization
    )
    
def extract_list_from_payload(payload: dict, field: str) -> list[str]:
    raw = payload.get(field)
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw if x]
    return [t for t in str(raw).split(",") if t]

def parse_modified_datetime(dt_str: str) -> datetime:
    if dt_str:
        try:
            last_external_modified = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
        except ValueError:
            last_external_modified = None
    return last_external_modified

def parse_date(dt_str: str) -> date:
    if dt_str:
        try:
            return datetime.strptime(dt_str, "%Y%m%d").date()
        except ValueError:
            return None
    return None

def set_income_type(earnCndSeCd: str) -> str:
    if earnCndSeCd == "0043001":
        return "ANY"
    elif earnCndSeCd == "0043002":
        return "RANGE"
    elif earnCndSeCd == "0043003":
        return "TEXT"
    else:
        return "UNKNOWN"
    
def set_marital_status(mrgSttsCd: str) -> str:
    if mrgSttsCd == "0055001":
        return "MARRIED"
    elif mrgSttsCd == "0055002":
        return "SINGLE"
    elif mrgSttsCd == "0055003":
        return "ANY"
    else:
        return "UNKNOWN"
    
# def set_policy_status(aplyPrdSeCd: str, aplyYmd: str) -> str: 
    # return None
### 새로 짤 코드 -> update_policy_status.py에서 별도 처리 ###
    # 1. core.policy.apply_type 확인 (ALWAYS_OPEN, PERIODIC, CLOSED, UNKNOWN)
    # 2-1. ALWAYS_OPEN이면 바로 "OPEN"
    #    - 참고) ALWAYS_OPEN인 경우: apply_start, apply_end null값임.
    # 2-2. CLOSED면 바로 "CLOSED" 반환
    #    - 참고) CLOSED인 경우: apply_start, apply_end null값임.
    # 3. PERIODIC이면 apply_start, apply_end 확인
    #    - 오늘 날짜가 apply_start와 apply_end 사이에 있으면 "OPEN"
    #    - 오늘 날짜가 apply_start 이전이면 "UPCOMING"
    #    - 오늘 날짜가 apply_end 이후면 "CLOSED"
### 기존 코드 ###
    # if aplyPrdSeCd == "0057003":
    #     return "CLOSED"
    # elif aplyPrdSeCd == "0057002":
    #     return "OPEN"
    # elif aplyPrdSeCd == "0057001":
    #     apply_start, apply_end = parse_period_field(aplyYmd)
    #     if apply_start and apply_end:
    #         today = date.today()
    #         if apply_start <= today <= apply_end:
    #             return "OPEN"
    #         else:
    #             return "CLOSED"
    #     else:
    #         return "UNKNOWN"
        
def set_period_type(bizPrdSeCd: str) -> str:
    if bizPrdSeCd == "0056001":
        return "PERIODIC"
    elif bizPrdSeCd == "0056002":
        return "ETC"
    else:
        return "UNKNOWN"
    
def set_apply_type(aplyPrdSeCd: str) -> str:
    if aplyPrdSeCd == "0057001":
        return "PERIODIC"
    elif aplyPrdSeCd == "0057002":
        return "ALWAYS_OPEN"
    elif aplyPrdSeCd == "0057003":
        return "CLOSED"
    else:
        return "UNKNOWN"

def set_education_restriction(schoolCd: str) -> bool:
    if schoolCd == "0049010":
        return False
    else:
        return True
    
def set_major_restriction(plcyMajorCd: str) -> bool:
    if plcyMajorCd == "0011009":
        return False
    else:
        return True

def set_job_status_restriction(jobCd: str) -> bool:
    if jobCd == "0013010":
        return False
    else:
        return True

def set_specialization_restriction(sbizCd: str) -> bool:
    if sbizCd == "0014010":
        return False
    else:
        return True
    
def parse_period_field(aplyYmd: str) -> tuple[Optional[date], Optional[date]]:
    # 형식 참고 - "aplyYmd": "20240823 ~ 20240913"
    try:
        parts = aplyYmd.split("~")
        if len(parts) != 2:
            return None, None
        start_str = parts[0].strip()
        end_str = parts[1].strip()
        apply_start = datetime.strptime(start_str, "%Y%m%d").date()
        apply_end = datetime.strptime(end_str, "%Y%m%d").date()
        return apply_start, apply_end
    except Exception as e:
        return None, None

def to_int_or_none(v: Any) -> Optional[int]:
    """빈 문자열/None/변환 불가 -> None, 숫자/숫자형 문자열 -> int"""
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        v = v.strip()
        if v == "":
            return None
        try:
            return int(v)
        except ValueError:
            return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None

def clean_dash_to_null(value: Any) -> Optional[str]:
    """값이 정확히 '-'이거나 빈 문자열이면 None을 반환, 그렇지 않으면 문자열로 반환"""
    if value is None:
        return None
    str_value = str(value).strip()
    if str_value == "-" or str_value == "":
        return None
    return str_value

def upsert_policy(conn: Connection, items: List[NormalizedPolicy]) -> int:
    if not items:
        return {}
    sql = text(
        """
        INSERT INTO core.policy (
            id, ext_id, ext_source, title, summary_raw, description_raw, summary_ai,
            apply_start, apply_end, last_external_modified, views,
            supervising_org, operating_org, apply_url, ref_url_1, ref_url_2,
            payload, content_hash,

            period_type, period_start, period_end, period_etc,
            apply_type, announcement, info_etc, first_external_created, required_documents, application_process
        
        ) VALUES (
            :id, :ext_id, :ext_source, :title, :summary_raw, :description_raw, :summary_ai,
            :apply_start, :apply_end, :last_external_modified, :views,
            :supervising_org, :operating_org, :apply_url, :ref_url_1, :ref_url_2,
            :payload, :content_hash,

            :period_type, :period_start, :period_end, :period_etc,
            :apply_type, :announcement, :info_etc, :first_external_created, :required_documents, :application_process
        
        ) ON CONFLICT (id) DO UPDATE SET
            ext_id = EXCLUDED.ext_id,
            ext_source = EXCLUDED.ext_source,
            title = EXCLUDED.title,
            summary_raw = EXCLUDED.summary_raw,
            description_raw = EXCLUDED.description_raw,
            summary_ai = EXCLUDED.summary_ai,
            apply_start = EXCLUDED.apply_start,
            apply_end = EXCLUDED.apply_end,
            last_external_modified = EXCLUDED.last_external_modified,
            views = EXCLUDED.views,
            supervising_org = EXCLUDED.supervising_org,
            operating_org = EXCLUDED.operating_org,
            apply_url = EXCLUDED.apply_url,
            ref_url_1 = EXCLUDED.ref_url_1,
            ref_url_2 = EXCLUDED.ref_url_2,
            payload = EXCLUDED.payload,
            content_hash = EXCLUDED.content_hash,

            period_type = EXCLUDED.period_type,
            period_start = EXCLUDED.period_start,
            period_end = EXCLUDED.period_end,
            period_etc = EXCLUDED.period_etc,
            apply_type = EXCLUDED.apply_type,
            announcement = EXCLUDED.announcement,
            info_etc = EXCLUDED.info_etc,
            first_external_created = EXCLUDED.first_external_created,
            required_documents = EXCLUDED.required_documents,
            application_process = EXCLUDED.application_process
        """)
    params = [{
        "id": item.id,
        "ext_id": item.ext_id,
        "ext_source": item.ext_source,
        "title": item.title,
        "summary_raw": item.summary_raw,
        "description_raw": item.description_raw,
        "summary_ai": item.summary_ai,
        # "status": item.status, # status는 update_policy_status.py에서 별도 처리
        "apply_start": item.apply_start,
        "apply_end": item.apply_end,
        "last_external_modified": item.last_external_modified,
        "views": item.views,
        "supervising_org": item.supervising_org,
        "operating_org": item.operating_org,
        "apply_url": item.apply_url,
        "ref_url_1": item.ref_url_1,
        "ref_url_2": item.ref_url_2,
        "payload": json.dumps(item.payload, ensure_ascii=False),
        "content_hash": item.content_hash,

        "period_type": item.period_type,
        "period_start": item.period_start,
        "period_end": item.period_end,
        "period_etc": item.period_etc,
        "apply_type": item.apply_type,
        "announcement": item.announcement,
        "info_etc": item.info_etc,
        "first_external_created": item.first_external_created,
        "required_documents": item.required_documents,
        "application_process": item.application_process
    } for item in items]

    conn.execute(sql, params)
    conn.commit()
    return len(items)


def load_subcategory_id_map(conn: Connection) -> dict[str, int]:
    sql = text("SELECT id, name FROM master.category WHERE parent_id IS NOT NULL")
    rows = conn.execute(sql).mappings()
    return {r["name"]: r["id"] for r in rows}

def load_education_id_map(conn: Connection) -> dict[str, int]:
    sql = text("SELECT id, code FROM master.education")
    rows = conn.execute(sql).mappings()
    return {r["code"]: r["id"] for r in rows}

def load_job_status_id_map(conn: Connection) -> dict[str, int]:
    sql = text("SELECT id, code FROM master.job_status")
    rows = conn.execute(sql).mappings()
    return {r["code"]: r["id"] for r in rows}

def load_major_id_map(conn: Connection) -> dict[str, int]:
    sql = text("SELECT id, code FROM master.major")
    rows = conn.execute(sql).mappings()
    return {r["code"]: r["id"] for r in rows}

def load_specialization_id_map(conn: Connection) -> dict[str, int]:
    sql = text("SELECT id, code FROM master.specialization")
    rows = conn.execute(sql).mappings()
    return {r["code"]: r["id"] for r in rows}

def load_keyword_id_map(conn: Connection) -> dict[str, int]:
    sql = text("SELECT id, name FROM master.keyword")
    rows = conn.execute(sql).mappings()
    return {r["name"]: r["id"] for r in rows}

def load_subregion_id_map(conn: Connection) -> dict[str, int]:
    sql = text("SELECT id, zip_code FROM master.region WHERE zip_code IS NOT NULL")
    rows = conn.execute(sql).mappings()
    return {r["zip_code"]: r["id"] for r in rows}


def sync_policy_eligibility(conn: Connection, items: List[Any]) -> Dict[str, int]:
    """
    NormalizedPolicy 값을 '있는 그대로' upsert.
    - 문자열 필드: 그대로 사용
    - 숫자 필드: 최소한의 int 캐스팅만 수행(실패/빈값 -> NULL)
    - 반환 형식 통일: {"inserted": X, "deleted": 0, "unknown": Y}
        * deleted: 본 함수에서는 삭제 로직이 없으므로 항상 0
        * unknown: updated + skipped (업데이트되었거나 policy_id 미존재 등으로 건너뛴 건수)
    - 트랜잭션은 호출자 관리
    """
    sql = text("""
        INSERT INTO core.policy_eligibility
            (policy_id, marital_status, age_min, age_max, income_type, income_min, income_max, income_text,
             eligibility_additional, eligibility_restrictive, restrict_education, restrict_major, restrict_job_status, restrict_specialization)
        VALUES
            (:policy_id, :marital_status, :age_min, :age_max, :income_type, :income_min, :income_max, :income_text,
             :eligibility_additional, :eligibility_restrictive, :restrict_education, :restrict_major, :restrict_job_status, :restrict_specialization)
        ON CONFLICT (policy_id) DO UPDATE
        SET
            marital_status = EXCLUDED.marital_status,
            age_min        = EXCLUDED.age_min,
            age_max        = EXCLUDED.age_max,
            income_type    = EXCLUDED.income_type,
            income_min     = EXCLUDED.income_min,
            income_max     = EXCLUDED.income_max,
            income_text    = EXCLUDED.income_text,
               
            eligibility_additional  = EXCLUDED.eligibility_additional,
            eligibility_restrictive = EXCLUDED.eligibility_restrictive,

            restrict_education      = EXCLUDED.restrict_education,
            restrict_major          = EXCLUDED.restrict_major,
            restrict_job_status     = EXCLUDED.restrict_job_status,
            restrict_specialization = EXCLUDED.restrict_specialization

        RETURNING (xmax = 0) AS inserted
    """)

    inserted = updated = skipped = 0

    for it in items:
        pid = to_int_or_none(getattr(it, "id", None))
        if pid is None:
            skipped += 1
            continue

        params = {
            "policy_id": pid,
            "marital_status": getattr(it, "marital_status", None),  # 그대로
            "age_min": to_int_or_none(getattr(it, "age_min", None)),
            "age_max": to_int_or_none(getattr(it, "age_max", None)),
            "income_type": getattr(it, "income_type", None),        # 그대로
            "income_min": to_int_or_none(getattr(it, "income_min", None)),
            "income_max": to_int_or_none(getattr(it, "income_max", None)),
            "income_text": getattr(it, "income_text", None),        # 그대로

            "eligibility_additional": getattr(it, "eligibility_additional", None),
            "eligibility_restrictive": getattr(it, "eligibility_restrictive", None),

            "restrict_education": getattr(it, "restrict_education", None),
            "restrict_major": getattr(it, "restrict_major", None),
            "restrict_job_status": getattr(it, "restrict_job_status", None),
            "restrict_specialization": getattr(it, "restrict_specialization", None),
        }

        row = conn.execute(sql, params).fetchone()
        if row and row[0] is True:
            inserted += 1
        else:
            updated += 1

    deleted = 0
    unknown = updated + skipped

    conn.commit()
    return {"inserted": inserted, "deleted": deleted, "unknown": unknown}

def sync_policy_region(
    conn: Connection,
    items: List["NormalizedPolicy"],
    *,
    batch_size: int = 20000,
    show_progress: bool = True,
) -> Dict[str, Any]:
    if not items:
        return {"inserted": 0, "deleted": 0, "unknown": set()}

    phase = tqdm(total=5, desc="policy_region phases", disable=not show_progress)

    # 0) region map 로드
    region_map = load_subregion_id_map(conn)
    phase.update(1)

    # 1) 아이템 스캔
    target_pairs: List[Dict[str, Any]] = []
    policy_ids: List[Dict[str, Any]] = []
    unknown: Set[str] = set()

    scan_bar = tqdm(items, desc="scan items", disable=not show_progress)
    for item in scan_bar:
        pid = getattr(item, "id", None)
        if pid is None:
            continue
        policy_ids.append({"policy_id": pid})
        for r in (getattr(item, "regions", []) or []):
            rid = region_map.get(r)
            if rid:
                target_pairs.append({"policy_id": pid, "region_id": rid})
            else:
                unknown.add(r)
    phase.update(1)

    # 2) 임시테이블 생성
    conn.execute(text("CREATE TEMP TABLE tmp_policy (policy_id TEXT) ON COMMIT DROP"))
    conn.execute(text("CREATE TEMP TABLE tmp_policy_region (policy_id TEXT, region_id BIGINT) ON COMMIT DROP"))
    phase.update(1)

    # 3) 임시테이블 적재 (배치 + 진행바)
    if policy_ids:
        bar1 = tqdm(total=len(policy_ids), desc="load tmp_policy", disable=not show_progress)
        for chunk in _chunked(policy_ids, batch_size):
            conn.execute(text("INSERT INTO tmp_policy(policy_id) VALUES (:policy_id)"), chunk)
            bar1.update(len(chunk))
        bar1.close()

    if target_pairs:
        bar2 = tqdm(total=len(target_pairs), desc="load tmp_policy_region", disable=not show_progress)
        for chunk in _chunked(target_pairs, batch_size):
            conn.execute(
                text("INSERT INTO tmp_policy_region(policy_id, region_id) VALUES (:policy_id, :region_id)"),
                chunk,
            )
            bar2.update(len(chunk))
        bar2.close()
    phase.update(1)

    # 4) 본 동기화(INSERT / DELETE)
    #  - 기존 쿼리는 그대로 유지
    r1 = conn.execute(text("""
        INSERT INTO core.policy_region(policy_id, region_id)
        SELECT t.policy_id, t.region_id
        FROM tmp_policy_region t
        LEFT JOIN core.policy_region pe
          ON pe.policy_id = t.policy_id AND pe.region_id = t.region_id
        WHERE pe.policy_id IS NULL
    """))
    inserted = r1.rowcount or 0

    r2 = conn.execute(text("""
        DELETE FROM core.policy_region pe
        USING tmp_policy p
        WHERE pe.policy_id = p.policy_id
          AND NOT EXISTS (
              SELECT 1 FROM tmp_policy_region t
              WHERE t.policy_id = pe.policy_id
                AND t.region_id = pe.region_id
          )
    """))
    deleted = r2.rowcount or 0

    conn.commit()
    phase.update(1)
    try:
        phase.close()
    except Exception:
        pass

    if unknown:
        print(f"⚠️ Unknown regions not in master: {sorted(unknown)}")

    return {"inserted": inserted, "deleted": deleted, "unknown": unknown}

def sync_policy_keywords(conn: Connection, items: list[NormalizedPolicy]) -> dict:
    if not items:
        return {"inserted": 0, "deleted": 0, "unknown": set()}

    keyword_map = load_keyword_id_map(conn)

    target_pairs, policy_ids, unknown = [], [], set()
    for item in items:
        policy_ids.append({"policy_id": item.id})
        for k in item.keywords:
            kid = keyword_map.get(k)
            if kid:
                target_pairs.append({"policy_id": item.id, "keyword_id": kid})
            else:
                unknown.add(k)

    conn.execute(text("CREATE TEMP TABLE tmp_policy (policy_id TEXT) ON COMMIT DROP"))
    conn.execute(text("CREATE TEMP TABLE tmp_policy_keyword (policy_id TEXT, keyword_id BIGINT) ON COMMIT DROP"))

    if policy_ids:
        conn.execute(text("INSERT INTO tmp_policy(policy_id) VALUES (:policy_id)"), policy_ids)
    if target_pairs:
        conn.execute(
            text("INSERT INTO tmp_policy_keyword(policy_id, keyword_id) VALUES (:policy_id, :keyword_id)"),
            target_pairs
        )
    r1 = conn.execute(text("""
        INSERT INTO core.policy_keyword(policy_id, keyword_id)
        SELECT t.policy_id, t.keyword_id
        FROM tmp_policy_keyword t
        LEFT JOIN core.policy_keyword pe
          ON pe.policy_id = t.policy_id AND pe.keyword_id = t.keyword_id
        WHERE pe.policy_id IS NULL
    """))
    inserted = r1.rowcount or 0
    r2 = conn.execute(text("""
        DELETE FROM core.policy_keyword pe
        USING tmp_policy p
        WHERE pe.policy_id = p.policy_id
          AND NOT EXISTS (
              SELECT 1 FROM tmp_policy_keyword t
              WHERE t.policy_id = pe.policy_id
                AND t.keyword_id = pe.keyword_id
          )
    """))
    deleted = r2.rowcount or 0

    conn.commit()
    if unknown:
        print(f"⚠️ Unknown keywords not in master: {sorted(unknown)}")

    return {"inserted": inserted, "deleted": deleted, "unknown": unknown}

def sync_policy_eligibility_major(conn: Connection, items: list[NormalizedPolicy]) -> dict:
    if not items:
        return {"inserted": 0, "deleted": 0, "unknown": set()}

    major_map = load_major_id_map(conn)

    target_pairs, policy_ids, unknown = [], [], set()
    for item in items:
        policy_ids.append({"policy_id": item.id})
        for m in item.majors:
            mid = major_map.get(m)
            if mid:
                target_pairs.append({"policy_id": item.id, "major_id": mid})
            else:
                unknown.add(m)

    conn.execute(text("CREATE TEMP TABLE tmp_policy (policy_id TEXT) ON COMMIT DROP"))
    conn.execute(text("CREATE TEMP TABLE tmp_policy_eligibility_major (policy_id TEXT, major_id BIGINT) ON COMMIT DROP"))

    if policy_ids:
        conn.execute(text("INSERT INTO tmp_policy(policy_id) VALUES (:policy_id)"), policy_ids)
    if target_pairs:
        conn.execute(
            text("INSERT INTO tmp_policy_eligibility_major(policy_id, major_id) VALUES (:policy_id, :major_id)"),
            target_pairs
        )
    r1 = conn.execute(text("""
        INSERT INTO core.policy_eligibility_major(policy_id, major_id)
        SELECT t.policy_id, t.major_id
        FROM tmp_policy_eligibility_major t
        LEFT JOIN core.policy_eligibility_major pe
          ON pe.policy_id = t.policy_id AND pe.major_id = t.major_id
        WHERE pe.policy_id IS NULL
    """))
    inserted = r1.rowcount or 0
    r2 = conn.execute(text("""
        DELETE FROM core.policy_eligibility_major pe
        USING tmp_policy p
        WHERE pe.policy_id = p.policy_id
          AND NOT EXISTS (
              SELECT 1 FROM tmp_policy_eligibility_major t
              WHERE t.policy_id = pe.policy_id
                AND t.major_id = pe.major_id
          )
    """))
    deleted = r2.rowcount or 0

    conn.commit()
    if unknown:
        print(f"⚠️ Unknown majors not in master: {sorted(unknown)}")

    return {"inserted": inserted, "deleted": deleted, "unknown": unknown}

def sync_policy_eligibility_specialization(conn: Connection, items: list[NormalizedPolicy]) -> dict:
    if not items:
        return {"inserted": 0, "deleted": 0, "unknown": set()}

    spec_map = load_specialization_id_map(conn)

    target_pairs, policy_ids, unknown = [], [], set()
    for item in items:
        policy_ids.append({"policy_id": item.id})
        for s in item.specializations:
            sid = spec_map.get(s)
            if sid:
                target_pairs.append({"policy_id": item.id, "specialization_id": sid})
            else:
                unknown.add(s)

    conn.execute(text("CREATE TEMP TABLE tmp_policy (policy_id TEXT) ON COMMIT DROP"))
    conn.execute(text("CREATE TEMP TABLE tmp_policy_eligibility_specialization (policy_id TEXT, specialization_id BIGINT) ON COMMIT DROP"))

    if policy_ids:
        conn.execute(text("INSERT INTO tmp_policy(policy_id) VALUES (:policy_id)"), policy_ids)
    if target_pairs:
        conn.execute(
            text("INSERT INTO tmp_policy_eligibility_specialization(policy_id, specialization_id) VALUES (:policy_id, :specialization_id)"),
            target_pairs
        )
    r1 = conn.execute(text("""
        INSERT INTO core.policy_eligibility_specialization(policy_id, specialization_id)
        SELECT t.policy_id, t.specialization_id
        FROM tmp_policy_eligibility_specialization t
        LEFT JOIN core.policy_eligibility_specialization pe
          ON pe.policy_id = t.policy_id AND pe.specialization_id = t.specialization_id
        WHERE pe.policy_id IS NULL
    """))
    inserted = r1.rowcount or 0
    r2 = conn.execute(text("""
        DELETE FROM core.policy_eligibility_specialization pe
        USING tmp_policy p
        WHERE pe.policy_id = p.policy_id
          AND NOT EXISTS (
              SELECT 1 FROM tmp_policy_eligibility_specialization t
              WHERE t.policy_id = pe.policy_id
                AND t.specialization_id = pe.specialization_id
          )
    """))
    deleted = r2.rowcount or 0

    conn.commit()
    if unknown:
        print(f"⚠️ Unknown specializations not in master: {sorted(unknown)}")

    return {"inserted": inserted, "deleted": deleted, "unknown": unknown}

def sync_policy_eligibility_job_status(conn: Connection, items: list[NormalizedPolicy]) -> dict:
    if not items:
        return {"inserted": 0, "deleted": 0, "unknown": set()}

    job_map = load_job_status_id_map(conn)

    target_pairs, policy_ids, unknown = [], [], set()
    for item in items:
        policy_ids.append({"policy_id": item.id})
        for j in item.job_status:
            jid = job_map.get(j)
            if jid:
                target_pairs.append({"policy_id": item.id, "job_status_id": jid})
            else:
                unknown.add(j)

    conn.execute(text("CREATE TEMP TABLE tmp_policy (policy_id TEXT) ON COMMIT DROP"))
    conn.execute(text("CREATE TEMP TABLE tmp_policy_eligibility_job_status (policy_id TEXT, job_status_id BIGINT) ON COMMIT DROP"))

    if policy_ids:
        conn.execute(text("INSERT INTO tmp_policy(policy_id) VALUES (:policy_id)"), policy_ids)
    if target_pairs:
        conn.execute(
            text("INSERT INTO tmp_policy_eligibility_job_status(policy_id, job_status_id) VALUES (:policy_id, :job_status_id)"),
            target_pairs
        )
    r1 = conn.execute(text("""
        INSERT INTO core.policy_eligibility_job_status(policy_id, job_status_id)
        SELECT t.policy_id, t.job_status_id
        FROM tmp_policy_eligibility_job_status t
        LEFT JOIN core.policy_eligibility_job_status pe
          ON pe.policy_id = t.policy_id AND pe.job_status_id = t.job_status_id
        WHERE pe.policy_id IS NULL
    """))
    inserted = r1.rowcount or 0
    r2 = conn.execute(text("""
        DELETE FROM core.policy_eligibility_job_status pe
        USING tmp_policy p
        WHERE pe.policy_id = p.policy_id
          AND NOT EXISTS (
              SELECT 1 FROM tmp_policy_eligibility_job_status t
              WHERE t.policy_id = pe.policy_id
                AND t.job_status_id = pe.job_status_id
          )
    """))
    deleted = r2.rowcount or 0

    conn.commit()
    if unknown:
        print(f"⚠️ Unknown job statuses not in master: {sorted(unknown)}")

    return {"inserted": inserted, "deleted": deleted, "unknown": unknown}

def sync_policy_eligibility_education(conn: Connection, items: list[NormalizedPolicy]) -> dict:
    if not items:
        return {"inserted": 0, "deleted": 0, "unknown": set()}

    edu_map = load_education_id_map(conn)

    target_pairs, policy_ids, unknown = [], [], set()
    for item in items:
        policy_ids.append({"policy_id": item.id})
        for e in item.educations:
            eid = edu_map.get(e)
            if eid:
                target_pairs.append({"policy_id": item.id, "education_id": eid})
            else:
                unknown.add(e)

    conn.execute(text("CREATE TEMP TABLE tmp_policy (policy_id TEXT) ON COMMIT DROP"))
    conn.execute(text("CREATE TEMP TABLE tmp_policy_eligibility_education (policy_id TEXT, education_id BIGINT) ON COMMIT DROP"))

    if policy_ids:
        conn.execute(text("INSERT INTO tmp_policy(policy_id) VALUES (:policy_id)"), policy_ids)
    if target_pairs:
        conn.execute(
            text("INSERT INTO tmp_policy_eligibility_education(policy_id, education_id) VALUES (:policy_id, :education_id)"),
            target_pairs
        )
    r1 = conn.execute(text("""
        INSERT INTO core.policy_eligibility_education(policy_id, education_id)
        SELECT t.policy_id, t.education_id
        FROM tmp_policy_eligibility_education t
        LEFT JOIN core.policy_eligibility_education pe
          ON pe.policy_id = t.policy_id AND pe.education_id = t.education_id
        WHERE pe.policy_id IS NULL
    """))
    inserted = r1.rowcount or 0 
    r2 = conn.execute(text("""
        DELETE FROM core.policy_eligibility_education pe
        USING tmp_policy p
        WHERE pe.policy_id = p.policy_id
          AND NOT EXISTS (
              SELECT 1 FROM tmp_policy_eligibility_education t
              WHERE t.policy_id = pe.policy_id
                AND t.education_id = pe.education_id
          )
    """))
    deleted = r2.rowcount or 0

    conn.commit()
    if unknown:
        print(f"⚠️ Unknown educations not in master: {sorted(unknown)}")

    return {"inserted": inserted, "deleted": deleted, "unknown": unknown}

def sync_policy_category(conn: Connection, items: list[NormalizedPolicy]) -> dict:
    if not items:
        return {"inserted": 0, "deleted": 0, "unknown": set()}

    cat_map = load_subcategory_id_map(conn)

    target_pairs, policy_ids, unknown = [], [], set()
    for item in items:
        policy_ids.append({"policy_id": item.id})
        for c in item.subcategories:
            cid = cat_map.get(c)
            if cid:
                target_pairs.append({"policy_id": item.id, "category_id": cid})
            else:
                unknown.add(c)

    conn.execute(text("CREATE TEMP TABLE tmp_policy (policy_id TEXT) ON COMMIT DROP"))
    conn.execute(text("CREATE TEMP TABLE tmp_policy_category (policy_id TEXT, category_id BIGINT) ON COMMIT DROP"))

    if policy_ids:
        conn.execute(text("INSERT INTO tmp_policy(policy_id) VALUES (:policy_id)"), policy_ids)
    if target_pairs:
        conn.execute(
            text("INSERT INTO tmp_policy_category(policy_id, category_id) VALUES (:policy_id, :category_id)"),
            target_pairs
        )

    r1 = conn.execute(text("""
        INSERT INTO core.policy_category(policy_id, category_id)
        SELECT t.policy_id, t.category_id
        FROM tmp_policy_category t
        LEFT JOIN core.policy_category pc
          ON pc.policy_id = t.policy_id AND pc.category_id = t.category_id
        WHERE pc.policy_id IS NULL
    """))
    inserted = r1.rowcount or 0

    r2 = conn.execute(text("""
        DELETE FROM core.policy_category pc
        USING tmp_policy p
        WHERE pc.policy_id = p.policy_id
          AND NOT EXISTS (
              SELECT 1 FROM tmp_policy_category t
              WHERE t.policy_id = pc.policy_id
                AND t.category_id = pc.category_id
          )
    """))
    deleted = r2.rowcount or 0

    conn.commit()
    if unknown:
        print(f"⚠️ Unknown subcategories not in master: {sorted(unknown)}")

    return {"inserted": inserted, "deleted": deleted, "unknown": unknown}

def run_etl():

    # 1. 엔진 연결 및 DB 연결 테스트
    engine = get_engine()
    test_connection(engine)

    # 2. 변경된 정책 가져오기 (raw_rows)
    with engine.connect() as conn:
        raw_rows = fetch_changed_rows(conn)
        print(f"✅ Fetched {len(raw_rows)} changed/new policies.")
        if DEBUG: pprint(raw_rows[:1])

    # 3. raw_rows -> items (Policy 객체 리스트) 변환
    items = [normalize_row(r) for r in raw_rows]
    print(f"✅ Normalized {len(items)} policies into Policy objects.")
    if DEBUG and items:
        print("✅ Sample normalized policy:")
        pprint(items[0])
    if not items:
        print("❌ No new or changed policies to process. ETL finished.")
        return

    # 4. Policy 객체 리스트를 DB에 저장 (upsert)
    with engine.connect() as conn:
        n = upsert_policy(conn, items)
        print(f"✅ Upserted {n} policies into core.policy.")

    # 5-1. policy_category 동기화
    with engine.connect() as conn:
        result = sync_policy_category(conn, items)
        print(f"✅ policy_category sync -> +{result['inserted']} / -{result['deleted']}")

    # 5-2. policy_eligibility_education 동기화
    with engine.connect() as conn:
        result = sync_policy_eligibility_education(conn, items)
        print(f"✅ policy_eligibility_education sync -> +{result['inserted']} / -{result['deleted']}")

    # 5-3. policy_eligibility_job_status 동기화
    with engine.connect() as conn:
        result = sync_policy_eligibility_job_status(conn, items)
        print(f"✅ policy_eligibility_job_status sync -> +{result['inserted']} / -{result['deleted']}")

    # 5-4. policy_eligibility_major 동기화
    with engine.connect() as conn:
        result = sync_policy_eligibility_major(conn, items)
        print(f"✅ policy_eligibility_major sync -> +{result['inserted']} / -{result['deleted']}")

    # 5-5. policy_eligibility_specialization 동기화
    with engine.connect() as conn:
        result = sync_policy_eligibility_specialization(conn, items)
        print(f"✅ policy_eligibility_specialization sync -> +{result['inserted']} / -{result['deleted']}")
        
    # 5-6. policy_keyword 동기화
    with engine.connect() as conn:
        result = sync_policy_keywords(conn, items)
        print(f"✅ policy_keyword sync -> +{result['inserted']} / -{result['deleted']}")
    
    # 5-7. policy_region 동기화
    with engine.connect() as conn:
        result = sync_policy_region(conn, items)
        print(f"✅ policy_region sync -> +{result['inserted']} / -{result['deleted']}")

    # 5-8. policy_eligibility 동기화
    with engine.connect() as conn:
        result = sync_policy_eligibility(conn, items)
        print(f"✅ policy_eligibility sync -> +{result['inserted']} / -{result['deleted']}")

if __name__ == "__main__":
    run_etl()
    
