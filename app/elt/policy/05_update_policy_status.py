"""
update_policy_status.py
-----------------------
매일 정기적으로 실행되어 core.policy.status를 오늘 날짜 기준으로 갱신합니다.

상태 규칙:
- apply_type = 'ALWAYS_OPEN' → 'OPEN'
- apply_type = 'CLOSED'      → 'CLOSED'
- apply_type = 'PERIODIC' → apply_start / apply_end 기준으로
    * apply_start <= 오늘 <= apply_end → 'OPEN'
    * 오늘 < apply_start → 'UPCOMING'
    * 오늘 > apply_end → 'CLOSED'
- 그 외 / NULL → 'UNKNOWN'
"""

import os
from datetime import date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


def update_policy_status():
    today = date.today()

    engine = create_engine(DATABASE_URL, future=True)
    with engine.begin() as conn:
        sql = text("""
            UPDATE core.policy
            SET status = CASE
                WHEN apply_type = 'ALWAYS_OPEN' THEN 'OPEN'
                WHEN apply_type = 'CLOSED' THEN 'CLOSED'
                WHEN apply_type = 'PERIODIC'
                     AND apply_start IS NOT NULL AND apply_end IS NOT NULL THEN
                    CASE
                        WHEN :today BETWEEN apply_start AND apply_end THEN 'OPEN'
                        WHEN :today < apply_start THEN 'UPCOMING'
                        WHEN :today > apply_end THEN 'CLOSED'
                        ELSE 'UNKNOWN'
                    END
                ELSE 'UNKNOWN'
            END
        """)

        result = conn.execute(sql, {"today": today})
        print(f"✅ Updated {result.rowcount} policies' status for {today}")


if __name__ == "__main__":
    try:
        update_policy_status()
        print("🎯 Policy status refresh completed successfully.")
    except Exception as e:
        print(f"❌ Error while updating policy status: {e}")