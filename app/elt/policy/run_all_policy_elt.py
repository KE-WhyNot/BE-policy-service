#!/usr/bin/env python3
"""
run_all_policy_elt.py
- 정책 ETL (RAW → STG → CORE → UPDATE) 순차 실행
- 각 단계는 실패 시 중단, 로그 출력
"""

import subprocess
import sys
import os
from datetime import datetime

STEPS = [
    "01_raw_ingest.py",
    "02_stg_landing.py",
    "03_stg_refresh_current.py",
    "04_stg_to_core.py",
    "05_update_policy_status.py",
]

BASE_DIR = os.path.join(os.path.dirname(__file__))

def run_step(script_name: str):
    path = os.path.join(BASE_DIR, script_name)
    print(f"\n🚀 [{datetime.now().strftime('%H:%M:%S')}] Running {script_name} ...")
    try:
        subprocess.run(
            [sys.executable, path],
            check=True
        )
        print(f"✅ {script_name} completed successfully\n")
    except subprocess.CalledProcessError:
        print(f"❌ {script_name} failed. Stopping pipeline.")
        sys.exit(1)

def main():
    print("🧑‍🤝‍🧑🧑‍🤝‍🧑🧑‍🤝‍🧑 Starting Policy ELT Pipeline...")
    for script in STEPS:
        run_step(script)

    print("🎉 All policy ELT steps completed successfully.")

if __name__ == "__main__":
    main()