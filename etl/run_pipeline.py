import os
from pathlib import Path
from dotenv import load_dotenv
from etl.db import get_conn

import sqlparse


load_dotenv()

def run_sql_file(path: str):
    sql_path = Path(path)
    sql_text = sql_path.read_text(encoding="utf-8")

    conn = get_conn()
    try:
        # ✅ Everything in one transaction
        with conn:
            with conn.cursor() as cur:
                # use sqlparse to split SQL into separate statements
                statements = sqlparse.split(sql_text)

                for i, stmt in enumerate(statements, start=1):
                    stmt = stmt.strip()
                    if not stmt:
                        continue
                    try:
                        cur.execute(stmt)
                    except Exception as e:
                        # rollback happens automatically due to `with conn:`
                        print("\n" + "="*70)
                        print(f"❌ DDL failed in file: {sql_path}")
                        print(f"❌ Statement #{i} failed:\n{stmt[:800]}")
                        print(f"\n❌ Error: {e}")
                        print("="*70 + "\n")
                        raise
    finally:
        conn.close()

def main():
    # 1) Ensure tables exist
    run_sql_file("scripts/bronze/ddl_bronze.sql")
    # run_sql_file("scripts/silver/ddl_silver.sql")
    # run_sql_file("scripts/gold/ddl_gold.sql")

    # # 2) Extract + load bronze
    # total = 0
    # offset = 0
    # limit = 5000

    # while True:
    #     data = fetch_permits(limit=limit, offset=offset)
    #     if not data:
    #         break
    #     total += load_raw(data)
    #     offset += limit

    #     # safety stop for early testing
    #     if offset >= 20000:
    #         break

    # print(f"Loaded {total} raw records into bronze.")

    # 3) Transform to silver/gold (you’ll implement these SQL scripts next)
    # run_sql_file("scripts/silver/proc_load_silver.sql")
    # run_sql_file("scripts/gold/proc_load_gold.sql")

    # 4) Run tests
    # run_sql_file("tests/quality_checks_bronze.sql")
    # run_sql_file("tests/quality_checks_silver.sql")
    # run_sql_file("tests/quality_checks_gold.sql")

if __name__ == "__main__":
    main()
