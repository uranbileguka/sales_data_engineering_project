import os
from dotenv import load_dotenv
from etl.extract_permits import fetch_permits
from etl.load_bronze import load_raw
from etl.db import get_conn

load_dotenv()

def run_sql_file(path: str):
    conn = get_conn()
    cur = conn.cursor()
    with open(path, "r", encoding="utf-8") as f:
        cur.execute(f.read())
    conn.commit()
    cur.close()
    conn.close()

def main():
    # 1) Ensure tables exist
    run_sql_file("scripts/bronze/ddl_bronze.sql")
    run_sql_file("scripts/silver/ddl_silver.sql")
    run_sql_file("scripts/gold/ddl_gold.sql")

    # 2) Extract + load bronze
    total = 0
    offset = 0
    limit = 5000

    while True:
        data = fetch_permits(limit=limit, offset=offset)
        if not data:
            break
        total += load_raw(data)
        offset += limit

        # safety stop for early testing
        if offset >= 20000:
            break

    print(f"Loaded {total} raw records into bronze.")

    # 3) Transform to silver/gold (you’ll implement these SQL scripts next)
    # run_sql_file("scripts/silver/proc_load_silver.sql")
    # run_sql_file("scripts/gold/proc_load_gold.sql")

    # 4) Run tests
    # run_sql_file("tests/quality_checks_bronze.sql")
    # run_sql_file("tests/quality_checks_silver.sql")
    # run_sql_file("tests/quality_checks_gold.sql")

if __name__ == "__main__":
    main()
