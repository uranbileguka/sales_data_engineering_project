from etl.utils.sql import run_sql_file
from etl.db import get_conn

def main():
    # 1) Ensure procedure exists
    run_sql_file("scripts/silver/proc_load_silver.sql")

    # 2) Execute procedure
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("CALL silver.load_silver();")
                for notice in conn.notices:
                    print(notice.strip())
    finally:
        conn.close()

if __name__ == "__main__":
    main()
