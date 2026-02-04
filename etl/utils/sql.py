from pathlib import Path
from typing import Iterable

import sqlparse

from etl.db import get_conn


def run_sql_file(path: str) -> None:
    """Run all statements in a SQL file inside a single transaction.

    This mirrors the previous implementation in `etl/run_pipeline.py` but
    is placed here for reuse.
    """
    sql_path = Path(path)
    sql_text = sql_path.read_text(encoding="utf-8")

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                statements = sqlparse.split(sql_text)

                for i, stmt in enumerate(statements, start=1):
                    stmt = stmt.strip()
                    if not stmt:
                        continue
                    try:
                        cur.execute(stmt)
                    except Exception as e:
                        # rollback happens automatically due to `with conn:`
                        print("\n" + "=" * 70)
                        print(f"❌ DDL failed in file: {sql_path}")
                        print(f"❌ Statement #{i} failed:\n{stmt[:800]}")
                        print(f"\n❌ Error: {e}")
                        print("=" * 70 + "\n")
                        raise
    finally:
        conn.close()
