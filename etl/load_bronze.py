import os
import time
import logging
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

from etl.db import get_conn


# --------------------------------------------------
# Load .env
# --------------------------------------------------
load_dotenv()


# --------------------------------------------------
# Logging (replaces RAISE NOTICE)
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("bronze_loader")


# --------------------------------------------------
# Table → CSV mapping
# --------------------------------------------------
TABLES = [
    ("bronze.crm_cust_info",      "source_crm/cust_info.csv"),
    ("bronze.crm_prd_info",       "source_crm/prd_info.csv"),
    ("bronze.crm_sales_details",  "source_crm/sales_details.csv"),
    ("bronze.erp_loc_info",       "source_erp/loc_info.csv"),
    ("bronze.erp_cust_info",      "source_erp/cust_info.csv"),
    ("bronze.erp_px_cat_info",    "source_erp/px_cat_info.csv"),
    ("bronze.marketing_discount_info", "source_marketing/discount_info.csv"),
    ("bronze.marketing_salesperson", "source_marketing/salesperson.csv"),
    ("bronze.marketing_salesperson_sales", "source_marketing/salesperson_sales.csv"),
    ("bronze.marketing_sales_discount", "source_marketing/sales_discount.csv"),
]


# --------------------------------------------------
# COPY helper (FASTEST)
# --------------------------------------------------
def copy_csv(cur, table_name: str, csv_path: Path):
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    with csv_path.open("r", encoding="utf-8") as f:
        cur.copy_expert(
            f"""
            COPY {table_name}
            FROM STDIN
            WITH (
                FORMAT csv,
                HEADER true,
                DELIMITER ',',
                QUOTE '"'
            )
            """,
            f,
        )


# --------------------------------------------------
# Main ETL
# --------------------------------------------------
def main():
    data_dir = Path(os.getenv("DATA_DIR", "./datasets"))

    batch_start = time.time()

    log.info("=" * 60)
    log.info("Loading Bronze Layer (CSV → PostgreSQL)")
    log.info("=" * 60)

    conn = get_conn()
    cur = conn.cursor()

    try:
        for table, rel_path in TABLES:
            csv_path = data_dir / rel_path
            start = time.time()

            log.info(f">> Truncating table: {table}")
            cur.execute(f"TRUNCATE TABLE {table};")

            log.info(f">> Loading {csv_path.name} → {table}")
            copy_csv(cur, table, csv_path)

            conn.commit()

            log.info(f">> Load Duration: {int(time.time() - start)} seconds")
            log.info(">> ----------------------------------------")

        log.info("=" * 60)
        log.info(
            f"Bronze load completed in {int(time.time() - batch_start)} seconds"
        )
        log.info("=" * 60)

    except Exception as e:
        conn.rollback()
        log.error("Error during bronze load")
        log.error(str(e))
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
