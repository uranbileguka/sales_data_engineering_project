import os
import logging
from dotenv import load_dotenv
from etl.load_bronze import main as load_bronze
from etl.load_silver import main as load_silver
from etl.utils.sql import run_sql_file


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

def main():
    run_sql_file("scripts/bronze/ddl_bronze.sql")
    load_bronze()

    # silver
    run_sql_file("scripts/silver/ddl_silver.sql")
    load_silver()
    
    # gold
    # run_sql_file("scripts/gold/ddl_gold.sql")
    # run_sql_file("scripts/gold/proc_load_gold.sql")

    # 4) Run tests
    # run_sql_file("tests/quality_checks_bronze.sql")
    # run_sql_file("tests/quality_checks_silver.sql")
    # run_sql_file("tests/quality_checks_gold.sql")

if __name__ == "__main__":
    main()
