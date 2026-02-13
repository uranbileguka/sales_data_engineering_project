# Sales Data Engineering Project

## Business Problem

Sales data is spread across CRM, ERP, and marketing systems, making reporting inconsistent and slow.
This project builds a unified analytics warehouse so stakeholders can track product performance, customer trends, and discount effectiveness from a trusted Gold layer.

## Tech Stack

- PostgreSQL
- Python
- SQL
- Docker / Docker Compose
- pandas
- psycopg2-binary
- python-dotenv
- sqlparse

## Data Engineering Scope

- Built end-to-end ETL pipeline from CSV sources into PostgreSQL.
- Implemented layered architecture: Bronze (raw), Silver (cleaned/standardized), Gold (analytics-ready star schema views).
- Added fail-fast data quality checks using PostgreSQL assertions (`DO $$ ... RAISE EXCEPTION ... $$`).
- Automated run flow through a single orchestration entrypoint: `python -m etl.run_pipeline`.

## Project Overview

This project builds a PostgreSQL-based sales analytics warehouse from multiple raw source systems (CRM, ERP, and marketing files).
The pipeline ingests source CSVs into a layered data model:

- Bronze: raw ingested data with minimal transformation
- Silver: cleaned and standardized business entities
- Gold: analytics-ready tables for reporting and downstream BI use

The implementation includes SQL DDL/procedures, Python ETL orchestration, and data quality checks.

## Architecture And Documentation

### Data Model
![Data Model](docs/data_model.png)

### Data Flow
![Data Flow](docs/data_flow.png)

### Supporting Docs
- [Data Catalog](docs/data_catalog.md)
- [Naming Conventions](docs/naming_conventions.md)

## Quick Start

### 1) Start database
```bash
docker compose up -d
```

### 2) Create Python environment and install dependencies
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r etl/requirements.txt
```

### 3) Run pipeline
```bash
python -m etl.run_pipeline
```

### Expected Outcome
- Pipeline finishes without Python or SQL exceptions.
- Silver quality checks pass.
- Gold quality checks pass.

## Key Data Quality Rules

- Duplicate or NULL primary keys are rejected in core Silver entities.
- Product cost must be non-negative and not NULL.
- Product start/end date ordering must be valid.
- Sales consistency rule enforced: `sales_amount = quantity * price`.
- Out-of-range ERP customer birthdates are normalized and validated.
- Referential integrity enforced across mapping tables (salesperson/order, discount/order).
- Gold surrogate keys are checked for uniqueness.
- Gold fact-to-dimension integrity is enforced for customer/product and optional salesperson/discount keys.

## Validation Queries (PostgreSQL)

Run these after the pipeline completes.

### 1) Row Counts In Gold Layer
```sql
SELECT 'dim_customers' AS table_name, COUNT(*) AS row_count FROM gold.dim_customers
UNION ALL
SELECT 'dim_products'  AS table_name, COUNT(*) AS row_count FROM gold.dim_products
UNION ALL
SELECT 'dim_salesperson' AS table_name, COUNT(*) AS row_count FROM gold.dim_salesperson
UNION ALL
SELECT 'dim_discount' AS table_name, COUNT(*) AS row_count FROM gold.dim_discount
UNION ALL
SELECT 'fact_sales' AS table_name, COUNT(*) AS row_count FROM gold.fact_sales;
```
![alt text](docs/image-1.png)

### 2) Sample Data In Gold Fact
```sql
SELECT *
FROM gold.fact_sales
LIMIT 20;
```
![alt text](image.png)

### 3) Top 10 Products By Sales
```sql
SELECT
    p.product_name,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_products p
  ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_sales DESC
LIMIT 10;
```
![alt text](docs/qry3.png)




## Pipeline Execution Log

Use this command to run the pipeline and save logs:

```bash
source .venv/bin/activate
python -m etl.run_pipeline 2>&1 | tee "logs/pipeline_run_$(date +%F_%H-%M-%S).log" logs/pipeline_latest.log
```

Log files:
- [Logs directory](logs/)
- [Latest pipeline log](logs/pipeline_latest.log)

Sample output:

```text
NOTICE:  Loading Silver Layer is Completed
2026-02-13 11:40:12,420 | INFO | Running SQL file: tests/quality_checks_silver.sql
2026-02-13 11:40:12,469 | INFO | Running SQL file: scripts/gold/ddl_gold.sql
2026-02-13 11:40:12,483 | INFO | Running SQL file: tests/quality_checks_gold.sql
```


## Data Source Attribution

- ERP and CRM source data used in this project includes material from Baraa Khatib Salkini (MIT License).
- Full license notice: [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)

## Project Outcomes

- Source domains integrated: CRM, ERP, Marketing
- Layers delivered: Bronze, Silver, Gold
- Gold model delivered: 4 dimensions + 1 fact view
- Automated quality checks: Silver + Gold fail-fast assertions
- Typical full pipeline runtime: < 1 minute on local setup

## Improvements Roadmap

- Add workflow scheduler/orchestrator (Airflow/Prefect) for productionized runs.
- Add CI pipeline to run ETL smoke tests and quality checks on each commit.
- Add dbt or equivalent semantic/data test layer for model documentation and lineage.
- Add incremental load strategy with watermarks and audit metadata.


## Project Structure

```text
.
├── datasets/
│   ├── source_crm/
│   ├── source_erp/
│   └── source_marketing/
├── docs/
│   ├── data_catalog.md
│   ├── data_flow.png
│   ├── data_model.png
│   ├── dataflow.xml
│   ├── gold_data_model.xml
│   └── naming_conventions.md
├── etl/
│   ├── db.py
│   ├── load_bronze.py
│   ├── load_silver.py
│   ├── read_csv.py
│   ├── requirements.txt
│   ├── run_pipeline.py
│   └── utils/
├── scripts/
│   ├── bronze/
│   ├── generators/
│   ├── gold/
│   ├── silver/
│   └── init_database.sql
├── tests/
└── docker-compose.yml
```
