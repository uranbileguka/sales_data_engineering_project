# Sales Data Engineering Project

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
python etl/run_pipeline.py
```

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
