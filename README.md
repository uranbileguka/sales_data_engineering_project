# WORCESTER_HOUSING_PSQL_DATA_ENGINEERING_PROJECT
# 1) Start database
docker compose up -d

# 2) Create python env + install deps
python3 -m venv .venv
source .venv/bin/activate
pip install -r etl/requirements.txt

# 3) Run pipeline
python etl/run_pipeline.py
