import csv
from pathlib import Path

def read_csv_as_records(csv_path: Path):
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)
