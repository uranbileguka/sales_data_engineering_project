"""Generate salesperson roster and salesperson-sales mapping CSVs from a sales_details.csv

By default the script reads the sales source from:
    datasets/source_crm/sales_details.csv
and writes outputs to:
    datasets/source_marketing/

Outputs created in the output directory:
 - salesperson.csv              (salesperson_id,name,region,email)
 - salesperson_sales.csv        (salesperson_id,sls_ord_num)
 - optionally, per-salesperson files under sales_by_salesperson/

Usage:
    python scripts/generate_salesperson_files.py [--input-path <csv>] [--output-dir <dir>] [--roster N] [--per-salesperson]

Notes:
 - If `<output-dir>/salesperson.csv` exists it will be used; otherwise a sample roster of N salespeople is generated and written there.
 - Assignment is deterministic by hashing `sls_cust_id`; this keeps assignments stable between runs.
"""
import argparse
import csv
import hashlib
import shutil
import time
from pathlib import Path
from typing import List, Dict

ROOT = Path(__file__).resolve().parents[2]

# Defaults:
#  - input: original sales file in source_crm (where your sales data currently lives)
#  - output: source_marketing (per your request earlier)
DEFAULT_INPUT = ROOT / "datasets" / "source_crm" / "sales_details.csv"
DEFAULT_OUTPUT_DIR = ROOT / "datasets" / "source_marketing"



def load_sales(file_path: Path) -> List[Dict[str, str]]:
    with file_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def load_or_make_roster(file_path: Path, n: int) -> List[Dict[str, str]]:
    if file_path.exists():
        with file_path.open("r", newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    roster = []
    for i in range(1, n + 1):
        sid = f"SP{str(i).zfill(3)}"
        name = f"Salesperson {i:02d}"
        roster.append({"salesperson_id": sid, "name": name, "region": "N/A", "email": f"{name.replace(' ','').lower()}@example.com"})

    # write roster
    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["salesperson_id", "name", "region", "email"])
        writer.writeheader()
        writer.writerows(roster)

    return roster


def stable_assign(customer_id: str, salesperson_ids: List[str]) -> str:
    key = (customer_id or "").encode("utf-8")
    h = hashlib.md5(key).hexdigest()
    idx = int(h[:8], 16) % len(salesperson_ids)
    return salesperson_ids[idx]


def generate_files(roster: List[Dict[str, str]], sales_rows: List[Dict[str, str]], per_salesperson: bool, map_file: Path, per_dir: Path, backup: bool = False, append: bool = False):
    sp_ids = [r["salesperson_id"] for r in roster]

    # ensure output dir exists
    map_file.parent.mkdir(parents=True, exist_ok=True)

    # backup existing mapping if requested and not appending
    if backup and map_file.exists() and not append:
        ts = int(time.time())
        bak = map_file.with_name(map_file.name + f".bak.{ts}")
        shutil.copy2(map_file, bak)

    # if appending, load existing pairs to avoid duplicates
    existing = set()
    if append and map_file.exists():
        with map_file.open("r", newline="", encoding="utf-8") as mf:
            reader = csv.reader(mf)
            try:
                header = next(reader)
            except StopIteration:
                header = None
            for r in reader:
                if not r:
                    continue
                existing.add((r[0], r[1]))

    mode = "a" if append and map_file.exists() else "w"
    write_header = True
    if mode == "a":
        # if appending to existing file, don't write header
        write_header = False

    # write mapping file
    with map_file.open(mode, newline="", encoding="utf-8") as mf:
        writer = csv.writer(mf)
        if write_header:
            writer.writerow(["salesperson_id", "sls_ord_num"])

        # optionally prepare per-salesperson files map
        per_files = {}
        if per_salesperson:
            per_dir.mkdir(parents=True, exist_ok=True)

        for row in sales_rows:
            cust = row.get("sls_cust_id")
            ord_num = row.get("sls_ord_num")
            sp = stable_assign(cust, sp_ids)

            if (sp, ord_num) in existing:
                continue

            writer.writerow([sp, ord_num])
            existing.add((sp, ord_num))

            if per_salesperson:
                pf = per_dir / f"{sp}.csv"
                # open per-salesperson file in append if exists and append requested, else write new
                if append and pf.exists():
                    pw = csv.writer(pf.open("a", newline="", encoding="utf-8"))
                else:
                    pw = csv.writer(pf.open("w", newline="", encoding="utf-8"))
                    pw.writerow(["sls_ord_num"])
                pw.writerow([ord_num])

        # close per-salesperson files
        if per_salesperson:
            # files were opened and closed per-write; nothing to close here
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--roster", type=int, default=10, help="Number of sample salespeople to generate if roster missing")
    parser.add_argument("--per-salesperson", action="store_true", help="Also create per-salesperson CSVs under sales_by_salesperson/")
    parser.add_argument("--input-path", default=str(DEFAULT_INPUT), help="Path to input sales_details.csv")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory to write salesperson files into")
    parser.add_argument("--backup", action="store_true", help="Backup existing mapping file before overwrite")
    parser.add_argument("--append", action="store_true", help="Append new mappings to existing mapping file (avoid duplicates)")
    args = parser.parse_args()
    SALES_FILE = Path(args.input_path)
    OUT_DIR = Path(args.output_dir)
    SP_FILE = OUT_DIR / "salesperson.csv"
    MAP_FILE = OUT_DIR / "salesperson_sales.csv"
    PER_DIR = OUT_DIR / "sales_by_salesperson"

    if not SALES_FILE.exists():
        print(f"Sales file not found: {SALES_FILE}")
        return

    sales_rows = load_sales(SALES_FILE)
    print(f"Loaded {len(sales_rows)} sales rows from {SALES_FILE}")

    # ensure output directory exists so roster can be created
    SP_FILE.parent.mkdir(parents=True, exist_ok=True)
    roster = load_or_make_roster(SP_FILE, args.roster)
    print(f"Using {len(roster)} salespeople (roster at: {SP_FILE})")

    generate_files(roster, sales_rows, args.per_salesperson, MAP_FILE, PER_DIR, backup=args.backup, append=args.append)
    print(f"Wrote mapping file: {MAP_FILE}")
    if args.per_salesperson:
        print(f"Wrote per-salesperson files to: {PER_DIR}")


if __name__ == "__main__":
    main()
