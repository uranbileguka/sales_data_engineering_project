"""Generate discount roster and sales->discount mapping CSVs from a sales_details.csv

By default the script reads the sales source from:
  datasets/source_crm/sales_details.csv
and writes outputs to:
  datasets/source_marketing/

Outputs created in the output directory:
 - discount_info.csv            (discount_id,description,percent,active)
 - sales_discount.csv           (discount_id,sls_ord_num[,sls_sales])
 - optionally, per-discount files under discounts_by_discount/

Usage:
  python scripts/generate_discounts.py [--input-path <csv>] [--output-dir <dir>] [--discount-count N] [--per-discount]

Notes:
 - If `<output-dir>/discount_info.csv` exists it will be used; otherwise a sample roster of N discounts is generated and written there.
 - Assignment is deterministic by hashing `sls_ord_num` so repeated runs are stable.
"""
import argparse
import csv
import hashlib
import shutil
import time
from pathlib import Path
from typing import List, Dict

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = ROOT / "datasets" / "source_crm" / "sales_details.csv"
DEFAULT_OUTPUT_DIR = ROOT / "datasets" / "source_marketing"


def load_sales(file_path: Path) -> List[Dict[str, str]]:
    with file_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def load_or_make_discounts(file_path: Path, n: int) -> List[Dict[str, str]]:
    if file_path.exists():
        with file_path.open("r", newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    roster = []
    # create a sample set of discounts (percent values distributed)
    percent_values = [5, 10, 15, 20, 25]
    for i in range(1, n + 1):
        did = f"D{str(i).zfill(3)}"
        pct = percent_values[(i - 1) % len(percent_values)]
        roster.append({"discount_id": did, "description": f"Promo {pct}% off", "percent": str(pct), "active": "true"})

    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["discount_id", "description", "percent", "active"])
        writer.writeheader()
        writer.writerows(roster)

    return roster


def stable_assign(key_value: str, ids: List[str]) -> str:
    key = (key_value or "").encode("utf-8")
    h = hashlib.md5(key).hexdigest()
    idx = int(h[:8], 16) % len(ids)
    return ids[idx]


def generate_files(discounts: List[Dict[str, str]], sales_rows: List[Dict[str, str]], per_discount: bool, map_file: Path, per_dir: Path, include_amount: bool, backup: bool = False, append: bool = False):
    disc_ids = [d["discount_id"] for d in discounts]
    map_file.parent.mkdir(parents=True, exist_ok=True)

    # backup existing mapping if requested and not appending
    if backup and map_file.exists() and not append:
        ts = int(time.time())
        bak = map_file.with_name(map_file.name + f".bak.{ts}")
        shutil.copy2(map_file, bak)

    # load existing pairs if appending
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
        write_header = False

    with map_file.open(mode, newline="", encoding="utf-8") as mf:
        if include_amount:
            writer = csv.writer(mf)
            if write_header:
                writer.writerow(["discount_id", "sls_ord_num", "sls_sales"])
        else:
            writer = csv.writer(mf)
            if write_header:
                writer.writerow(["discount_id", "sls_ord_num"])

        per_files = {}
        if per_discount:
            per_dir.mkdir(parents=True, exist_ok=True)

        for row in sales_rows:
            ord_num = row.get("sls_ord_num")
            sales_amt = row.get("sls_sales")
            disc = stable_assign(ord_num, disc_ids)

            if (disc, ord_num) in existing:
                continue

            if include_amount:
                writer.writerow([disc, ord_num, sales_amt])
            else:
                writer.writerow([disc, ord_num])

            existing.add((disc, ord_num))

            if per_discount:
                pf = per_dir / f"{disc}.csv"
                if append and pf.exists():
                    pw = csv.writer(pf.open("a", newline="", encoding="utf-8"))
                else:
                    pw = csv.writer(pf.open("w", newline="", encoding="utf-8"))
                    if include_amount:
                        pw.writerow(["sls_ord_num", "sls_sales"])
                    else:
                        pw.writerow(["sls_ord_num"])
                if include_amount:
                    pw.writerow([ord_num, sales_amt])
                else:
                    pw.writerow([ord_num])

        if per_discount:
            # files opened per write, nothing to close here
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--discount-count", type=int, default=5, help="Number of sample discounts to generate if discount_info.csv is missing")
    parser.add_argument("--per-discount", action="store_true", help="Also create per-discount CSVs under discounts_by_discount/")
    parser.add_argument("--include-amount", action="store_true", help="Include sls_sales amount in the mapping file")
    parser.add_argument("--input-path", default=str(DEFAULT_INPUT), help="Path to input sales_details.csv")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory to write discount files into")
    parser.add_argument("--backup", action="store_true", help="Backup existing mapping file before overwrite")
    parser.add_argument("--append", action="store_true", help="Append new mappings to existing mapping file (avoid duplicates)")
    args = parser.parse_args()

    SALES_FILE = Path(args.input_path)
    OUT_DIR = Path(args.output_dir)
    DISC_FILE = OUT_DIR / "discount_info.csv"
    MAP_FILE = OUT_DIR / "sales_discount.csv"
    PER_DIR = OUT_DIR / "discounts_by_discount"

    if not SALES_FILE.exists():
        print(f"Sales file not found: {SALES_FILE}")
        return

    sales_rows = load_sales(SALES_FILE)
    print(f"Loaded {len(sales_rows)} sales rows from {SALES_FILE}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    discounts = load_or_make_discounts(DISC_FILE, args.discount_count)
    print(f"Using {len(discounts)} discounts (discount roster at: {DISC_FILE})")

    generate_files(discounts, sales_rows, args.per_discount, MAP_FILE, PER_DIR, args.include_amount, backup=args.backup, append=args.append)
    print(f"Wrote mapping file: {MAP_FILE}")
    if args.per_discount:
        print(f"Wrote per-discount files to: {PER_DIR}")


if __name__ == "__main__":
    main()
