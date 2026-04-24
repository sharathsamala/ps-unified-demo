"""
Seed synthetic PartsSource-shaped data into a UC volume.

Generates CSVs for:
  - parts          (medical replacement parts catalog)
  - suppliers      (supplier directory)
  - work_orders    (service/repair orders against a part)
  - inventory      (on-hand + location)
  - purchases      (historical buys — price benchmarking)
  - customers      (hospital / facility buyers)

Runs as the first task of the setup job. Idempotent — regenerates every run.
"""

import argparse
import csv
import io
import random
from datetime import datetime, timedelta

from databricks.sdk import WorkspaceClient

N_PARTS = 50_000
N_SUPPLIERS = 200
N_CUSTOMERS = 1_500
N_WORK_ORDERS = 25_000
N_PURCHASES = 120_000
N_INVENTORY_ROWS = 35_000

random.seed(42)

CATEGORIES = [
    "Imaging", "Patient Monitoring", "Infusion", "Ventilator", "Surgical",
    "Laboratory", "Dialysis", "Endoscopy", "Cardiology", "Sterilization",
]
MANUFACTURERS = [
    "GE Healthcare", "Philips", "Siemens Healthineers", "Medtronic", "Baxter",
    "Fresenius", "Olympus", "Stryker", "Hill-Rom", "B. Braun",
]
REGIONS = ["NE", "SE", "MW", "SW", "W"]
HOSPITAL_TYPES = ["Academic", "Community", "Specialty", "IDN", "ASC"]


def _cents(lo, hi):
    return round(random.uniform(lo, hi), 2)


def _iso(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def gen_parts():
    rows = [("part_id", "sku", "name", "category", "manufacturer",
             "list_price_usd", "weight_kg", "hazmat_flag", "created_at")]
    for i in range(1, N_PARTS + 1):
        cat = random.choice(CATEGORIES)
        mfr = random.choice(MANUFACTURERS)
        rows.append((
            i,
            f"PS-{cat[:3].upper()}-{i:06d}",
            f"{mfr} {cat} Part {i}",
            cat,
            mfr,
            _cents(25, 18_000),
            round(random.uniform(0.05, 45.0), 2),
            random.choices([0, 1], weights=[97, 3])[0],
            _iso(datetime(2019, 1, 1) + timedelta(days=random.randint(0, 2400))),
        ))
    return rows


def gen_suppliers():
    rows = [("supplier_id", "name", "region", "tier", "on_time_rate",
             "defect_rate_ppm", "tax_id", "contact_email", "active")]
    for i in range(1, N_SUPPLIERS + 1):
        rows.append((
            i,
            f"Supplier {i:03d} {random.choice(['Medical', 'Devices', 'Parts', 'Systems', 'Group'])}",
            random.choice(REGIONS),
            random.choice(["Preferred", "Approved", "Probation"]),
            round(random.uniform(0.82, 0.995), 3),
            random.randint(50, 4000),
            f"{random.randint(10,99)}-{random.randint(1000000, 9999999)}",
            f"ops{i}@supplier{i}.example.com",
            random.choices([1, 0], weights=[93, 7])[0],
        ))
    return rows


def gen_customers():
    rows = [("customer_id", "facility_name", "type", "region", "beds",
             "ssn_last4_contact", "contract_tier")]
    for i in range(1, N_CUSTOMERS + 1):
        rows.append((
            i,
            f"{random.choice(['St.', 'Mercy', 'Valley', 'Regional', 'University'])} "
            f"{random.choice(['Medical Center', 'Health', 'Hospital', 'Clinic'])} {i}",
            random.choice(HOSPITAL_TYPES),
            random.choice(REGIONS),
            random.randint(30, 900),
            f"{random.randint(1000, 9999)}",  # PII — masked downstream
            random.choice(["Gold", "Silver", "Bronze"]),
        ))
    return rows


def gen_work_orders():
    rows = [("work_order_id", "part_id", "customer_id", "opened_at",
             "closed_at", "status", "priority", "technician_notes")]
    for i in range(1, N_WORK_ORDERS + 1):
        opened = datetime(2025, 10, 1) + timedelta(
            days=random.randint(0, 200), hours=random.randint(0, 23)
        )
        closed = opened + timedelta(hours=random.randint(2, 240))
        status = random.choices(
            ["closed", "in_progress", "open", "cancelled"],
            weights=[70, 18, 10, 2],
        )[0]
        rows.append((
            i,
            random.randint(1, N_PARTS),
            random.randint(1, N_CUSTOMERS),
            _iso(opened),
            _iso(closed) if status == "closed" else "",
            status,
            random.choice(["P1", "P2", "P3", "P4"]),
            f"Replaced unit {i}",
        ))
    return rows


def gen_purchases():
    rows = [("purchase_id", "part_id", "supplier_id", "qty", "unit_price_usd",
             "purchased_at", "discount_pct")]
    for i in range(1, N_PURCHASES + 1):
        rows.append((
            i,
            random.randint(1, N_PARTS),
            random.randint(1, N_SUPPLIERS),
            random.randint(1, 25),
            _cents(20, 15_000),
            _iso(datetime(2023, 1, 1) + timedelta(days=random.randint(0, 1200))),
            round(random.uniform(0, 0.35), 3),
        ))
    return rows


def gen_inventory():
    rows = [("inventory_id", "part_id", "warehouse", "on_hand_qty",
             "reorder_point", "updated_at")]
    for i in range(1, N_INVENTORY_ROWS + 1):
        rows.append((
            i,
            random.randint(1, N_PARTS),
            random.choice(["CLE-01", "CHI-02", "DAL-03", "ATL-04", "LAX-05"]),
            random.randint(0, 500),
            random.randint(5, 100),
            _iso(datetime(2026, 4, 1) + timedelta(hours=random.randint(0, 500))),
        ))
    return rows


def _write_csv(rows, path):
    buf = io.StringIO()
    writer = csv.writer(buf)
    for r in rows:
        writer.writerow(r)
    w = WorkspaceClient()
    w.files.upload(
        file_path=path,
        contents=buf.getvalue().encode("utf-8"),
        overwrite=True,
    )
    print(f"  wrote {path} ({len(rows) - 1:,} rows)")


def generate_all(catalog):
    """Seed all datasets into /Volumes/{catalog}/raw/landing/{dataset}/{dataset}.csv."""
    volume_path = f"/Volumes/{catalog}/raw/landing"
    print(f"Seeding into {volume_path}")
    # Each dataset goes in its own subdirectory — Auto Loader (cloudFiles) requires
    # the `load()` argument to be a directory, not a file path.
    # Re-runs are safe: upload(overwrite=True) replaces CSVs in place, and the
    # pipeline does full_refresh each run (wipes its checkpoint + rebuilds tables).
    datasets = {
        "parts": gen_parts(),
        "suppliers": gen_suppliers(),
        "customers": gen_customers(),
        "work_orders": gen_work_orders(),
        "purchases": gen_purchases(),
        "inventory": gen_inventory(),
    }
    for name, rows in datasets.items():
        _write_csv(rows, f"{volume_path}/{name}/{name}.csv")
    print("Seed complete.")


def _cli_main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    args, _ = parser.parse_known_args()
    generate_all(args.catalog)


if __name__ == "__main__":
    _cli_main()
