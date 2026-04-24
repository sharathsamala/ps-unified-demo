"""
Seed synthetic PartsSource data into the raw landing volume.

Layout (mixed formats, sharded so Auto Loader sees multiple files per dataset):

  /Volumes/<catalog>/raw/landing/
    parts/parts_{0001..0005}.csv            (CSV)
    purchases/purchases_{0001..0008}.csv    (CSV)
    work_orders/work_orders_{0001..0005}.csv (CSV)
    suppliers/suppliers_{0001..0003}.json   (JSON, newline-delimited)
    customers/customers_{0001..0004}.json   (JSON, newline-delimited)
    inventory/inventory_{0001..0003}.json   (JSON, newline-delimited)

Each dataset intentionally carries a small fraction of defects that the SDP
pipelines' expectations catch:
  - suppliers:   5% have on_time_rate out of [0,1]   -> expect_or_drop
  - parts:       3% have list_price_usd <= 0          -> expect_or_drop
  - work_orders: 2% have closed_at < opened_at        -> expect (warn-only)
  - purchases:   0.5% have qty <= 0                   -> expect_or_fail (soft)
"""

import argparse
import csv
import io
import json
import random
from datetime import datetime, timedelta

from databricks.sdk import WorkspaceClient

N_PARTS = 50_000
N_SUPPLIERS = 200
N_CUSTOMERS = 1_500
N_WORK_ORDERS = 25_000
N_PURCHASES = 120_000
N_INVENTORY_ROWS = 35_000

SHARDS_PARTS = 5
SHARDS_PURCHASES = 8
SHARDS_WORK_ORDERS = 5
SHARDS_SUPPLIERS = 3
SHARDS_CUSTOMERS = 4
SHARDS_INVENTORY = 3

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
    """CSV rows. 3% of records have list_price_usd = 0 (DQ defect)."""
    for i in range(1, N_PARTS + 1):
        cat = random.choice(CATEGORIES)
        mfr = random.choice(MANUFACTURERS)
        defect = random.random() < 0.03
        yield {
            "part_id": i,
            "sku": f"PS-{cat[:3].upper()}-{i:06d}",
            "name": f"{mfr} {cat} Part {i}",
            "category": cat,
            "manufacturer": mfr,
            "list_price_usd": 0.0 if defect else _cents(25, 18_000),
            "weight_kg": round(random.uniform(0.05, 45.0), 2),
            "hazmat_flag": random.choices([0, 1], weights=[97, 3])[0],
            "created_at": _iso(datetime(2019, 1, 1) + timedelta(days=random.randint(0, 2400))),
        }


def gen_suppliers():
    """JSON records. 5% have out-of-range on_time_rate (DQ defect)."""
    for i in range(1, N_SUPPLIERS + 1):
        defect = random.random() < 0.05
        if defect:
            otr = random.choice([-0.1, 1.4])
        else:
            otr = round(random.uniform(0.82, 0.995), 3)
        yield {
            "supplier_id": i,
            "name": f"Supplier {i:03d} {random.choice(['Medical', 'Devices', 'Parts', 'Systems', 'Group'])}",
            "region": random.choice(REGIONS),
            "tier": random.choice(["Preferred", "Approved", "Probation"]),
            "on_time_rate": otr,
            "defect_rate_ppm": random.randint(50, 4000),
            "tax_id": f"{random.randint(10, 99)}-{random.randint(1000000, 9999999)}",
            "contact_email": f"ops{i}@supplier{i}.example.com",
            "active": random.choices([1, 0], weights=[93, 7])[0],
        }


def gen_customers():
    """JSON records. No defects injected here."""
    for i in range(1, N_CUSTOMERS + 1):
        yield {
            "customer_id": i,
            "facility_name": (
                f"{random.choice(['St.', 'Mercy', 'Valley', 'Regional', 'University'])} "
                f"{random.choice(['Medical Center', 'Health', 'Hospital', 'Clinic'])} {i}"
            ),
            "type": random.choice(HOSPITAL_TYPES),
            "region": random.choice(REGIONS),
            "beds": random.randint(30, 900),
            "ssn_last4_contact": f"{random.randint(1000, 9999)}",
            "contract_tier": random.choice(["Gold", "Silver", "Bronze"]),
        }


def gen_work_orders():
    """CSV rows. 2% have closed_at < opened_at (DQ defect — warn-only expectation)."""
    for i in range(1, N_WORK_ORDERS + 1):
        opened = datetime(2025, 10, 1) + timedelta(
            days=random.randint(0, 200), hours=random.randint(0, 23)
        )
        status = random.choices(
            ["closed", "in_progress", "open", "cancelled"],
            weights=[70, 18, 10, 2],
        )[0]
        defect = status == "closed" and random.random() < 0.02
        if defect:
            closed = opened - timedelta(hours=random.randint(1, 48))
        else:
            closed = opened + timedelta(hours=random.randint(2, 240))
        yield {
            "work_order_id": i,
            "part_id": random.randint(1, N_PARTS),
            "customer_id": random.randint(1, N_CUSTOMERS),
            "opened_at": _iso(opened),
            "closed_at": _iso(closed) if status == "closed" else "",
            "status": status,
            "priority": random.choice(["P1", "P2", "P3", "P4"]),
            "technician_notes": f"Replaced unit {i}",
        }


def gen_purchases():
    """CSV rows. 0.5% have qty <= 0 (DQ defect — hard-fail expectation, kept under threshold)."""
    for i in range(1, N_PURCHASES + 1):
        defect = random.random() < 0.005
        yield {
            "purchase_id": i,
            "part_id": random.randint(1, N_PARTS),
            "supplier_id": random.randint(1, N_SUPPLIERS),
            "qty": -1 if defect else random.randint(1, 25),
            "unit_price_usd": _cents(20, 15_000),
            "purchased_at": _iso(datetime(2023, 1, 1) + timedelta(days=random.randint(0, 1200))),
            "discount_pct": round(random.uniform(0, 0.35), 3),
        }


def gen_inventory():
    """JSON records. No defects."""
    for i in range(1, N_INVENTORY_ROWS + 1):
        yield {
            "inventory_id": i,
            "part_id": random.randint(1, N_PARTS),
            "warehouse": random.choice(["CLE-01", "CHI-02", "DAL-03", "ATL-04", "LAX-05"]),
            "on_hand_qty": random.randint(0, 500),
            "reorder_point": random.randint(5, 100),
            "updated_at": _iso(datetime(2026, 4, 1) + timedelta(hours=random.randint(0, 500))),
        }


def _upload(w, path, payload):
    w.files.upload(file_path=path, contents=payload, overwrite=True)


def _write_csv_shards(w, rows_iter, dataset, volume_path, n_shards):
    """Round-robin rows across `n_shards` CSV files under <volume_path>/<dataset>/."""
    rows = list(rows_iter)
    if not rows:
        return
    header = list(rows[0].keys())
    per_shard = (len(rows) + n_shards - 1) // n_shards
    total = 0
    for shard in range(n_shards):
        start = shard * per_shard
        end = min(start + per_shard, len(rows))
        if start >= end:
            break
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=header)
        writer.writeheader()
        for r in rows[start:end]:
            writer.writerow(r)
        file_path = f"{volume_path}/{dataset}/{dataset}_{shard+1:04d}.csv"
        _upload(w, file_path, buf.getvalue().encode("utf-8"))
        total += (end - start)
        print(f"  wrote {file_path} ({end - start:,} rows)")
    print(f"  {dataset}: {total:,} rows across {n_shards} shards")


def _write_json_shards(w, rows_iter, dataset, volume_path, n_shards):
    """Round-robin rows across `n_shards` newline-delimited JSON files."""
    rows = list(rows_iter)
    if not rows:
        return
    per_shard = (len(rows) + n_shards - 1) // n_shards
    total = 0
    for shard in range(n_shards):
        start = shard * per_shard
        end = min(start + per_shard, len(rows))
        if start >= end:
            break
        payload = "\n".join(json.dumps(r) for r in rows[start:end]) + "\n"
        file_path = f"{volume_path}/{dataset}/{dataset}_{shard+1:04d}.json"
        _upload(w, file_path, payload.encode("utf-8"))
        total += (end - start)
        print(f"  wrote {file_path} ({end - start:,} rows)")
    print(f"  {dataset}: {total:,} rows across {n_shards} shards")


def generate_all(catalog):
    """Seed the six datasets with DQ defects into the raw landing volume."""
    w = WorkspaceClient()
    volume_path = f"/Volumes/{catalog}/raw/landing"
    print(f"Seeding into {volume_path}")

    print("parts (CSV, sharded)")
    _write_csv_shards(w, gen_parts(), "parts", volume_path, SHARDS_PARTS)

    print("purchases (CSV, sharded)")
    _write_csv_shards(w, gen_purchases(), "purchases", volume_path, SHARDS_PURCHASES)

    print("work_orders (CSV, sharded)")
    _write_csv_shards(w, gen_work_orders(), "work_orders", volume_path, SHARDS_WORK_ORDERS)

    print("suppliers (JSON, sharded)")
    _write_json_shards(w, gen_suppliers(), "suppliers", volume_path, SHARDS_SUPPLIERS)

    print("customers (JSON, sharded)")
    _write_json_shards(w, gen_customers(), "customers", volume_path, SHARDS_CUSTOMERS)

    print("inventory (JSON, sharded)")
    _write_json_shards(w, gen_inventory(), "inventory", volume_path, SHARDS_INVENTORY)

    print("Seed complete.")


def _cli_main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    args, _ = parser.parse_known_args()
    generate_all(args.catalog)


if __name__ == "__main__":
    _cli_main()
