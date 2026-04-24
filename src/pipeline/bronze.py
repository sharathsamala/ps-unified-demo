"""Bronze layer — Lakeflow Declarative Pipeline ingest from the landing Volume.

All tables publish to the pipeline's single target schema (configured in
resources/pipeline.yml as `schema: pipeline`). Table names are prefixed with
`bronze_` to preserve the medallion shape within one schema.
"""

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("ps.catalog")
LANDING = f"/Volumes/{CATALOG}/raw/landing"


def _csv(name, schema):
    # Auto Loader expects a directory; seed writes {LANDING}/{name}/{name}.csv
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{LANDING}/_schemas/{name}")
        .schema(schema)
        .load(f"{LANDING}/{name}")
    )


@dlt.table(name="bronze_parts", comment="Bronze parts catalog — auto-loaded from Volume")
def bronze_parts():
    schema = (
        "part_id INT, sku STRING, name STRING, category STRING, manufacturer STRING, "
        "list_price_usd DOUBLE, weight_kg DOUBLE, hazmat_flag INT, created_at STRING"
    )
    return _csv("parts", schema).withColumn("_ingested_at", F.current_timestamp())


@dlt.table(name="bronze_suppliers", comment="Bronze supplier directory")
def bronze_suppliers():
    schema = (
        "supplier_id INT, name STRING, region STRING, tier STRING, on_time_rate DOUBLE, "
        "defect_rate_ppm INT, tax_id STRING, contact_email STRING, active INT"
    )
    return _csv("suppliers", schema).withColumn("_ingested_at", F.current_timestamp())


@dlt.table(name="bronze_customers", comment="Bronze customer/facility directory (contains PII)")
def bronze_customers():
    schema = (
        "customer_id INT, facility_name STRING, type STRING, region STRING, beds INT, "
        "ssn_last4_contact STRING, contract_tier STRING"
    )
    return _csv("customers", schema).withColumn("_ingested_at", F.current_timestamp())


@dlt.table(name="bronze_work_orders", comment="Bronze work order events")
def bronze_work_orders():
    schema = (
        "work_order_id INT, part_id INT, customer_id INT, opened_at STRING, "
        "closed_at STRING, status STRING, priority STRING, technician_notes STRING"
    )
    return _csv("work_orders", schema).withColumn("_ingested_at", F.current_timestamp())


@dlt.table(name="bronze_purchases", comment="Bronze purchase transactions")
def bronze_purchases():
    schema = (
        "purchase_id INT, part_id INT, supplier_id INT, qty INT, "
        "unit_price_usd DOUBLE, purchased_at STRING, discount_pct DOUBLE"
    )
    return _csv("purchases", schema).withColumn("_ingested_at", F.current_timestamp())


@dlt.table(name="bronze_inventory", comment="Bronze inventory snapshots")
def bronze_inventory():
    schema = (
        "inventory_id INT, part_id INT, warehouse STRING, on_hand_qty INT, "
        "reorder_point INT, updated_at STRING"
    )
    return _csv("inventory", schema).withColumn("_ingested_at", F.current_timestamp())
