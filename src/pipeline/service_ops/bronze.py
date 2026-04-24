"""service_ops bronze — raw ingest."""

import dlt
from _shared import read_csv, read_json


@dlt.table(
    name="bronze_customers",
    comment="Raw customers / facilities (JSON, sharded).",
    table_properties={"domain": "service_ops", "layer": "bronze"},
)
def bronze_customers():
    return read_json("customers")


@dlt.table(
    name="bronze_work_orders",
    comment="Raw work orders (CSV, sharded).",
    table_properties={"domain": "service_ops", "layer": "bronze"},
)
def bronze_work_orders():
    return read_csv("work_orders")
