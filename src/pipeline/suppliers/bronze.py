"""suppliers bronze — raw Auto Loader ingest from the landing volume."""

import dlt
from _shared import read_csv, read_json


@dlt.table(
    name="bronze_suppliers",
    comment="Raw supplier records (JSON, sharded). Schema inferred by Auto Loader.",
    table_properties={"domain": "suppliers", "layer": "bronze"},
)
def bronze_suppliers():
    return read_json("suppliers")


@dlt.table(
    name="bronze_purchases",
    comment="Raw purchases (CSV, sharded). One row per buy.",
    table_properties={"domain": "suppliers", "layer": "bronze"},
)
def bronze_purchases():
    return read_csv("purchases")
