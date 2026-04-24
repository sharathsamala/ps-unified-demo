"""parts bronze — raw ingest."""

import dlt
from _shared import read_csv, read_json


@dlt.table(
    name="bronze_parts",
    comment="Raw parts (CSV, sharded).",
    table_properties={"domain": "parts", "layer": "bronze"},
)
def bronze_parts():
    return read_csv("parts")


@dlt.table(
    name="bronze_inventory",
    comment="Raw inventory on-hand snapshots (JSON, sharded).",
    table_properties={"domain": "parts", "layer": "bronze"},
)
def bronze_inventory():
    return read_json("inventory")
