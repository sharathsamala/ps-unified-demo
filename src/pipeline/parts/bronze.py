"""parts bronze — raw ingest."""

import dlt
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
CATALOG = spark.conf.get("ps.catalog")


def _read(dataset, fmt):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaLocation", f"/Volumes/{CATALOG}/raw/landing/_schemas/{dataset}")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued")
        .load(f"/Volumes/{CATALOG}/raw/landing/{dataset}")
    )


@dlt.table(
    name="bronze_parts",
    comment="Raw parts (CSV, sharded).",
    table_properties={"domain": "parts", "layer": "bronze"},
)
def bronze_parts():
    return _read("parts", "csv")


@dlt.table(
    name="bronze_inventory",
    comment="Raw inventory on-hand snapshots (JSON, sharded).",
    table_properties={"domain": "parts", "layer": "bronze"},
)
def bronze_inventory():
    return _read("inventory", "json")
