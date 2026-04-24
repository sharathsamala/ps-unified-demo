"""service_ops bronze — raw ingest."""

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
    name="bronze_customers",
    comment="Raw customers / facilities (JSON, sharded).",
    table_properties={"domain": "service_ops", "layer": "bronze"},
)
def bronze_customers():
    return _read("customers", "json")


@dlt.table(
    name="bronze_work_orders",
    comment="Raw work orders (CSV, sharded).",
    table_properties={"domain": "service_ops", "layer": "bronze"},
)
def bronze_work_orders():
    return _read("work_orders", "csv")
