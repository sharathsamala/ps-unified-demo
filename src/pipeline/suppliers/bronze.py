"""suppliers bronze — raw Auto Loader ingest from the landing volume."""

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
    name="bronze_suppliers",
    comment="Raw supplier records (JSON, sharded). Schema inferred by Auto Loader.",
    table_properties={"domain": "suppliers", "layer": "bronze"},
)
def bronze_suppliers():
    return _read("suppliers", "json")


@dlt.table(
    name="bronze_purchases",
    comment="Raw purchases (CSV, sharded). One row per buy.",
    table_properties={"domain": "suppliers", "layer": "bronze"},
)
def bronze_purchases():
    return _read("purchases", "csv")
