"""Shared Auto Loader helpers for every domain pipeline.

The raw volume lives at `/Volumes/<catalog>/raw/landing/<dataset>/*`.
Catalog is passed via pipeline configuration as `ps.catalog`.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def _volume(dataset):
    catalog = spark.conf.get("ps.catalog")
    return f"/Volumes/{catalog}/raw/landing/{dataset}"


def read_csv(dataset):
    """Auto Loader reading sharded CSVs from the dataset directory."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"/Volumes/{spark.conf.get('ps.catalog')}/raw/landing/_schemas/{dataset}")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued")
        .load(_volume(dataset))
    )


def read_json(dataset):
    """Auto Loader reading newline-delimited JSON from the dataset directory."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"/Volumes/{spark.conf.get('ps.catalog')}/raw/landing/_schemas/{dataset}")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued")
        .load(_volume(dataset))
    )
