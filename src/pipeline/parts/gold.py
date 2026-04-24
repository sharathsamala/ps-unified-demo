"""parts gold — pricing benchmark + reorder recommendations.

`gold_part_pricing_benchmark` joins with the suppliers domain's silver_purchases
by reading it as a batch UC table. The job orchestrates suppliers -> parts so
that source is populated before this pipeline runs.
"""

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, expr, max as sql_max, min as sql_min, round as sql_round, when,
)

spark = SparkSession.builder.getOrCreate()


@dlt.table(
    name="gold_part_pricing_benchmark",
    comment="List vs paid price per part (savings_vs_list_pct). Sourced from silver_parts + suppliers.silver_purchases.",
    table_properties={"domain": "parts", "layer": "gold", "certified": "true"},
)
def gold_part_pricing_benchmark():
    catalog = spark.conf.get("ps.catalog")
    parts = dlt.read("silver_parts")
    purchases = spark.read.table(f"{catalog}.suppliers.silver_purchases")

    paid = (
        purchases.groupBy("part_id")
        .agg(
            avg("net_unit_price_usd").alias("avg_paid_usd"),
            sql_min("net_unit_price_usd").alias("best_paid_usd"),
            count("purchase_id").alias("purchase_count"),
            sql_max("purchased_at").alias("last_purchase_at"),
        )
    )

    return (
        parts.join(paid, "part_id", "left")
        .withColumn(
            "savings_vs_list_pct",
            when(col("list_price_usd") > 0,
                 (col("list_price_usd") - col("avg_paid_usd")) / col("list_price_usd"))
            .otherwise(None),
        )
        .select(
            "part_id", "sku", "part_name", "category", "manufacturer",
            sql_round("list_price_usd", 2).alias("list_price_usd"),
            sql_round("avg_paid_usd", 2).alias("avg_paid_usd"),
            sql_round("best_paid_usd", 2).alias("best_paid_usd"),
            "purchase_count",
            sql_round("savings_vs_list_pct", 4).alias("savings_vs_list_pct"),
            "last_purchase_at",
        )
    )


@dlt.table(
    name="gold_reorder_needed",
    comment="Parts at or below reorder point, per warehouse. Surfaces operational backlog.",
    table_properties={"domain": "parts", "layer": "gold", "certified": "true"},
)
def gold_reorder_needed():
    parts = dlt.read("silver_parts")
    inv = dlt.read("silver_inventory")

    return (
        inv.join(parts, "part_id", "left")
        .filter("on_hand_qty <= reorder_point")
        .select(
            "part_id", "sku", "part_name", "category", "manufacturer",
            "warehouse", "on_hand_qty", "reorder_point",
            sql_round("list_price_usd", 2).alias("list_price_usd"),
            expr("(reorder_point - on_hand_qty) AS shortfall_qty"),
            expr("current_timestamp() AS generated_at"),
        )
    )
