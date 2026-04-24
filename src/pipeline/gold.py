"""Gold layer — business-facing marts + a reverse-ETL table for Lakebase sync.

All tables publish to the pipeline's single target schema.
"""

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F


@dlt.table(
    name="gold_supplier_performance",
    comment="Supplier scorecard — on-time rate, defect PPM, net spend over time",
)
def gold_supplier_performance():
    suppliers = dlt.read("silver_suppliers")
    purchases = dlt.read("silver_purchases")
    agg = (
        purchases.groupBy("supplier_id")
        .agg(
            F.sum("total_spend_usd").alias("total_spend_usd"),
            F.count("purchase_id").alias("purchase_count"),
            F.avg("net_price_usd").alias("avg_net_price_usd"),
            F.max("purchased_at").alias("last_purchase_at"),
        )
    )
    return (
        suppliers.join(agg, "supplier_id", "left")
        .withColumn(
            "composite_score",
            (F.col("on_time_rate") * 0.6)
            + (F.lit(1) - F.col("defect_rate_ppm") / F.lit(5000)) * 0.4,
        )
        .select(
            "supplier_id", "name", "region", "tier",
            "on_time_rate", "defect_rate_ppm",
            "total_spend_usd", "purchase_count", "avg_net_price_usd",
            "last_purchase_at", "composite_score",
        )
    )


@dlt.table(
    name="gold_part_demand_monthly",
    comment="Monthly work-order volume per part — feeds demand forecasting",
)
def gold_part_demand_monthly():
    wo = dlt.read("silver_work_orders")
    parts = dlt.read("silver_parts")
    agg = (
        wo.withColumn("month", F.date_trunc("month", "opened_at"))
          .groupBy("part_id", "month")
          .agg(
              F.count("work_order_id").alias("work_orders"),
              F.avg("duration_hours").alias("avg_duration_hours"),
          )
    )
    return agg.join(parts.select("part_id", "sku", "name", "category", "manufacturer"),
                    "part_id", "left")


@dlt.table(
    name="gold_part_pricing_benchmark",
    comment="List price vs best-actual-paid price — surfaces savings opportunities",
)
def gold_part_pricing_benchmark():
    parts = dlt.read("silver_parts")
    purchases = dlt.read("silver_purchases")
    best = (
        purchases.groupBy("part_id")
        .agg(
            F.min("net_price_usd").alias("best_paid_usd"),
            F.avg("net_price_usd").alias("avg_paid_usd"),
            F.max("purchased_at").alias("last_purchase_at"),
        )
    )
    return (
        parts.join(best, "part_id", "left")
        .withColumn(
            "savings_vs_list_pct",
            (F.col("list_price_usd") - F.col("avg_paid_usd"))
            / F.col("list_price_usd"),
        )
        .select(
            "part_id", "sku", "name", "category", "manufacturer",
            "list_price_usd", "best_paid_usd", "avg_paid_usd",
            "savings_vs_list_pct", "last_purchase_at",
        )
    )


@dlt.table(
    name="gold_reorder_recommendations",
    comment="Parts needing reorder + best supplier by composite score",
)
def gold_reorder_recommendations():
    inv = dlt.read("silver_inventory").filter("needs_reorder = true")
    parts = dlt.read("silver_parts")
    purchases = dlt.read("silver_purchases")
    perf = dlt.read("gold_supplier_performance")

    w = Window.partitionBy("part_id").orderBy(F.desc("composite_score"))
    part_supplier = (
        purchases.select("part_id", "supplier_id").distinct()
        .join(perf, "supplier_id")
        .withColumn("rank", F.row_number().over(w))
        .filter("rank = 1")
        .select(
            "part_id",
            F.col("supplier_id").alias("recommended_supplier_id"),
            F.col("name").alias("recommended_supplier_name"),
        )
    )

    return (
        inv.join(parts.select("part_id", "sku", "name", "category"), "part_id", "left")
           .join(part_supplier, "part_id", "left")
           .select(
               "part_id", "sku", "name", "category", "warehouse",
               "on_hand_qty", "reorder_point",
               "recommended_supplier_id", "recommended_supplier_name",
               F.current_timestamp().alias("generated_at"),
           )
    )


@dlt.table(
    name="reverse_etl_operational_ranks",
    comment="Reverse-ETL target — synced to Lakebase to power the ops app",
)
def reverse_etl_operational_ranks():
    pricing = dlt.read("gold_part_pricing_benchmark")
    reco = dlt.read("gold_reorder_recommendations")
    return (
        pricing.join(
            reco.select("part_id", "recommended_supplier_id",
                        "recommended_supplier_name", "on_hand_qty", "warehouse"),
            "part_id", "left",
        )
        .withColumn("generated_at", F.current_timestamp())
    )
