"""suppliers gold — supplier performance + spend-by-region aggregates."""

import dlt
from pyspark.sql.functions import (
    avg, countDistinct, expr, max as sql_max, round as sql_round, sum as sql_sum,
)


@dlt.table(
    name="gold_supplier_performance",
    comment="Per-supplier aggregate: total spend, composite score, on-time + defect rates.",
    table_properties={"domain": "suppliers", "layer": "gold", "certified": "true"},
)
def gold_supplier_performance():
    suppliers = dlt.read("silver_suppliers")
    purchases = dlt.read("silver_purchases")

    spend = (
        purchases.groupBy("supplier_id")
        .agg(
            sql_sum(expr("qty * net_unit_price_usd")).alias("total_spend_usd"),
            countDistinct("purchase_id").alias("purchase_count"),
            avg("net_unit_price_usd").alias("avg_net_price_usd"),
            sql_max("purchased_at").alias("last_purchase_at"),
        )
    )

    return (
        suppliers.join(spend, "supplier_id", "left")
        .withColumn(
            "composite_score",
            expr("0.6 * on_time_rate + 0.4 * (1 - defect_rate_ppm / 5000.0)"),
        )
        .select(
            "supplier_id", "supplier_name", "region", "tier",
            sql_round("composite_score", 3).alias("composite_score"),
            sql_round("on_time_rate", 3).alias("on_time_rate"),
            "defect_rate_ppm",
            sql_round("total_spend_usd", 2).alias("total_spend_usd"),
            "purchase_count",
            sql_round("avg_net_price_usd", 2).alias("avg_net_price_usd"),
            "last_purchase_at",
        )
    )


@dlt.table(
    name="gold_spend_by_region",
    comment="Total spend and supplier count rolled up by region and tier.",
    table_properties={"domain": "suppliers", "layer": "gold", "certified": "true"},
)
def gold_spend_by_region():
    return (
        dlt.read("gold_supplier_performance")
        .groupBy("region", "tier")
        .agg(
            sql_sum("total_spend_usd").alias("total_spend_usd"),
            countDistinct("supplier_id").alias("supplier_count"),
            avg("composite_score").alias("avg_composite_score"),
        )
    )
