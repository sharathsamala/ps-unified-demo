"""parts silver — cleaned with expectations."""

import dlt
from pyspark.sql.functions import col, to_timestamp


@dlt.table(
    name="silver_parts",
    comment="Cleaned parts — drops rows where list_price_usd <= 0.",
    table_properties={"domain": "parts", "layer": "silver"},
)
@dlt.expect_or_drop("list_price_positive", "list_price_usd > 0")
@dlt.expect("weight_positive", "weight_kg > 0")
@dlt.expect("sku_present", "sku IS NOT NULL AND length(sku) > 0")
def silver_parts():
    return (
        dlt.read_stream("bronze_parts")
        .select(
            col("part_id").cast("bigint"),
            col("sku"),
            col("name").alias("part_name"),
            col("category"),
            col("manufacturer"),
            col("list_price_usd").cast("double"),
            col("weight_kg").cast("double"),
            col("hazmat_flag").cast("int"),
            to_timestamp(col("created_at")).alias("created_at"),
        )
    )


@dlt.table(
    name="silver_inventory",
    comment="Cleaned inventory — non-negative on-hand and reorder point.",
    table_properties={"domain": "parts", "layer": "silver"},
)
@dlt.expect("on_hand_nonneg", "on_hand_qty >= 0")
@dlt.expect("reorder_point_positive", "reorder_point > 0")
def silver_inventory():
    return (
        dlt.read_stream("bronze_inventory")
        .select(
            col("inventory_id").cast("bigint"),
            col("part_id").cast("bigint"),
            col("warehouse"),
            col("on_hand_qty").cast("int"),
            col("reorder_point").cast("int"),
            to_timestamp(col("updated_at")).alias("updated_at"),
        )
    )
