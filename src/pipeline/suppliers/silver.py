"""suppliers silver — typed, cleaned, expectations enforced."""

import dlt
from pyspark.sql.functions import col, to_timestamp


@dlt.table(
    name="silver_suppliers",
    comment="Cleaned suppliers — drops rows with out-of-range on_time_rate.",
    table_properties={"domain": "suppliers", "layer": "silver"},
)
@dlt.expect_or_drop("on_time_rate_in_range", "on_time_rate >= 0 AND on_time_rate <= 1")
@dlt.expect("defect_rate_ppm_nonneg", "defect_rate_ppm >= 0")
@dlt.expect("active_flag_known", "active IN (0, 1)")
def silver_suppliers():
    return (
        dlt.read("bronze_suppliers")
        .select(
            col("supplier_id").cast("bigint"),
            col("name").alias("supplier_name"),
            col("region"),
            col("tier"),
            col("on_time_rate").cast("double"),
            col("defect_rate_ppm").cast("int"),
            col("tax_id"),
            col("contact_email"),
            col("active").cast("int"),
        )
    )


@dlt.table(
    name="silver_purchases",
    comment="Cleaned purchases — net price computed; qty must be positive.",
    table_properties={"domain": "suppliers", "layer": "silver"},
)
@dlt.expect_or_fail("qty_positive", "qty > 0")
@dlt.expect("unit_price_positive", "unit_price_usd > 0")
@dlt.expect("discount_in_range", "discount_pct >= 0 AND discount_pct <= 1")
def silver_purchases():
    return (
        dlt.read("bronze_purchases")
        .select(
            col("purchase_id").cast("bigint"),
            col("part_id").cast("bigint"),
            col("supplier_id").cast("bigint"),
            col("qty").cast("int"),
            col("unit_price_usd").cast("double"),
            (col("unit_price_usd").cast("double") * (1 - col("discount_pct").cast("double")))
            .alias("net_unit_price_usd"),
            to_timestamp(col("purchased_at")).alias("purchased_at"),
            col("discount_pct").cast("double"),
        )
    )
