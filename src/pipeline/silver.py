"""Silver layer — typed, conformed, expectation-checked.

All tables publish to the pipeline's single target schema.
"""

import dlt
from pyspark.sql import functions as F


@dlt.table(name="silver_parts", comment="Silver parts — typed + validated")
@dlt.expect_or_drop("valid_price", "list_price_usd >= 0")
@dlt.expect_or_drop("non_null_sku", "sku IS NOT NULL")
def silver_parts():
    df = dlt.read_stream("bronze_parts")
    return (
        df.withColumn("list_price_usd", F.col("list_price_usd").cast("double"))
          .withColumn("weight_kg", F.col("weight_kg").cast("double"))
          .withColumn("hazmat_flag", F.col("hazmat_flag").cast("int") == 1)
          .withColumn("created_at", F.to_timestamp("created_at"))
          .dropDuplicates(["part_id"])
    )


@dlt.table(name="silver_suppliers", comment="Silver suppliers — typed, active filter applied")
@dlt.expect_or_drop("on_time_rate_range", "on_time_rate BETWEEN 0 AND 1")
def silver_suppliers():
    df = dlt.read_stream("bronze_suppliers")
    return (
        df.withColumn("on_time_rate", F.col("on_time_rate").cast("double"))
          .withColumn("defect_rate_ppm", F.col("defect_rate_ppm").cast("int"))
          .withColumn("active", F.col("active").cast("int") == 1)
          .filter("active = true")
          .dropDuplicates(["supplier_id"])
    )


@dlt.table(name="silver_customers", comment="Silver customers — PII stays tagged for governance")
def silver_customers():
    df = dlt.read_stream("bronze_customers")
    return (
        df.withColumn("beds", F.col("beds").cast("int"))
          .dropDuplicates(["customer_id"])
    )


@dlt.table(name="silver_work_orders", comment="Silver work orders — durations computed")
@dlt.expect_or_drop("valid_status",
                    "status IN ('open', 'in_progress', 'closed', 'cancelled')")
def silver_work_orders():
    df = dlt.read_stream("bronze_work_orders")
    opened = F.to_timestamp("opened_at")
    closed = F.when(F.col("closed_at") != "", F.to_timestamp("closed_at"))
    return (
        df.withColumn("opened_at", opened)
          .withColumn("closed_at", closed)
          .withColumn(
              "duration_hours",
              F.when(
                  F.col("closed_at").isNotNull(),
                  (F.col("closed_at").cast("long") - F.col("opened_at").cast("long")) / 3600,
              ),
          )
    )


@dlt.table(name="silver_purchases", comment="Silver purchases — final price computed")
@dlt.expect_or_drop("positive_qty", "qty > 0")
@dlt.expect_or_drop("price_range", "unit_price_usd BETWEEN 0 AND 200000")
def silver_purchases():
    df = dlt.read_stream("bronze_purchases")
    return (
        df.withColumn("qty", F.col("qty").cast("int"))
          .withColumn("unit_price_usd", F.col("unit_price_usd").cast("double"))
          .withColumn("discount_pct", F.col("discount_pct").cast("double"))
          .withColumn(
              "net_price_usd",
              F.col("unit_price_usd") * (1 - F.col("discount_pct")),
          )
          .withColumn("purchased_at", F.to_timestamp("purchased_at"))
          .withColumn("total_spend_usd", F.col("qty") * F.col("net_price_usd"))
    )


@dlt.table(name="silver_inventory", comment="Silver inventory — current snapshot per (part, warehouse)")
def silver_inventory():
    df = dlt.read_stream("bronze_inventory")
    return (
        df.withColumn("on_hand_qty", F.col("on_hand_qty").cast("int"))
          .withColumn("reorder_point", F.col("reorder_point").cast("int"))
          .withColumn("updated_at", F.to_timestamp("updated_at"))
          .withColumn(
              "needs_reorder",
              F.col("on_hand_qty") <= F.col("reorder_point"),
          )
    )
