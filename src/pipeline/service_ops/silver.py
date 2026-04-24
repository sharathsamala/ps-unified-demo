"""service_ops silver — cleaned customers + work orders with expectations."""

import dlt
from pyspark.sql.functions import col, to_timestamp, when


@dlt.table(
    name="silver_customers",
    comment="Cleaned customer facilities.",
    table_properties={"domain": "service_ops", "layer": "silver"},
)
@dlt.expect("beds_positive", "beds > 0")
@dlt.expect("contract_tier_known", "contract_tier IN ('Gold', 'Silver', 'Bronze')")
def silver_customers():
    return (
        dlt.read("bronze_customers")
        .select(
            col("customer_id").cast("bigint"),
            col("facility_name"),
            col("type").alias("facility_type"),
            col("region"),
            col("beds").cast("int"),
            col("ssn_last4_contact"),
            col("contract_tier"),
        )
    )


@dlt.table(
    name="silver_work_orders",
    comment="Cleaned work orders — closed_at >= opened_at is a warn-only expectation.",
    table_properties={"domain": "service_ops", "layer": "silver"},
)
@dlt.expect("closed_at_after_opened_at",
            "closed_at IS NULL OR closed_at >= opened_at")
@dlt.expect("status_known", "status IN ('closed','in_progress','open','cancelled')")
@dlt.expect("priority_known", "priority IN ('P1','P2','P3','P4')")
def silver_work_orders():
    raw = dlt.read("bronze_work_orders")
    return (
        raw.select(
            col("work_order_id").cast("bigint"),
            col("part_id").cast("bigint"),
            col("customer_id").cast("bigint"),
            to_timestamp(col("opened_at")).alias("opened_at"),
            when(col("closed_at") == "", None)
            .otherwise(to_timestamp(col("closed_at")))
            .alias("closed_at"),
            col("status"),
            col("priority"),
            col("technician_notes"),
        )
    )
