"""service_ops gold — work order volume, part demand monthly, SLA performance."""

import dlt
from pyspark.sql.functions import (
    avg, col, count, countDistinct, date_format, expr, round as sql_round, sum as sql_sum, when,
)


@dlt.table(
    name="gold_work_order_volume",
    comment="Monthly work order counts by priority and status.",
    table_properties={"domain": "service_ops", "layer": "gold", "certified": "true"},
)
def gold_work_order_volume():
    return (
        dlt.read("silver_work_orders")
        .withColumn("month", date_format(col("opened_at"), "yyyy-MM"))
        .groupBy("month", "priority", "status")
        .agg(count("work_order_id").alias("work_orders"))
    )


@dlt.table(
    name="gold_part_demand_monthly",
    comment="Monthly work-order volume per part — the demand signal.",
    table_properties={"domain": "service_ops", "layer": "gold", "certified": "true"},
)
def gold_part_demand_monthly():
    return (
        dlt.read("silver_work_orders")
        .withColumn("month", date_format(col("opened_at"), "yyyy-MM"))
        .groupBy("part_id", "month")
        .agg(
            count("work_order_id").alias("work_orders"),
            sql_round(
                avg(expr("CASE WHEN closed_at IS NOT NULL THEN (unix_timestamp(closed_at) - unix_timestamp(opened_at)) / 3600.0 END")),
                2,
            ).alias("avg_duration_hours"),
        )
    )


@dlt.table(
    name="gold_sla_performance",
    comment="SLA attainment by priority. SLA target = 48h for P1, 120h for P2, 240h for P3/P4.",
    table_properties={"domain": "service_ops", "layer": "gold", "certified": "true"},
)
def gold_sla_performance():
    closed = (
        dlt.read("silver_work_orders")
        .filter("status = 'closed' AND closed_at IS NOT NULL AND closed_at >= opened_at")
        .withColumn("duration_hours",
                    expr("(unix_timestamp(closed_at) - unix_timestamp(opened_at)) / 3600.0"))
        .withColumn("sla_target_hours",
                    expr("CASE priority WHEN 'P1' THEN 48 WHEN 'P2' THEN 120 ELSE 240 END"))
        .withColumn("met_sla", expr("duration_hours <= sla_target_hours"))
    )

    return (
        closed.groupBy("priority")
        .agg(
            count("work_order_id").alias("closed_orders"),
            sql_sum(when(col("met_sla"), 1).otherwise(0)).alias("orders_meeting_sla"),
            sql_round(avg("duration_hours"), 2).alias("avg_duration_hours"),
        )
        .withColumn("sla_pct",
                    sql_round(col("orders_meeting_sla") / col("closed_orders"), 4))
    )
