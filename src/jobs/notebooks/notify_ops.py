# Databricks notebook source
# MAGIC %md
# MAGIC # Notify Ops — Reorder Alerts Published
# MAGIC
# MAGIC Fires only when `check_reorder_alerts` returned `alert_count > 0`. In a real
# MAGIC customer env this would trigger a Slack/email/PagerDuty notification via a
# MAGIC webhook. Here we log the top alerts so the run output shows what would be
# MAGIC sent.

# COMMAND ----------

dbutils.widgets.text("catalog", "partssource_demo", "Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F

alerts = (
    spark.table(f"{CATALOG}.reverse_etl.reorder_alerts_feed")
    .orderBy(F.col("est_reorder_value_usd").desc())
)

total_alerts = alerts.count()
total_value = alerts.agg(F.sum("est_reorder_value_usd")).collect()[0][0] or 0

print(f"Reorder alert batch published at {alerts.select(F.max('generated_at')).collect()[0][0]}")
print(f"  total alerts:                {total_alerts:,}")
print(f"  total estimated reorder $:   ${total_value:,.2f}")
print()
print("Top 10 alerts by estimated value:")

# COMMAND ----------

display(alerts.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Downstream: the `reverse_etl.reorder_alerts_feed` table is synced to Lakebase
# MAGIC by the supply-chain ops app. A separate Slack webhook task (not provisioned
# MAGIC in this demo) consumes this output and posts to `#supply-chain-ops`.
