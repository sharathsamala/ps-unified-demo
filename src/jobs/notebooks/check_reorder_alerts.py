# Databricks notebook source
# MAGIC %md
# MAGIC # Check Reorder Alerts
# MAGIC
# MAGIC Counts urgent reorder alerts (shortfall > 10) and sets `alert_count` as a
# MAGIC task value. The downstream `alerts_branch` condition task uses that value
# MAGIC to route execution.

# COMMAND ----------

dbutils.widgets.text("catalog", "partssource_demo", "Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

alert_count = spark.sql(
    f"""
    SELECT COUNT(*) AS n
    FROM {CATALOG}.parts.gold_reorder_needed
    WHERE shortfall_qty > 10
    """
).collect()[0]["n"]

print(f"reorder alert_count (shortfall > 10): {alert_count}")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="alert_count", value=int(alert_count))
