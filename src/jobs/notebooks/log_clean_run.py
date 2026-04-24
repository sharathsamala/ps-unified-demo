# Databricks notebook source
# MAGIC %md
# MAGIC # Clean Run — No Reorder Alerts
# MAGIC
# MAGIC Fires when `check_reorder_alerts` returned `alert_count = 0` (i.e., no
# MAGIC parts are meaningfully below reorder point). We log a quick health
# MAGIC summary and move on.

# COMMAND ----------

dbutils.widgets.text("catalog", "partssource_demo", "Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

from pyspark.sql import functions as F

reorder = spark.table(f"{CATALOG}.parts.gold_reorder_needed")
suppliers = spark.table(f"{CATALOG}.suppliers.gold_supplier_performance")

print("Clean run — no actionable reorder alerts.")
print(f"  reorder_needed rows (informational only): {reorder.count():,}")
print(f"  suppliers tracked:                        {suppliers.count():,}")
print(f"  last supplier purchase:                   {suppliers.agg(F.max('last_purchase_at')).collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC Nothing to publish to reverse_etl; the operational app will see the last
# MAGIC good state until the next pipeline run.
