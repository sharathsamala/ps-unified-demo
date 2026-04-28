# Databricks notebook source
# MAGIC %md
# MAGIC # Vector Search demo — PartsSource parts catalog
# MAGIC
# MAGIC Runs a handful of natural-language similarity queries against the
# MAGIC `parts.parts_searchable_index` index. The index is a Delta-sync index
# MAGIC backed by managed `databricks-gte-large-en` embeddings.
# MAGIC
# MAGIC No embeddings are computed in this notebook — Vector Search handles
# MAGIC both ingestion-time and query-time embedding.

# COMMAND ----------

dbutils.widgets.text("catalog", "partssource_demo")
dbutils.widgets.text("endpoint_name", "partssource-vs-endpoint")

CATALOG = dbutils.widgets.get("catalog")
ENDPOINT = dbutils.widgets.get("endpoint_name")
INDEX_NAME = f"{CATALOG}.parts.parts_searchable_index"

print(f"Querying index {INDEX_NAME} on endpoint {ENDPOINT}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

QUERIES = [
    "ventilator parts compatible with GE machines",
    "imaging components for MRI systems",
    "hazardous material handling for sterilization",
    "infusion pump consumables",
    "endoscopy replacement parts",
]

COLUMNS = ["part_id", "sku", "part_name", "category", "manufacturer", "list_price_usd"]


def run_query(query, k=5):
    resp = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME,
        query_text=query,
        columns=COLUMNS,
        num_results=k,
    )
    rows = resp.result.data_array if resp.result else []
    return rows


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run each query and print top-5 hits

# COMMAND ----------

for q in QUERIES:
    print(f"\n=== Query: {q!r} ===")
    rows = run_query(q, k=5)
    if not rows:
        print("  (no results — index may still be syncing)")
        continue
    for r in rows:
        # Last column is the similarity score.
        part_id, sku, name, cat, mfr, price, score = r
        print(f"  [{score:.3f}] {sku} — {name} ({cat}, {mfr}, ${price})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes for the PartsSource team
# MAGIC
# MAGIC - **Embedding model is BYO.** This demo uses Databricks' built-in
# MAGIC   `databricks-gte-large-en`. The same index can point at a Google
# MAGIC   embedding endpoint (matching the production GenAI stack) by
# MAGIC   registering it as a serving endpoint and swapping the
# MAGIC   `embedding_model_endpoint_name`.
# MAGIC - **Source is governed.** The index reads from a UC Delta table —
# MAGIC   masks, tags, and lineage on `parts.silver_parts` flow through.
# MAGIC - **Sync is incremental.** CDF on `parts.parts_searchable` means new
# MAGIC   or changed rows reembed automatically; full reindex is not needed.
