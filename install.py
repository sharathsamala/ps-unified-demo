# Databricks notebook source
# MAGIC %md
# MAGIC # PartsSource Unified Demo — One-Click Installer
# MAGIC
# MAGIC Run this notebook top to bottom. It will provision the full demo on your workspace using only the Databricks Python SDK — no CLI, no PAT, no local setup.
# MAGIC
# MAGIC **What gets created:**
# MAGIC - Unity Catalog `<catalog>` with schemas (`raw`, `pipeline`, `gold`, `reverse_etl`) + a landing volume
# MAGIC - ~230k rows of synthetic PartsSource-shaped data across 6 CSV datasets
# MAGIC - Serverless SQL warehouse
# MAGIC - Spark Declarative Pipeline (bronze → silver → gold + reverse_etl) — full refresh
# MAGIC - Governance: PII tags, dynamic column masks, certified-table tags
# MAGIC - Certified metric views (`gold.mv_*`)
# MAGIC - Genie space pointed at the metric views
# MAGIC
# MAGIC **Prereqs:**
# MAGIC - Unity Catalog enabled
# MAGIC - Serverless compute enabled (Jobs + SQL Warehouses + Pipelines)
# MAGIC - The running user has `CREATE CATALOG` on the metastore
# MAGIC
# MAGIC **How to use:**
# MAGIC 1. Clone this repo into Workspace → Repos
# MAGIC 2. Open this notebook (`install.py`)
# MAGIC 3. (Optional) change the catalog name in the widget below
# MAGIC 4. Click **Run All**
# MAGIC
# MAGIC End-to-end: ~8–12 minutes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "partssource_demo", "Unity Catalog name")
dbutils.widgets.text("warehouse_name", "partssource-demo-wh", "SQL warehouse name")
dbutils.widgets.text("pipeline_name", "partssource-medallion-demo", "Pipeline name")

CATALOG = dbutils.widgets.get("catalog").strip()
WAREHOUSE_NAME = dbutils.widgets.get("warehouse_name").strip()
PIPELINE_NAME = dbutils.widgets.get("pipeline_name").strip()

assert CATALOG, "Catalog name cannot be empty"
print(f"Catalog:       {CATALOG}")
print(f"Warehouse:     {WAREHOUSE_NAME}")
print(f"Pipeline:      {PIPELINE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Resolve the repo path
# MAGIC
# MAGIC On serverless notebook compute, reading files under `/Workspace/...` via plain `open()` is unreliable (FUSE flakiness). We use the Workspace API (`w.workspace.export`) to pull file contents on demand.

# COMMAND ----------

import base64
import importlib.util
import os
import sys
import tempfile

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
nb_path = ctx.notebookPath().get()            # e.g. /Users/<you>/ps-unified-demo/install  OR  /Repos/<you>/ps-unified-demo/install
REPO_API_PATH = os.path.dirname(nb_path)      # Workspace API path (no /Workspace prefix)
REPO_WS_PATH = "/Workspace" + REPO_API_PATH   # /Workspace-prefixed path for pipeline libraries

print(f"Repo (API path):       {REPO_API_PATH}")
print(f"Repo (/Workspace path): {REPO_WS_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Pick a serverless SQL warehouse (create one if needed)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType, EndpointInfoWarehouseType

w = WorkspaceClient()


def read_workspace_file(api_path):
    """Read a file from the workspace via the API. Reliable on serverless."""
    export = w.workspace.export(path=api_path)
    return base64.b64decode(export.content).decode()


def import_from_workspace(module_name, api_path):
    """Download a .py file from workspace via API and import it as a module."""
    content = read_workspace_file(api_path)
    tmp_path = os.path.join(tempfile.gettempdir(), f"{module_name}.py")
    with open(tmp_path, "w") as f:
        f.write(content)
    spec = importlib.util.spec_from_file_location(module_name, tmp_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def find_or_create_warehouse():
    # Prefer an existing warehouse by our configured name.
    for wh in w.warehouses.list():
        if wh.name == WAREHOUSE_NAME:
            print(f"Found warehouse: {WAREHOUSE_NAME} ({wh.id})")
            return wh.id

    # Otherwise any serverless warehouse that's running.
    for wh in w.warehouses.list():
        if wh.enable_serverless_compute and str(wh.state) == "State.RUNNING":
            print(f"Using existing serverless warehouse: {wh.name} ({wh.id})")
            return wh.id

    # Otherwise create one.
    print(f"Creating warehouse: {WAREHOUSE_NAME}")
    created = w.warehouses.create(
        name=WAREHOUSE_NAME,
        cluster_size="Small",
        warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
        enable_serverless_compute=True,
        auto_stop_mins=10,
        max_num_clusters=1,
        min_num_clusters=1,
    ).result()
    print(f"Created: {created.id}")
    return created.id


WAREHOUSE_ID = find_or_create_warehouse()
print(f"Warehouse ID: {WAREHOUSE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create catalog, schemas, and landing volume

# COMMAND ----------

from databricks.sdk.service.sql import StatementState


def sql_exec(statement, catalog=None):
    """Run a single SQL statement against the warehouse. Raises on failure."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        catalog=catalog,
        wait_timeout="50s",
    )
    # Poll if not done.
    sid = resp.statement_id
    while resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        resp = w.statement_execution.get_statement(sid)
    if resp.status and resp.status.state != StatementState.SUCCEEDED:
        err = getattr(resp.status.error, "message", str(resp.status)) if resp.status.error else str(resp.status.state)
        raise RuntimeError(f"SQL failed: {err}\nStatement: {statement[:200]}")
    return resp


# Catalog first — on Default Storage workspaces this resolves the account-level default.
sql_exec(f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'PartsSource unified demo'")
print(f"Catalog ensured: {CATALOG}")

for schema, comment in [
    ("raw",         "Landing zone for raw CSVs"),
    ("pipeline",    "Pipeline target schema — bronze_*, silver_*, gold_* tables"),
    ("gold",        "Certified metric views (mv_*) on top of pipeline.gold_*"),
    ("reverse_etl", "Reverse-ETL tables synced out to operational stores"),
]:
    sql_exec(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{comment}'")
    print(f"  schema: {schema}")

sql_exec(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing COMMENT 'CSV landing volume'")
print(f"  volume: {CATALOG}.raw.landing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Seed synthetic data into the landing volume

# COMMAND ----------

seed_mod = import_from_workspace("generate_data", f"{REPO_API_PATH}/src/seed/generate_data.py")
seed_mod.generate_all(CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Create (or update) the Spark Declarative Pipeline

# COMMAND ----------

from databricks.sdk.service.pipelines import PipelineLibrary, FileLibrary

PIPELINE_LIBS = [
    PipelineLibrary(file=FileLibrary(path=f"{REPO_WS_PATH}/src/pipeline/bronze.py")),
    PipelineLibrary(file=FileLibrary(path=f"{REPO_WS_PATH}/src/pipeline/silver.py")),
    PipelineLibrary(file=FileLibrary(path=f"{REPO_WS_PATH}/src/pipeline/gold.py")),
]


def find_pipeline_id(name):
    for p in w.pipelines.list_pipelines(filter=f"name LIKE '{name}'"):
        if p.name == name:
            return p.pipeline_id
    return None


pipeline_id = find_pipeline_id(PIPELINE_NAME)

if pipeline_id:
    print(f"Updating pipeline: {PIPELINE_NAME} ({pipeline_id})")
    w.pipelines.update(
        pipeline_id=pipeline_id,
        name=PIPELINE_NAME,
        catalog=CATALOG,
        target="pipeline",
        serverless=True,
        photon=True,
        channel="PREVIEW",
        edition="ADVANCED",
        continuous=False,
        libraries=PIPELINE_LIBS,
        configuration={"ps.catalog": CATALOG},
    )
else:
    print(f"Creating pipeline: {PIPELINE_NAME}")
    created = w.pipelines.create(
        name=PIPELINE_NAME,
        catalog=CATALOG,
        target="pipeline",
        serverless=True,
        photon=True,
        channel="PREVIEW",
        edition="ADVANCED",
        continuous=False,
        libraries=PIPELINE_LIBS,
        configuration={"ps.catalog": CATALOG},
    )
    pipeline_id = created.pipeline_id
    print(f"  created: {pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Run a full-refresh update on the pipeline and wait for completion

# COMMAND ----------

import time
from databricks.sdk.service.pipelines import UpdateInfoState

update = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=True)
update_id = update.update_id
print(f"Pipeline update started: {update_id}")

TERMINAL = {UpdateInfoState.COMPLETED, UpdateInfoState.FAILED, UpdateInfoState.CANCELED}
last_state = None

while True:
    info = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
    state = info.update.state
    if state != last_state:
        print(f"  state: {state}")
        last_state = state
    if state in TERMINAL:
        break
    time.sleep(15)

if last_state != UpdateInfoState.COMPLETED:
    raise RuntimeError(f"Pipeline update ended in state {last_state}. Check the pipeline UI for details.")

print("Pipeline full refresh complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Apply governance (PII tags, column masks, certified tags)

# COMMAND ----------

import re


def run_sql_file(api_path, catalog):
    """Read a .sql file via Workspace API, substitute `:catalog`, split, execute each statement."""
    raw = read_workspace_file(api_path)
    cleaned = "\n".join(l for l in raw.splitlines() if not l.strip().startswith("--"))
    substituted = cleaned.replace("IDENTIFIER(:catalog)", catalog).replace(":catalog", catalog)
    statements = [s.strip() for s in re.split(r";\s*\n", substituted) if s.strip()]
    for stmt in statements:
        print(f"  exec: {stmt.splitlines()[0][:90]}...")
        sql_exec(stmt)


run_sql_file(f"{REPO_API_PATH}/src/governance/apply_governance.sql", CATALOG)
print("Governance applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Build certified metric views

# COMMAND ----------

run_sql_file(f"{REPO_API_PATH}/src/semantics/metric_views.sql", CATALOG)
print("Metric views built.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Create / update the Genie space (fail-soft on SDK drift)

# COMMAND ----------

genie_mod = import_from_workspace("setup_genie", f"{REPO_API_PATH}/src/genie/setup_genie.py")
genie_mod.run_genie_setup(CATALOG, WAREHOUSE_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done — verify the output

# COMMAND ----------

for mv in ["mv_supplier_spend", "mv_part_demand", "mv_pricing_benchmark", "mv_reorder_needed"]:
    cnt = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.gold.{mv}").collect()[0]["n"]
    print(f"  {CATALOG}.gold.{mv:<24} rows={cnt:,}")

print(f"""
Install complete.

Catalog:    {CATALOG}
Pipeline:   {PIPELINE_NAME}
Warehouse:  {WAREHOUSE_NAME} ({WAREHOUSE_ID})
Metric views: {CATALOG}.gold.mv_*

Next:
  - Open Catalog Explorer → {CATALOG}
  - Open the pipeline graph (Jobs & Pipelines)
  - Ask the Genie space "Top 10 suppliers by spend"
""")
