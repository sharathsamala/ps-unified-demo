# Databricks notebook source
# MAGIC %md
# MAGIC # PartsSource Unified Demo — One-Click Installer
# MAGIC
# MAGIC Run this notebook top to bottom. It will:
# MAGIC 1. Install the Databricks CLI in the notebook runtime
# MAGIC 2. Authenticate using the notebook's workspace context (no PAT needed)
# MAGIC 3. Deploy the Asset Bundle (`databricks bundle deploy`)
# MAGIC 4. Run the one-shot setup job (creates catalog, seeds data, runs pipeline, applies governance, builds metric views, sets up Genie)
# MAGIC
# MAGIC **Prereqs (almost any modern workspace already has these):**
# MAGIC - Unity Catalog enabled
# MAGIC - Serverless compute enabled (Jobs + SQL Warehouses + Pipelines)
# MAGIC - Your user has `CREATE CATALOG` on the metastore
# MAGIC
# MAGIC **How to use:**
# MAGIC 1. Clone this repo into Workspace → Repos
# MAGIC 2. Open this notebook (`install.py`)
# MAGIC 3. Adjust the `catalog` widget below if you want a custom name
# MAGIC 4. Click **Run All**
# MAGIC
# MAGIC End-to-end time: ~8–12 minutes on first run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "partssource_demo", "Unity Catalog name")
dbutils.widgets.dropdown("target", "demo", ["dev", "demo"], "Bundle target")

CATALOG = dbutils.widgets.get("catalog").strip()
TARGET = dbutils.widgets.get("target")

assert CATALOG, "Catalog name cannot be empty"
print(f"Catalog:  {CATALOG}")
print(f"Target:   {TARGET}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Install the Databricks CLI

# COMMAND ----------

# MAGIC %sh
# MAGIC set -euo pipefail
# MAGIC if ! command -v databricks >/dev/null 2>&1; then
# MAGIC   echo "Installing Databricks CLI..."
# MAGIC   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
# MAGIC fi
# MAGIC databricks --version

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Authenticate using notebook context
# MAGIC
# MAGIC We grab the workspace host + API token from the notebook runtime context and export them as env vars. The CLI picks these up automatically.

# COMMAND ----------

import os

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()

os.environ["DATABRICKS_HOST"] = host
os.environ["DATABRICKS_TOKEN"] = token
os.environ["DATABRICKS_BUNDLE_ENGINE"] = "direct"

print(f"Host: {host}")
print("Auth: notebook context token (OK)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Locate the bundle directory
# MAGIC
# MAGIC When this notebook is opened from a Databricks Repo, the bundle YAML sits in the same folder.

# COMMAND ----------

import os

nb_path = ctx.notebookPath().get()          # /Repos/<user>/ps-unified-demo/install
bundle_dir_ws = "/Workspace" + os.path.dirname(nb_path)

if not os.path.exists(os.path.join(bundle_dir_ws, "databricks.yml")):
    raise RuntimeError(
        f"databricks.yml not found at {bundle_dir_ws}. "
        "Make sure you opened this notebook from a Databricks Repo clone of ps-unified-demo."
    )

os.chdir(bundle_dir_ws)
print(f"Bundle dir: {bundle_dir_ws}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Pre-create the catalog
# MAGIC
# MAGIC On workspaces using Default Storage, the Catalog REST API refuses `CREATE CATALOG` without an explicit managed location. SQL against a warehouse works — the serverless SQL path resolves the account-level default storage.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

warehouse_id = None
for wh in w.warehouses.list():
    if wh.enable_serverless_compute and str(wh.state) == "State.RUNNING":
        warehouse_id = wh.id
        break
if warehouse_id is None:
    for wh in w.warehouses.list():
        if wh.enable_serverless_compute:
            warehouse_id = wh.id
            break
if warehouse_id is None:
    whs = list(w.warehouses.list())
    if not whs:
        raise RuntimeError("No SQL warehouse found. Create one, then re-run.")
    warehouse_id = whs[0].id

print(f"Using warehouse: {warehouse_id}")

stmt = w.statement_execution.execute_statement(
    warehouse_id=warehouse_id,
    statement=f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'PartsSource unified demo'",
    wait_timeout="30s",
)
print(f"Catalog ensured: {CATALOG}  (statement={stmt.statement_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Deploy the bundle

# COMMAND ----------

import subprocess, sys

result = subprocess.run(
    ["databricks", "bundle", "deploy", "--target", TARGET,
     "--var", f"catalog={CATALOG}"],
    cwd=bundle_dir_ws,
    capture_output=True,
    text=True,
)
print(result.stdout)
if result.returncode != 0:
    print(result.stderr, file=sys.stderr)
    raise RuntimeError(f"bundle deploy failed (exit {result.returncode})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Run the setup job
# MAGIC
# MAGIC One job runs the full chain: namespaces → seed data → pipeline (bronze→silver→gold) → governance → metric views → Genie.

# COMMAND ----------

result = subprocess.run(
    ["databricks", "bundle", "run", "partssource_setup", "--target", TARGET],
    cwd=bundle_dir_ws,
    capture_output=True,
    text=True,
)
print(result.stdout)
if result.returncode != 0:
    print(result.stderr, file=sys.stderr)
    raise RuntimeError(f"bundle run failed (exit {result.returncode})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done — what to open next
# MAGIC
# MAGIC - **Catalog Explorer** → `{CATALOG}` — browse schemas, tags, lineage
# MAGIC - **Jobs & Pipelines** → `partssource-medallion-{TARGET}` — pipeline graph + DQ metrics
# MAGIC - **Dashboards** → *PartsSource — Operations Overview*
# MAGIC - **Genie** → *PartsSource — Supply Chain Intelligence* — ask "Top 10 suppliers by spend?"

# COMMAND ----------

print(f"""
Install complete.

Catalog:    {CATALOG}
Target:     {TARGET}
Pipeline:   partssource-medallion-{TARGET}
Dashboard:  PartsSource — Operations Overview
Genie:      PartsSource — Supply Chain Intelligence
""")
