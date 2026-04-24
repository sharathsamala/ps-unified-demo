# PartsSource Unified Demo

A single Databricks Asset Bundle that stands up a governed, AI-ready Lakehouse demo on any workspace.

**What you get:**
- **Unity Catalog** — 6 schemas (`raw`, `suppliers`, `parts`, `service_ops`, `business_metrics`, `reverse_etl`), PII tags, column masks, certified-table tags
- **Mixed-format raw zone** — CSV + JSON, sharded into multiple files per dataset (so Auto Loader behaves like production)
- **Three domain SDP pipelines** (bronze → silver → gold) with **data-quality expectations** that catch injected defects
- **Certified Metric Views** in `business_metrics` (`mv_supplier_spend`, `mv_pricing_benchmark`, `mv_reorder_needed`, `mv_part_demand`, `mv_sla_performance`)
- **Orchestration job** demonstrating SQL tasks, pipeline tasks, notebook tasks, a condition task with branching, and reverse-ETL publication
- **4-page AI/BI Dashboard** — Executive / Supply Chain / Parts & Inventory / Service Ops (auto-published by the setup job)
- **Genie Space** pointed at all 5 metric views, provisioned via REST in the setup job
- **Serverless SQL warehouse** — auto-created

---

## Install — Deploy Bundle from the Databricks UI (recommended)

1. In your Databricks workspace: **Workspace → Repos → Add repo** → paste the GitHub URL of this repo and clone
2. Open the repo folder. The workspace auto-detects `databricks.yml` and shows the **Bundle** side panel (right side)
3. Click **Deploy bundle**. Pick the `demo` target (or `dev` for an isolated `partssource_demo_dev` catalog)
4. Go to **Jobs & Pipelines → `partssource-setup-demo`** and click **Run now**
5. ~8–12 min end to end

No local CLI, no PAT, no SSH.

---

## Install — from the CLI (alternative path)

```bash
git clone <repo-url>
cd ps-unified-demo
./scripts/install.sh demo    # or: ./scripts/install.sh dev
```

Prereqs:
- Databricks CLI v0.240+ (`databricks auth login` done once)
- Unity Catalog + serverless compute enabled
- The installing user has `CREATE CATALOG` on the metastore (or pre-create the catalog)

---

## What the setup job does

| # | Task | Type | What |
|---|---|---|---|
| 1 | `create_namespaces` | SQL | Catalog + 6 schemas + landing volume |
| 2 | `seed_data` | Python (serverless) | Shards CSV + JSON into `/Volumes/<cat>/raw/landing/<dataset>/*` |
| 3 | `run_suppliers_pipeline` | Pipeline | Suppliers domain full-refresh |
| 4 | `run_parts_pipeline` | Pipeline | Parts domain (depends on suppliers for pricing join) |
| 5 | `run_service_ops_pipeline` | Pipeline | Service Ops domain (parallel with parts) |
| 6 | `build_metric_views` | SQL | 5 YAML Metric Views in `business_metrics` |
| 7 | `apply_governance` | SQL | PII tags + column masks + certified tags |
| 8 | `check_reorder_alerts` | Notebook | Counts urgent reorders → sets `alert_count` task value |
| 9 | `alerts_branch` | Condition | `alert_count > 0` → path A; else path B |
| 10a | `publish_reverse_etl` → `notify_ops` | SQL → Notebook | Rebuild `reverse_etl.*` + log top alerts |
| 10b | `log_clean_run` | Notebook | Health summary when no alerts |
| 11 | `setup_genie` | Python | Create/update Genie space over the 5 metric views + publish the dashboard |

---

## Data quality defects (seeded + caught by SDP expectations)

| Dataset | Defect rate | Expectation | Action |
|---|---|---|---|
| `suppliers.silver_suppliers` | 5% `on_time_rate` out of `[0,1]` | `expect_or_drop` | Dropped + counted in the pipeline event log |
| `parts.silver_parts` | 3% `list_price_usd = 0` | `expect_or_drop` | Dropped |
| `service_ops.silver_work_orders` | 2% `closed_at < opened_at` | `expect` | Warn-only, visible in event log |
| `suppliers.silver_purchases` | 0.5% `qty <= 0` | `expect_or_drop` | Dropped |

Query the pipeline event log (`event_log(pipeline_id)`) to see per-expectation pass/drop counts.

---

## Targets

Both targets use the same catalog (`partssource_demo`). `dev` runs in bundle
**development mode** (resource names are prefixed with the deploying user) so
multiple developers can share a workspace without collisions. `demo` runs in
**production mode** for the customer-facing install.

| Target | Catalog | Mode |
|---|---|---|
| `dev` | `partssource_demo` | development (user-prefixed resources) |
| `demo` | `partssource_demo` | production |

---

## After install — what you can explore

1. **Catalog Explorer** → `partssource_demo` → 6 schemas, lineage across domains, PII tags
2. **Jobs & Pipelines** → three domain pipelines + the orchestration job — inspect the DAG, conditional branch, and DQ expectation stats
3. **Dashboards → *PartsSource — Operations Overview*** — 4 pages: Executive, Supply Chain, Parts & Inventory, Service Ops
4. **Genie → *PartsSource — Supply Chain Intelligence*** — open, ask:
   - "Top 10 suppliers by total spend"
   - "Which parts are below reorder point in warehouse CHI-02?"
   - "Parts where we're paying >20% below list price"
   - "SLA attainment by priority last quarter"

---

## Idempotency

Every step is safe to re-run:
- `CREATE ... IF NOT EXISTS` for namespaces + volume
- `upload(overwrite=True)` for seed shards
- `full_refresh: true` on every pipeline
- `CREATE OR REPLACE` for metric views, masking functions, reverse_etl tables
- `SET TAGS` / `SET MASK` overwrite semantics in UC
- `bundle deploy` upserts by resource name

Re-running produces the same end state.

---

## Tear-down

```bash
databricks bundle destroy --target demo
```

Destroys bundle-owned resources (jobs, pipelines, dashboard, warehouse). The UC catalog is preserved by default — safer on shared workspaces. Drop it manually if you want a clean slate.

---

## File layout

```
ps-unified-demo/
├── databricks.yml                       # bundle definition, variables, targets
├── resources/
│   ├── warehouse.yml                    # Serverless SQL warehouse
│   ├── pipelines.yml                    # 3 domain SDP pipelines
│   ├── jobs.yml                         # Orchestration job (DAG with branching)
│   ├── dashboard.yml                    # AI/BI dashboard
│   └── catalog.yml                      # (placeholder — catalog is SQL-created)
├── src/
│   ├── setup/create_namespaces.sql
│   ├── seed/generate_data.py            # sharded CSV + JSON with injected DQ defects
│   ├── pipeline/
│   │   ├── suppliers/{bronze,silver,gold}.py
│   │   ├── parts/{bronze,silver,gold}.py
│   │   └── service_ops/{bronze,silver,gold}.py
│   ├── governance/apply_governance.sql
│   ├── jobs/
│   │   ├── sql/build_metric_views.sql
│   │   ├── sql/publish_reverse_etl.sql
│   │   └── notebooks/{check_reorder_alerts,notify_ops,log_clean_run}.py
│   ├── dashboard/partssource_ops.lvdash.json
│   └── genie/setup_genie.py
└── scripts/install.sh                   # CLI alternative installer
```

---

## Known notes

- **Lakebase + Synced Tables** are not provisioned by this bundle — Lakebase DAB resources are still in preview. Add via the UI after install if you want the OLTP leg. The `reverse_etl.*` tables are the designed sync source.
- **Genie instructions + sample questions** — `setup_genie.py` creates the space with the 5 metric views over the `GenieSpaceExport` v2 proto (tables only, since the instructions/sample_questions sub-proto fields are still private). Add curated instructions + seed questions via the Genie UI after install.
- **Account groups** — the masking functions reference `partssource_pii_viewers`. Create that at the account level if you want "viewer sees unmasked" behavior. Without it, everyone sees the masked value (safe default).
