# PartsSource Unified Demo

A single Databricks Asset Bundle that stands up a complete, governed, AI-ready demo on any workspace in one click.

**What you get:**
- **Unity Catalog** — catalog + schemas (`raw`, `pipeline`, `gold`, `reverse_etl`), PII tags, dynamic column masks, certified-table tags
- **Synthetic data** — ~50k parts, 200 suppliers, 1.5k customers, 25k work orders, 120k purchases, 35k inventory rows
- **Spark Declarative Pipelines** — bronze → silver → gold medallion with inline data quality expectations
- **Certified Metric Views** — `mv_supplier_spend`, `mv_part_demand`, `mv_pricing_benchmark`, `mv_reorder_needed`
- **AI/BI Dashboard** — *PartsSource — Operations Overview*
- **Genie Space** — natural-language analytics over the gold metric views, with curated instructions + sample questions
- **Serverless SQL Warehouse** — auto-created, powers the dashboard + Genie

---

## Install — one click from a Databricks notebook (recommended)

1. In your Databricks workspace, go to **Workspace → Repos → Add repo**
2. Paste the GitHub URL of this repo and clone
3. Open `install.py` from the repo
4. (Optional) Change the catalog name in the widget at the top
5. Click **Run All**

End-to-end: ~8–12 minutes on the first run. That's it — no local CLI, no PAT, no SSH.

---

## Install — from your laptop (CLI)

If you'd rather run from your laptop:

```bash
git clone <repo-url>
cd ps-unified-demo
./scripts/install.sh demo        # or: ./scripts/install.sh dev
```

Prereqs:
- Databricks CLI v0.240+ (`databricks auth login` done once)
- Unity Catalog + serverless compute enabled in the workspace

---

## Prereqs (both install paths)

- Unity Catalog enabled (nearly every modern workspace has this)
- Serverless compute enabled for Jobs + SQL Warehouses + Pipelines
- The installing user has `CREATE CATALOG` on the metastore (or pre-create the catalog + grant `CREATE SCHEMA`)

---

## What the setup job does

| Task | Runs on | What |
|---|---|---|
| `create_namespaces` | Serverless SQL warehouse | `CREATE CATALOG / SCHEMA / VOLUME IF NOT EXISTS` |
| `seed_data` | Serverless Python | Writes 6 CSV datasets to `/Volumes/<catalog>/raw/landing/` |
| `run_pipeline` | Spark Declarative Pipeline | Full-refresh bronze → silver → gold + reverse_etl |
| `apply_governance` | Serverless SQL warehouse | PII tags, dynamic column masks, certified tags |
| `build_metric_views` | Serverless SQL warehouse | Certified `mv_*` views on gold |
| `setup_genie` | Serverless Python | Creates/updates the Genie space via SDK |

---

## Targets

| Target | Catalog | Use |
|---|---|---|
| `dev` | `partssource_demo_dev` | Iteration; bundle in development mode (prefixed resource names) |
| `demo` | `partssource_demo` | Customer-facing demo |

---

## After install — what you can explore

1. **Catalog Explorer** → your catalog → 4 schemas, PII tags on columns, lineage from bronze through gold
2. **Jobs & Pipelines → `partssource-medallion-<target>`** — pipeline graph, DQ expectations, observability
3. **Dashboards → *PartsSource — Operations Overview*** — reorder needs, supplier spend, pricing savings, demand trends
4. **Genie → *PartsSource — Supply Chain Intelligence*** — open, ask:
   - "Top 10 suppliers by total spend"
   - "Which parts are below reorder point in CHI-02?"
   - "Parts where we're paying >20% below list price"

---

## Idempotency

Everything here is safe to re-run. The setup job uses replacement semantics at every step:
- `CREATE ... IF NOT EXISTS` for namespaces and volume
- `upload(overwrite=True)` for seed CSVs
- `full_refresh: true` for the pipeline
- `SET TAGS` / `SET MASK` overwrite semantics in UC
- `CREATE OR REPLACE FUNCTION` / `VIEW`
- `bundle deploy` upserts by resource name

Running `install.py` twice produces the same end state as running it once.

---

## Tear-down

```bash
databricks bundle destroy --target demo
```

Destroys the bundle-owned objects. The UC catalog itself is preserved by default — safer for shared workspaces. Drop the catalog manually via Catalog Explorer if you want a fully clean slate.

---

## File layout

```
ps-unified-demo/
├── install.py                    # notebook installer (one-click)
├── databricks.yml                # bundle definition, variables, targets
├── resources/
│   ├── warehouse.yml             # Serverless SQL warehouse
│   ├── pipeline.yml              # Spark Declarative pipeline
│   ├── jobs.yml                  # Orchestrating setup job
│   └── dashboard.yml             # AI/BI dashboard
├── src/
│   ├── setup/create_namespaces.sql
│   ├── seed/generate_data.py
│   ├── pipeline/{bronze,silver,gold}.py
│   ├── governance/apply_governance.sql
│   ├── semantics/metric_views.sql
│   ├── dashboard/partssource_ops.lvdash.json
│   └── genie/setup_genie.py
└── scripts/install.sh            # CLI-based installer (alternative path)
```

---

## Known notes

- **Lakebase + Synced Tables** are not provisioned by this bundle — Lakebase DAB resources are still in preview. Add via the UI or CLI after installing if you want the OLTP leg.
- **Genie SDK** is evolving; `setup_genie.py` fails soft — if the API signature changes, the job logs a warning and you can create the Genie space manually pointing at `gold.mv_*` views.
- **Account groups** referenced in the masking functions (e.g. `partssource_pii_viewers`) should be created at the account level if you want the "viewer sees unmasked" behavior. If the group doesn't exist, all users see the masked value — which is the safe default.
