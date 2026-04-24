"""
Create or update the PartsSource Genie space.

Genie spaces don't have a first-class DAB resource, so we provision via the
Databricks SDK. Idempotent: if a space with the same title exists, it's updated.
Fail-soft: if the SDK's genie surface has drifted, we log a warning and return
cleanly so the job still succeeds.
"""

import argparse

from databricks.sdk import WorkspaceClient


INSTRUCTIONS = """\
You are an analyst helping PartsSource operations, procurement, and service teams.

Use only the certified metric views in the `business_metrics` schema. Each view
maps to a business domain:
  - mv_supplier_spend     (suppliers)   supplier spend, on-time rate, composite score
  - mv_pricing_benchmark  (parts)       list vs paid pricing, savings opportunities
  - mv_reorder_needed     (parts)       parts at or below reorder point per warehouse
  - mv_part_demand        (service_ops) monthly work-order volume per part
  - mv_sla_performance    (service_ops) SLA attainment by priority

Key terms:
- "Composite score" = 0.6 * on_time_rate + 0.4 * (1 - defect_rate_ppm / 5000).
- "Reorder point" = threshold below which a warehouse replenishes stock.
- "Savings vs list" = (list_price - avg_paid) / list_price. Negative means overpaying.
- "Tier" order: Preferred > Approved > Probation.
- "SLA target": P1 = 48h, P2 = 120h, P3/P4 = 240h. sla_pct is (met / closed).

Style:
- Be concise. Prefer small tables to long prose.
- When ambiguous ("top suppliers"), ask once: by spend, on-time rate, or composite?
- Never query bronze or silver schemas directly — only `business_metrics.mv_*`.
"""

SAMPLE_QUESTIONS = [
    "Top 10 suppliers by total spend",
    "Which parts are below reorder point in warehouse CHI-02?",
    "Parts where we're paying more than 20% below list price",
    "Trend of work orders per month for the Imaging category",
    "Suppliers with defect rate over 2000 PPM and spend above $50k",
    "SLA attainment by priority last quarter",
    "Estimated reorder value by warehouse right now",
    "Average work order duration per priority",
]


def run_genie_setup(catalog, warehouse_id):
    """Create or update the Genie space. Fail-soft on SDK drift."""
    title = f"PartsSource — Supply Chain Intelligence ({catalog})"
    description = (
        "Natural-language analytics over PartsSource operations: suppliers, "
        "parts, and service. Governed by Unity Catalog metric views in "
        "`business_metrics`."
    )
    tables = [
        f"{catalog}.business_metrics.mv_supplier_spend",
        f"{catalog}.business_metrics.mv_pricing_benchmark",
        f"{catalog}.business_metrics.mv_reorder_needed",
        f"{catalog}.business_metrics.mv_part_demand",
        f"{catalog}.business_metrics.mv_sla_performance",
    ]

    w = WorkspaceClient()
    genie = getattr(w, "genie", None)
    if genie is None:
        print("[warn] WorkspaceClient has no `genie` attribute in this SDK version.")
        print("Create the Genie space manually in the UI pointing at business_metrics.mv_*.")
        return

    list_fn = getattr(genie, "list_spaces", None)
    create_fn = getattr(genie, "create_space", None)
    update_fn = getattr(genie, "update_space", None)

    existing = None
    if callable(list_fn):
        try:
            for space in list_fn():
                if getattr(space, "title", None) == title:
                    existing = space
                    break
        except Exception as e:
            print(f"[warn] list_spaces failed ({e}); proceeding to create")

    try:
        if existing and callable(update_fn):
            print(f"Updating Genie space: {existing.space_id}")
            update_fn(
                space_id=existing.space_id,
                title=title,
                description=description,
                warehouse_id=warehouse_id,
                tables=tables,
                instructions=INSTRUCTIONS,
                sample_questions=SAMPLE_QUESTIONS,
            )
            print("Updated.")
        elif callable(create_fn):
            print("Creating Genie space")
            resp = create_fn(
                title=title,
                description=description,
                warehouse_id=warehouse_id,
                tables=tables,
                instructions=INSTRUCTIONS,
                sample_questions=SAMPLE_QUESTIONS,
            )
            print(f"Created: {getattr(resp, 'space_id', '<no id>')}")
        else:
            print("[warn] Genie SDK does not expose create_space/update_space.")
            print("Create the Genie space manually pointing at business_metrics.mv_*.")
    except Exception as e:
        print(f"[warn] Genie setup failed: {e}")
        print("Continuing — create the Genie space manually pointing at business_metrics.mv_*.")


def _cli_main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--warehouse-id", required=True)
    args, _ = parser.parse_known_args()
    run_genie_setup(args.catalog, args.warehouse_id)


if __name__ == "__main__":
    _cli_main()
