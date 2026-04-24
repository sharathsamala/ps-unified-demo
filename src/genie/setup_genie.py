"""
Create or update the PartsSource Genie space.

Genie spaces don't have a first-class DAB resource yet, so we provision via the
Databricks SDK. Idempotent: if a space with the same title exists, it's updated.

Note on SDK drift: the `w.genie.*` surface is still evolving. If any call
raises, we log a warning and return cleanly so the job still succeeds — the
customer can create the space manually via the UI.
"""

import argparse

from databricks.sdk import WorkspaceClient

_parser = argparse.ArgumentParser()
_parser.add_argument("--catalog", required=True)
_parser.add_argument("--warehouse-id", required=True)
_args, _ = _parser.parse_known_args()
CATALOG = _args.catalog
WAREHOUSE_ID = _args.warehouse_id
TITLE = f"PartsSource — Supply Chain Intelligence ({CATALOG})"
DESCRIPTION = (
    "Natural-language analytics over PartsSource operations: suppliers, "
    "demand, pricing, reorder recommendations. Governed by Unity Catalog "
    "metric views."
)

INSTRUCTIONS = """\
You are an analyst helping PartsSource operations + procurement teams.

Use only the certified metric views in the `gold` schema. Prefer:
- mv_supplier_spend      — supplier performance, spend, on-time rate, composite score
- mv_part_demand         — monthly work-order volume per part / category
- mv_pricing_benchmark   — list vs paid price, savings opportunities
- mv_reorder_needed      — parts at/below reorder point + recommended supplier

Key terms:
- "Composite score" = 0.6 × on-time rate + 0.4 × (1 − defect_rate_ppm / 5000). Higher is better.
- "Reorder point" = threshold below which stock is replenished.
- "Savings vs list" = (list_price − avg_paid) / list_price. Negative means overpaying.
- "Tier" of a supplier: Preferred > Approved > Probation.

Style:
- Be concise. Prefer small tables to long prose.
- When a question is ambiguous (e.g. "top suppliers"), ask once what metric
  ("spend", "on-time rate", "composite score") before querying.
- Do not query bronze or silver schemas directly — only gold metric views.
"""

SAMPLE_QUESTIONS = [
    "Top 10 suppliers by total spend",
    "Which parts are below reorder point right now?",
    "Trend of work orders per month by category",
    "Which suppliers have the best on-time rate above $50k spend?",
    "Parts where we're paying >20% below list price",
    "Work-order volume for Imaging vs Cardiology last 3 months",
    "Suppliers with defect rate > 2000 PPM",
    "Recommended supplier for each reorder-needed part in the CHI-02 warehouse",
]


def _run():
    w = WorkspaceClient()
    tables = [
        f"{CATALOG}.gold.mv_supplier_spend",
        f"{CATALOG}.gold.mv_part_demand",
        f"{CATALOG}.gold.mv_pricing_benchmark",
        f"{CATALOG}.gold.mv_reorder_needed",
    ]
    genie = getattr(w, "genie", None)
    if genie is None:
        print("[warn] WorkspaceClient has no `genie` attribute in this SDK version.")
        print("Create the Genie space manually in the UI pointing at gold.mv_* views.")
        return

    list_fn = getattr(genie, "list_spaces", None)
    create_fn = getattr(genie, "create_space", None)
    update_fn = getattr(genie, "update_space", None)

    existing = None
    if callable(list_fn):
        try:
            for space in list_fn():
                if getattr(space, "title", None) == TITLE:
                    existing = space
                    break
        except Exception as e:
            print(f"[warn] list_spaces failed ({e}); proceeding to create")

    try:
        if existing and callable(update_fn):
            print(f"Updating Genie space: {existing.space_id}")
            update_fn(
                space_id=existing.space_id,
                title=TITLE,
                description=DESCRIPTION,
                warehouse_id=WAREHOUSE_ID,
                tables=tables,
                instructions=INSTRUCTIONS,
                sample_questions=SAMPLE_QUESTIONS,
            )
            print("Updated.")
        elif callable(create_fn):
            print("Creating Genie space")
            resp = create_fn(
                title=TITLE,
                description=DESCRIPTION,
                warehouse_id=WAREHOUSE_ID,
                tables=tables,
                instructions=INSTRUCTIONS,
                sample_questions=SAMPLE_QUESTIONS,
            )
            print(f"Created: {getattr(resp, 'space_id', '<no id>')}")
        else:
            print("[warn] Genie SDK does not expose create_space/update_space in this version.")
            print("Create the Genie space manually in the UI pointing at gold.mv_* views.")
    except Exception as e:
        print(f"[warn] Genie setup failed: {e}")
        print("Continuing — create the Genie space manually via the UI pointing at gold.mv_* views.")


if __name__ == "__main__":
    _run()
