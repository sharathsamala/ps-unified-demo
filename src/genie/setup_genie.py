"""Provision the Genie space + publish the AI/BI dashboard.

Genie spaces and dashboard publishing don't have first-class DAB resources,
so we provision via the REST API.

- Idempotent: existing space is updated (trash + recreate) to keep the
  serialized_space in sync.
- Publishes the '[...] PartsSource — Operations Overview' dashboard so its
  public URL is usable without editing.
"""

import argparse
import json
import sys
import time

from databricks.sdk import WorkspaceClient


SPACE_TITLE_TEMPLATE = "PartsSource — Supply Chain Intelligence ({catalog})"
DASHBOARD_NAME_SUFFIX = "PartsSource — Operations Overview"


def _serialized_space(catalog):
    """Return the Genie space export proto (version 2) as a JSON string.

    The proto `databricks.datarooms.export.GenieSpaceExport` accepts
    `version` + `data_sources.tables[*].identifier`. Instructions and sample
    questions need to be added via the UI — the proto's extended fields are
    private and not yet documented.
    """
    tables = sorted([
        f"{catalog}.business_metrics.mv_supplier_spend",
        f"{catalog}.business_metrics.mv_pricing_benchmark",
        f"{catalog}.business_metrics.mv_reorder_needed",
        f"{catalog}.business_metrics.mv_part_demand",
        f"{catalog}.business_metrics.mv_sla_performance",
    ])
    return json.dumps({
        "version": 2,
        "data_sources": {
            "tables": [{"identifier": t} for t in tables],
        },
    })


def _api(w, method, path, body=None):
    return w.api_client.do(method, path, body=body)


def provision_genie_space(w, catalog, warehouse_id):
    title = SPACE_TITLE_TEMPLATE.format(catalog=catalog)
    description = (
        f"Natural-language analytics over PartsSource operations on the "
        f"`{catalog}` catalog. Backed by certified metric views in "
        f"`{catalog}.business_metrics`."
    )

    existing_id = None
    try:
        resp = _api(w, "GET", "/api/2.0/genie/spaces")
        for s in resp.get("spaces", []):
            if s.get("title") == title:
                existing_id = s["space_id"]
                break
    except Exception as e:
        print(f"[warn] list Genie spaces failed: {e}")

    payload = {
        "title": title,
        "description": description,
        "warehouse_id": warehouse_id,
        "serialized_space": _serialized_space(catalog),
    }

    if existing_id:
        print(f"Updating Genie space {existing_id}")
        _api(w, "PATCH", f"/api/2.0/genie/spaces/{existing_id}", payload)
        space_id = existing_id
    else:
        print(f"Creating Genie space '{title}'")
        resp = _api(w, "POST", "/api/2.0/genie/spaces", payload)
        space_id = resp.get("space_id")
        print(f"Created Genie space {space_id}")

    return space_id


def publish_dashboard(w):
    try:
        resp = _api(w, "GET", "/api/2.0/lakeview/dashboards")
    except Exception as e:
        print(f"[warn] list dashboards failed: {e}")
        return

    target = None
    for d in resp.get("dashboards", []):
        name = d.get("display_name", "")
        if DASHBOARD_NAME_SUFFIX in name and d.get("lifecycle_state") == "ACTIVE":
            target = d
            break

    if not target:
        print(f"[warn] no active dashboard matching '{DASHBOARD_NAME_SUFFIX}'")
        return

    dashboard_id = target["dashboard_id"]
    warehouse_id = target.get("warehouse_id")
    print(f"Publishing dashboard {dashboard_id} ({target.get('display_name')})")
    _api(
        w,
        "POST",
        f"/api/2.0/lakeview/dashboards/{dashboard_id}/published",
        {"embed_credentials": True, "warehouse_id": warehouse_id},
    )
    print("Dashboard published.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--warehouse-id", required=True)
    args, _ = parser.parse_known_args()

    w = WorkspaceClient()

    # Genie
    try:
        provision_genie_space(w, args.catalog, args.warehouse_id)
    except Exception as e:
        print(f"[warn] Genie provisioning failed: {e}")
        print("Create the Genie space manually over business_metrics.mv_*.")

    # Dashboard publish (independent of Genie)
    try:
        publish_dashboard(w)
    except Exception as e:
        print(f"[warn] dashboard publish failed: {e}")


if __name__ == "__main__":
    main()
