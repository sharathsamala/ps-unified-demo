#!/usr/bin/env bash
# CLI installer for the PartsSource Unified DAB. Kept as an alternative path;
# the default customer flow is "Deploy bundle" from the Databricks workspace UI.
#
# Usage:
#   ./scripts/install.sh             # deploys `demo` target + runs the setup job
#   ./scripts/install.sh dev         # deploys `dev` target + runs the setup job
#   ./scripts/install.sh demo --deploy-only
#
# Prereqs: Databricks CLI v0.240+ (databricks auth login).

set -euo pipefail

TARGET="${1:-demo}"
RUN_JOB=1
if [[ "${2:-}" == "--deploy-only" ]]; then
  RUN_JOB=0
fi

CATALOG="partssource_demo"

export DATABRICKS_BUNDLE_ENGINE=direct

echo "==> Validating bundle"
databricks bundle validate --target "$TARGET"

# Bootstrap catalog before deploy — the job's create_namespaces task handles this
# at runtime, but any pipeline referencing the catalog validates at deploy time.
echo "==> Ensuring catalog '$CATALOG' exists"
WAREHOUSE_ID="$(
  databricks warehouses list --output json 2>/dev/null \
    | python3 -c '
import json, sys
whs = json.load(sys.stdin)
for w in whs:
    if w.get("enable_serverless_compute") and w.get("state") == "RUNNING":
        print(w["id"]); break
else:
    for w in whs:
        if w.get("enable_serverless_compute"):
            print(w["id"]); break
    else:
        if whs: print(whs[0]["id"])
'
)"

if [[ -z "$WAREHOUSE_ID" ]]; then
  echo "  No SQL warehouse available. Create a serverless warehouse in the UI, then re-run." >&2
  exit 1
fi

databricks api post /api/2.0/sql/statements \
  --json "{\"warehouse_id\":\"$WAREHOUSE_ID\",\"statement\":\"CREATE CATALOG IF NOT EXISTS $CATALOG COMMENT 'PartsSource unified demo'\",\"wait_timeout\":\"30s\"}" \
  > /dev/null

echo "==> Deploying bundle to target: $TARGET"
databricks bundle deploy --target "$TARGET"

if [[ "$RUN_JOB" == "1" ]]; then
  echo "==> Running setup job"
  databricks bundle run partssource_setup --target "$TARGET"
  echo ""
  echo "Setup complete. Open the workspace to see:"
  echo "  - Catalog:    $CATALOG  (schemas: raw, suppliers, parts, service_ops, business_metrics, reverse_etl)"
  echo "  - Pipelines:  partssource-suppliers-$TARGET / -parts-$TARGET / -service-ops-$TARGET"
  echo "  - Dashboard:  PartsSource — Operations Overview"
  echo "  - Genie:      PartsSource — Supply Chain Intelligence ($CATALOG)"
else
  echo "==> Deploy-only mode; skipping job run"
  echo "To run later: databricks bundle run partssource_setup --target $TARGET"
fi
