#!/usr/bin/env bash
# One-shot installer for the PartsSource Unified DAB.
# Usage:
#   ./scripts/install.sh                  # deploys to `demo` target + runs setup job
#   ./scripts/install.sh dev              # deploys to `dev` target + runs setup job
#   ./scripts/install.sh demo --deploy-only   # skip running the setup job
#
# Prereqs: Databricks CLI v0.240+ authenticated (databricks auth login).
#   Optionally export DATABRICKS_CONFIG_PROFILE to pick a specific profile.

set -euo pipefail

TARGET="${1:-demo}"
RUN_JOB=1
if [[ "${2:-}" == "--deploy-only" ]]; then
  RUN_JOB=0
fi

# Derive the catalog name the same way the bundle does.
case "$TARGET" in
  dev)  CATALOG="partssource_demo_dev" ;;
  demo) CATALOG="partssource_demo" ;;
  *)    CATALOG="partssource_demo" ;;
esac

export DATABRICKS_BUNDLE_ENGINE=direct

echo "==> Validating bundle"
databricks bundle validate --target "$TARGET"

# --------------------------------------------------------------------------
# Bootstrap: create the catalog BEFORE deploy.
# The pipeline resource is validated against UC at deploy time, so the catalog
# must already exist. On Default Storage workspaces the Catalog REST API refuses
# to create without an explicit MANAGED LOCATION, but SQL via a warehouse works.
# --------------------------------------------------------------------------
echo "==> Ensuring catalog '$CATALOG' exists (pre-deploy bootstrap)"

# Pick any running/available serverless warehouse. Fall back to the first one.
WAREHOUSE_ID="$(
  databricks warehouses list --output json 2>/dev/null \
    | python3 -c '
import json, sys
whs = json.load(sys.stdin)
# Prefer serverless + running
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
  echo "  No SQL warehouse available for bootstrap. Create one in the UI, then re-run." >&2
  exit 1
fi

echo "  Using warehouse $WAREHOUSE_ID to run CREATE CATALOG IF NOT EXISTS"
databricks api post /api/2.0/sql/statements \
  --json "{\"warehouse_id\":\"$WAREHOUSE_ID\",\"statement\":\"CREATE CATALOG IF NOT EXISTS $CATALOG COMMENT 'PartsSource unified demo'\",\"wait_timeout\":\"30s\"}" \
  > /dev/null

echo "==> Deploying bundle to target: $TARGET"
databricks bundle deploy --target "$TARGET"

if [[ "$RUN_JOB" == "1" ]]; then
  echo "==> Running setup job (create_namespaces → seed → pipeline → governance → metric views → Genie)"
  databricks bundle run partssource_setup --target "$TARGET"
  echo ""
  echo "Setup complete. Open your workspace to see:"
  echo "  - Catalog:    $CATALOG"
  echo "  - Pipeline:   partssource-medallion-$TARGET"
  echo "  - Dashboard:  PartsSource — Operations Overview"
  echo "  - Genie:      PartsSource — Supply Chain Intelligence"
else
  echo "==> Deploy-only mode; skipping job run"
  echo "To run later: databricks bundle run partssource_setup --target $TARGET"
fi
