-- Apply Unity Catalog governance: PII tags, column masks, certification tags.
-- Runs after the pipeline via the orchestration job.
-- :param catalog = variable passed in from the job (DAB ${var.catalog})
--
-- Note on grants: `GRANT ... ON CATALOG IDENTIFIER(:catalog)` is not accepted by
-- the SQL parser (parameter markers disallowed in GRANT/REVOKE). For the demo we
-- rely on the caller already having full privileges on the catalog they created.
-- Add `account users` grants out-of-band (Catalog Explorer) if needed.
--
-- Pipeline output lives in `${catalog}.pipeline.*` (UC pipelines require a single
-- target schema). Metric views live in `${catalog}.gold.*`.

USE CATALOG IDENTIFIER(:catalog);

---------------------------------------------------------
-- 1. PII tags on sensitive columns
---------------------------------------------------------
ALTER TABLE pipeline.silver_customers
  ALTER COLUMN ssn_last4_contact SET TAGS ('pii' = 'true', 'classification' = 'restricted');

ALTER TABLE pipeline.silver_customers
  ALTER COLUMN facility_name SET TAGS ('pii' = 'false', 'classification' = 'internal');

ALTER TABLE pipeline.silver_suppliers
  ALTER COLUMN tax_id SET TAGS ('pii' = 'true', 'classification' = 'restricted');

ALTER TABLE pipeline.silver_suppliers
  ALTER COLUMN contact_email SET TAGS ('pii' = 'true', 'classification' = 'confidential');

---------------------------------------------------------
-- 2. Column masking functions (live in `gold` schema alongside metric views)
---------------------------------------------------------
CREATE OR REPLACE FUNCTION gold.mask_ssn(val STRING)
RETURN CASE
  WHEN is_account_group_member('partssource_pii_viewers') THEN val
  ELSE 'XXXX'
END;

CREATE OR REPLACE FUNCTION gold.mask_tax_id(val STRING)
RETURN CASE
  WHEN is_account_group_member('partssource_pii_viewers') THEN val
  ELSE CONCAT('***-*', SUBSTRING(val, -4))
END;

CREATE OR REPLACE FUNCTION gold.mask_email(val STRING)
RETURN CASE
  WHEN is_account_group_member('partssource_pii_viewers') THEN val
  ELSE CONCAT('***@', SPLIT(val, '@')[1])
END;

---------------------------------------------------------
-- 3. Apply masks to silver tables
---------------------------------------------------------
ALTER TABLE pipeline.silver_customers
  ALTER COLUMN ssn_last4_contact SET MASK gold.mask_ssn;

ALTER TABLE pipeline.silver_suppliers
  ALTER COLUMN tax_id SET MASK gold.mask_tax_id;

ALTER TABLE pipeline.silver_suppliers
  ALTER COLUMN contact_email SET MASK gold.mask_email;

---------------------------------------------------------
-- 4. Certified-status tags on gold tables (semantic hints for Genie/AI/BI)
---------------------------------------------------------
ALTER TABLE pipeline.gold_supplier_performance
  SET TAGS ('certified' = 'true', 'domain' = 'supply_chain', 'steward' = 'supply_chain_team');

ALTER TABLE pipeline.gold_part_demand_monthly
  SET TAGS ('certified' = 'true', 'domain' = 'demand', 'steward' = 'planning_team');

ALTER TABLE pipeline.gold_part_pricing_benchmark
  SET TAGS ('certified' = 'true', 'domain' = 'procurement', 'steward' = 'procurement_team');

ALTER TABLE pipeline.gold_reorder_recommendations
  SET TAGS ('certified' = 'true', 'domain' = 'operations', 'steward' = 'operations_team');
