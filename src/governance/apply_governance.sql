-- Apply Unity Catalog governance: PII tags + column masks + certified-table tags.
-- Runs after all three SDP pipelines complete (they own silver/gold DDL).
-- :catalog — target catalog name.
--
-- Grants: `GRANT ... ON CATALOG IDENTIFIER(:catalog)` is not accepted by the
-- SQL parser (parameter markers disallowed in GRANT/REVOKE). Caller already
-- has privileges on the catalog they created; add `account users` reads via
-- Catalog Explorer if needed.

USE CATALOG IDENTIFIER(:catalog);

-- ---------------------------------------------------------------------
-- 1. Masking functions — colocated with the masked tables per schema.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION suppliers.mask_tax_id(val STRING)
RETURN CASE
  WHEN is_account_group_member('partssource_pii_viewers') THEN val
  ELSE CONCAT('***-*', SUBSTRING(val, -4))
END;

CREATE OR REPLACE FUNCTION suppliers.mask_email(val STRING)
RETURN CASE
  WHEN is_account_group_member('partssource_pii_viewers') THEN val
  ELSE CONCAT('***@', SPLIT(val, '@')[1])
END;

CREATE OR REPLACE FUNCTION service_ops.mask_ssn(val STRING)
RETURN CASE
  WHEN is_account_group_member('partssource_pii_viewers') THEN val
  ELSE 'XXXX'
END;

-- ---------------------------------------------------------------------
-- 2. PII tags + masks on suppliers domain
-- ---------------------------------------------------------------------
ALTER TABLE suppliers.silver_suppliers
  ALTER COLUMN tax_id SET TAGS ('pii' = 'true', 'classification' = 'restricted');

ALTER TABLE suppliers.silver_suppliers
  ALTER COLUMN contact_email SET TAGS ('pii' = 'true', 'classification' = 'confidential');

ALTER TABLE suppliers.silver_suppliers
  ALTER COLUMN tax_id SET MASK suppliers.mask_tax_id;

ALTER TABLE suppliers.silver_suppliers
  ALTER COLUMN contact_email SET MASK suppliers.mask_email;

-- ---------------------------------------------------------------------
-- 3. PII tags + masks on service_ops domain
-- ---------------------------------------------------------------------
ALTER TABLE service_ops.silver_customers
  ALTER COLUMN ssn_last4_contact SET TAGS ('pii' = 'true', 'classification' = 'restricted');

ALTER TABLE service_ops.silver_customers
  ALTER COLUMN facility_name SET TAGS ('pii' = 'false', 'classification' = 'internal');

ALTER TABLE service_ops.silver_customers
  ALTER COLUMN ssn_last4_contact SET MASK service_ops.mask_ssn;

-- ---------------------------------------------------------------------
-- 4. Certified tags on gold tables (Genie/AI/BI consumption hints)
-- ---------------------------------------------------------------------
ALTER VIEW suppliers.gold_supplier_performance
  SET TAGS ('certified' = 'true', 'domain' = 'suppliers', 'steward' = 'supply_chain_team');

ALTER VIEW suppliers.gold_spend_by_region
  SET TAGS ('certified' = 'true', 'domain' = 'suppliers', 'steward' = 'supply_chain_team');

ALTER VIEW parts.gold_part_pricing_benchmark
  SET TAGS ('certified' = 'true', 'domain' = 'parts', 'steward' = 'procurement_team');

ALTER VIEW parts.gold_reorder_needed
  SET TAGS ('certified' = 'true', 'domain' = 'parts', 'steward' = 'operations_team');

ALTER VIEW service_ops.gold_work_order_volume
  SET TAGS ('certified' = 'true', 'domain' = 'service_ops', 'steward' = 'service_team');

ALTER VIEW service_ops.gold_part_demand_monthly
  SET TAGS ('certified' = 'true', 'domain' = 'service_ops', 'steward' = 'planning_team');

ALTER VIEW service_ops.gold_sla_performance
  SET TAGS ('certified' = 'true', 'domain' = 'service_ops', 'steward' = 'service_team');
