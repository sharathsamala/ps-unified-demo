-- Create catalog + schemas + landing volume.
-- Runs before any other setup task. Idempotent (IF NOT EXISTS everywhere).
--
-- Parameters:
--   :catalog — target catalog name (e.g. partssource_demo_dev / partssource_demo)
--
-- Note on Default Storage workspaces:
--   On workspaces that have Default Storage enabled, CREATE CATALOG without
--   MANAGED LOCATION uses the account-level default. That path is not reachable
--   via the Catalog API from a DAB resource (which is why we run this via SQL).

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog)
  COMMENT 'PartsSource unified demo — seeded by the partssource_setup job';

-- Use the catalog for the remaining statements.
USE CATALOG IDENTIFIER(:catalog);

CREATE SCHEMA IF NOT EXISTS raw         COMMENT 'Landing zone for raw CSVs';
CREATE SCHEMA IF NOT EXISTS pipeline    COMMENT 'DLT target schema — bronze_*, silver_*, gold_* tables';
CREATE SCHEMA IF NOT EXISTS gold        COMMENT 'Certified metric views (mv_*) on top of pipeline.gold_*';
CREATE SCHEMA IF NOT EXISTS reverse_etl COMMENT 'Reverse-ETL tables synced out to operational stores (Lakebase)';

CREATE VOLUME IF NOT EXISTS raw.landing
  COMMENT 'CSV landing volume — the seed task writes generated data here';
