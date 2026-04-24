-- Create catalog + domain schemas + landing volume. Runs first. Idempotent.
-- :catalog — target catalog name.
-- On Default Storage workspaces, CREATE CATALOG without MANAGED LOCATION uses
-- the account-level default — which isn't reachable via the Catalog API, so
-- we provision via SQL instead.

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog)
  COMMENT 'PartsSource unified demo — seeded by the partssource-setup job';

USE CATALOG IDENTIFIER(:catalog);

CREATE SCHEMA IF NOT EXISTS raw              COMMENT 'Landing zone for raw CSV + JSON files';
CREATE SCHEMA IF NOT EXISTS suppliers        COMMENT 'Supplier domain — bronze/silver/gold supplier + purchase tables';
CREATE SCHEMA IF NOT EXISTS parts            COMMENT 'Parts domain — bronze/silver/gold parts + inventory + pricing';
CREATE SCHEMA IF NOT EXISTS service_ops      COMMENT 'Service Ops domain — bronze/silver/gold customers + work orders';
CREATE SCHEMA IF NOT EXISTS business_metrics COMMENT 'Certified Metric Views (YAML) — the semantic layer on gold';
CREATE SCHEMA IF NOT EXISTS reverse_etl      COMMENT 'Reverse-ETL tables synced out to operational stores (Lakebase)';

CREATE VOLUME IF NOT EXISTS raw.landing
  COMMENT 'Raw landing volume — seed task writes sharded CSV + JSON here';
