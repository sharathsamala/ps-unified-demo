-- Publish reorder alerts and supplier tier changes to reverse_etl tables.
-- Downstream consumers (Lakebase, Postgres syncs, operational apps) read these.
-- :catalog is supplied as a SQL task parameter.

USE CATALOG IDENTIFIER(:catalog);

-- Reorder alerts — one row per (part, warehouse) currently below reorder.
CREATE OR REPLACE TABLE reverse_etl.reorder_alerts_feed AS
SELECT
  part_id,
  sku,
  part_name,
  category,
  warehouse,
  on_hand_qty,
  reorder_point,
  shortfall_qty,
  list_price_usd,
  CAST(shortfall_qty * list_price_usd AS DECIMAL(18, 2)) AS est_reorder_value_usd,
  current_timestamp() AS generated_at
FROM parts.gold_reorder_needed
WHERE shortfall_qty > 10;

ALTER TABLE reverse_etl.reorder_alerts_feed
  SET TAGS ('reverse_etl' = 'true', 'sink' = 'lakebase', 'domain' = 'parts');

-- Supplier tier feed — downstream operational store uses this for scorecards.
CREATE OR REPLACE TABLE reverse_etl.supplier_tier_feed AS
SELECT
  supplier_id,
  supplier_name,
  region,
  tier,
  composite_score,
  on_time_rate,
  total_spend_usd,
  CASE
    WHEN composite_score >= 0.85 THEN 'Preferred'
    WHEN composite_score >= 0.70 THEN 'Approved'
    ELSE 'Probation'
  END AS recommended_tier,
  current_timestamp() AS generated_at
FROM suppliers.gold_supplier_performance
WHERE total_spend_usd IS NOT NULL;

ALTER TABLE reverse_etl.supplier_tier_feed
  SET TAGS ('reverse_etl' = 'true', 'sink' = 'lakebase', 'domain' = 'suppliers');
