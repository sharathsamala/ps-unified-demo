-- Metric Views — semantic layer on top of gold. Consumed by AI/BI, Genie, and SQL users.
-- :param catalog

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA gold;

---------------------------------------------------------
-- Supplier metrics
---------------------------------------------------------
CREATE OR REPLACE VIEW mv_supplier_spend AS
WITH base AS (
  SELECT
    supplier_id,
    name           AS supplier_name,
    region,
    tier,
    composite_score,
    total_spend_usd,
    purchase_count,
    avg_net_price_usd,
    on_time_rate,
    defect_rate_ppm,
    last_purchase_at
  FROM pipeline.gold_supplier_performance
)
SELECT
  supplier_id,
  supplier_name,
  region,
  tier,
  ROUND(composite_score, 3)     AS composite_score,
  ROUND(on_time_rate, 3)        AS on_time_rate,
  defect_rate_ppm,
  ROUND(total_spend_usd, 2)     AS total_spend_usd,
  purchase_count,
  ROUND(avg_net_price_usd, 2)   AS avg_net_price_usd,
  last_purchase_at
FROM base;

COMMENT ON VIEW mv_supplier_spend IS
'Supplier spend + performance semantic view. Use for QBR decks, Genie, and AI/BI dashboards. Certified.';

---------------------------------------------------------
-- Demand metrics
---------------------------------------------------------
CREATE OR REPLACE VIEW mv_part_demand AS
SELECT
  part_id,
  sku,
  name             AS part_name,
  category,
  manufacturer,
  month,
  work_orders,
  ROUND(avg_duration_hours, 2) AS avg_duration_hours
FROM pipeline.gold_part_demand_monthly;

COMMENT ON VIEW mv_part_demand IS
'Monthly part demand with work-order volume + avg repair duration. Used by demand planning + Genie.';

---------------------------------------------------------
-- Pricing benchmark
---------------------------------------------------------
CREATE OR REPLACE VIEW mv_pricing_benchmark AS
SELECT
  part_id,
  sku,
  part_name,
  category,
  manufacturer,
  list_price_usd,
  best_paid_usd,
  avg_paid_usd,
  ROUND(savings_vs_list_pct * 100, 2) AS savings_vs_list_pct,
  last_purchase_at
FROM (
  SELECT
    part_id, sku, name AS part_name, category, manufacturer,
    list_price_usd, best_paid_usd, avg_paid_usd, savings_vs_list_pct, last_purchase_at
  FROM pipeline.gold_part_pricing_benchmark
);

COMMENT ON VIEW mv_pricing_benchmark IS
'Parts with list vs actual-paid pricing. Surfaces procurement savings opportunities. Certified.';

---------------------------------------------------------
-- Operations view — the one Genie is pointed at
---------------------------------------------------------
CREATE OR REPLACE VIEW mv_reorder_needed AS
SELECT
  r.part_id,
  r.sku,
  r.name            AS part_name,
  r.category,
  r.warehouse,
  r.on_hand_qty,
  r.reorder_point,
  r.recommended_supplier_name,
  p.avg_paid_usd,
  p.list_price_usd,
  r.generated_at
FROM pipeline.gold_reorder_recommendations r
LEFT JOIN pipeline.gold_part_pricing_benchmark p USING (part_id);

COMMENT ON VIEW mv_reorder_needed IS
'Parts at or below reorder point + recommended supplier + benchmark price. Feeds ops dashboard + Genie.';

---------------------------------------------------------
-- Certify all metric views
---------------------------------------------------------
ALTER VIEW mv_supplier_spend     SET TAGS ('certified' = 'true', 'metric_view' = 'true');
ALTER VIEW mv_part_demand        SET TAGS ('certified' = 'true', 'metric_view' = 'true');
ALTER VIEW mv_pricing_benchmark  SET TAGS ('certified' = 'true', 'metric_view' = 'true');
ALTER VIEW mv_reorder_needed     SET TAGS ('certified' = 'true', 'metric_view' = 'true');
