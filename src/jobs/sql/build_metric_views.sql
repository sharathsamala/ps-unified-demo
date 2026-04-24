-- Build Metric Views (YAML) in `business_metrics` schema.
-- Runs after all three domain SDP pipelines complete.
-- :catalog is supplied by the job as a SQL task parameter.

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA business_metrics;

-- ------------------------------------------------------------------
-- Supplier spend
-- ------------------------------------------------------------------
CREATE OR REPLACE VIEW mv_supplier_spend
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1
source: ${catalog}.suppliers.gold_supplier_performance
dimensions:
  - name: supplier_id
    expr: supplier_id
  - name: supplier_name
    expr: supplier_name
  - name: region
    expr: region
  - name: tier
    expr: tier
measures:
  - name: total_spend_usd
    expr: SUM(total_spend_usd)
  - name: avg_composite_score
    expr: AVG(composite_score)
  - name: avg_on_time_rate
    expr: AVG(on_time_rate)
  - name: supplier_count
    expr: COUNT(DISTINCT supplier_id)
$$;

ALTER VIEW mv_supplier_spend SET TAGS ('certified' = 'true', 'metric_view' = 'true', 'domain' = 'suppliers');


-- ------------------------------------------------------------------
-- Pricing benchmark
-- ------------------------------------------------------------------
CREATE OR REPLACE VIEW mv_pricing_benchmark
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1
source: ${catalog}.parts.gold_part_pricing_benchmark
dimensions:
  - name: part_id
    expr: part_id
  - name: sku
    expr: sku
  - name: category
    expr: category
  - name: manufacturer
    expr: manufacturer
measures:
  - name: avg_list_price
    expr: AVG(list_price_usd)
  - name: avg_paid_price
    expr: AVG(avg_paid_usd)
  - name: avg_savings_pct
    expr: AVG(savings_vs_list_pct)
  - name: part_count
    expr: COUNT(DISTINCT part_id)
$$;

ALTER VIEW mv_pricing_benchmark SET TAGS ('certified' = 'true', 'metric_view' = 'true', 'domain' = 'parts');


-- ------------------------------------------------------------------
-- Reorder needed
-- ------------------------------------------------------------------
CREATE OR REPLACE VIEW mv_reorder_needed
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1
source: ${catalog}.parts.gold_reorder_needed
dimensions:
  - name: part_id
    expr: part_id
  - name: sku
    expr: sku
  - name: category
    expr: category
  - name: warehouse
    expr: warehouse
measures:
  - name: open_reorders
    expr: COUNT(*)
  - name: total_shortfall_qty
    expr: SUM(shortfall_qty)
  - name: estimated_reorder_value_usd
    expr: SUM(shortfall_qty * list_price_usd)
$$;

ALTER VIEW mv_reorder_needed SET TAGS ('certified' = 'true', 'metric_view' = 'true', 'domain' = 'parts');


-- ------------------------------------------------------------------
-- Part demand (monthly)
-- ------------------------------------------------------------------
CREATE OR REPLACE VIEW mv_part_demand
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1
source: ${catalog}.service_ops.gold_part_demand_monthly
dimensions:
  - name: part_id
    expr: part_id
  - name: month
    expr: month
measures:
  - name: work_orders
    expr: SUM(work_orders)
  - name: avg_duration_hours
    expr: AVG(avg_duration_hours)
$$;

ALTER VIEW mv_part_demand SET TAGS ('certified' = 'true', 'metric_view' = 'true', 'domain' = 'service_ops');


-- ------------------------------------------------------------------
-- SLA performance
-- ------------------------------------------------------------------
CREATE OR REPLACE VIEW mv_sla_performance
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1
source: ${catalog}.service_ops.gold_sla_performance
dimensions:
  - name: priority
    expr: priority
measures:
  - name: closed_orders
    expr: SUM(closed_orders)
  - name: orders_meeting_sla
    expr: SUM(orders_meeting_sla)
  - name: sla_pct
    expr: AVG(sla_pct)
  - name: avg_duration_hours
    expr: AVG(avg_duration_hours)
$$;

ALTER VIEW mv_sla_performance SET TAGS ('certified' = 'true', 'metric_view' = 'true', 'domain' = 'service_ops');
