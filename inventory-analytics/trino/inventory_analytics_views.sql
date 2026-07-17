CREATE SCHEMA IF NOT EXISTS dataproducts.inventory_analytics;

CREATE OR REPLACE VIEW dataproducts.inventory_analytics.turnover AS
SELECT item, window_start, window_end, restock_requested_count, stockout_cancelled_count
FROM iceberg.dataproducts.inventory_turnover;

CREATE OR REPLACE VIEW dataproducts.inventory_analytics.stockout_risk AS
SELECT item, at_risk, last_stockout_event_at
FROM iceberg.dataproducts.stockout_risk;
