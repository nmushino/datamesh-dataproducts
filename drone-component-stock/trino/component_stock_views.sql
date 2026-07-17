CREATE SCHEMA IF NOT EXISTS dataproducts.component_stock;

CREATE OR REPLACE VIEW dataproducts.component_stock.stock_current AS
SELECT sku_id, item, latest_status, last_event_at
FROM iceberg.dataproducts.component_stock_current;

CREATE OR REPLACE VIEW dataproducts.component_stock.stock_events AS
SELECT event_id, sku_id, item, event_type, event_timestamp, source_topic
FROM iceberg.dataproducts.component_stock_events;
