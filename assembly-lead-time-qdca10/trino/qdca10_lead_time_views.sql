CREATE SCHEMA IF NOT EXISTS dataproducts.assembly_lead_time;

CREATE OR REPLACE VIEW dataproducts.assembly_lead_time.qdca10 AS
SELECT item_id, order_id, item, placed_at, fulfilled_at, lead_time_seconds, assembly_line
FROM iceberg.dataproducts.qdca10_lead_time;
