CREATE SCHEMA IF NOT EXISTS dataproducts.assembly_lead_time;

CREATE OR REPLACE VIEW dataproducts.assembly_lead_time.qdca10pro AS
SELECT item_id, order_id, item, placed_at, fulfilled_at, lead_time_seconds, assembly_line
FROM iceberg.dataproducts.qdca10pro_lead_time;

-- 機種間比較用の統合ビュー
CREATE OR REPLACE VIEW dataproducts.assembly_lead_time.all_lines AS
SELECT * FROM dataproducts.assembly_lead_time.qdca10
UNION ALL
SELECT * FROM dataproducts.assembly_lead_time.qdca10pro;
