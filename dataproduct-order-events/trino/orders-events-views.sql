-- Trino 公開ビュー: dataproducts.order_events
--
-- 他ドメイン・RHDH・AI Agent からのアクセスは、
-- 必ずこのスキーマ配下のビュー経由とする。
-- iceberg.dataproducts.order_events_* テーブルへの直接権限は付与しない。

CREATE SCHEMA IF NOT EXISTS dataproducts.order_events;

CREATE OR REPLACE VIEW dataproducts.order_events.orders_current AS
SELECT
    order_id,
    order_status,
    order_source,
    location,
    loyalty_member_id,
    placed_at,
    last_updated_at,
    assembly_line
FROM iceberg.dataproducts.order_events_current;

CREATE OR REPLACE VIEW dataproducts.order_events.order_events_history AS
SELECT
    event_id,
    order_id,
    event_type,
    event_timestamp,
    order_source,
    location,
    loyalty_member_id,
    order_status,
    line_item_id,
    line_item_name,
    line_item_price,
    line_item_status,
    assembly_line,
    source_domain,
    source_topic
FROM iceberg.dataproducts.order_events_history;
