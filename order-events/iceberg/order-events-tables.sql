-- Iceberg テーブル定義 (Flink SQL / Trino いずれからも実行可能な DDL)
-- カタログ: iceberg, データベース: dataproducts

CREATE DATABASE IF NOT EXISTS iceberg.dataproducts;

-- 全 OrderEvent を append-only で保持する履歴テーブル。
-- 監査・Time travel・他プロダクトからの再集計のソースとなる。
CREATE TABLE IF NOT EXISTS iceberg.dataproducts.order_events_history (
    event_id          VARCHAR,
    order_id          VARCHAR,
    event_type        VARCHAR,
    event_timestamp   TIMESTAMP(3),
    order_source      VARCHAR,
    location          VARCHAR,
    loyalty_member_id VARCHAR,
    order_status      VARCHAR,
    line_item_id      VARCHAR,
    line_item_name    VARCHAR,
    line_item_price   DECIMAL(10, 2),
    line_item_status  VARCHAR,
    assembly_line     VARCHAR,
    source_domain     VARCHAR,
    source_topic      VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(event_timestamp)', 'event_type']
);

-- 注文単位の最新状態 (upsert)。
-- order_id が Iceberg v2 の identifier field となり、
-- Flink の Iceberg upsert sink (write.upsert.enabled=true) から更新する。
CREATE TABLE IF NOT EXISTS iceberg.dataproducts.order_events_current (
    order_id          VARCHAR,
    order_status      VARCHAR,
    order_source      VARCHAR,
    location          VARCHAR,
    loyalty_member_id VARCHAR,
    placed_at         TIMESTAMP(3),
    last_updated_at   TIMESTAMP(3),
    assembly_line     VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['location']
);
