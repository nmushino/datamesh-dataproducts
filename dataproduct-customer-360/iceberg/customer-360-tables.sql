CREATE SCHEMA IF NOT EXISTS iceberg.dataproducts;

-- 【2026-07-21 再設計】Flink の upsert-kafka (customer_360 の無限 GROUP BY) が
-- 生成する更新/削除ストリームは、素の追記専用 Iceberg シンクへは書き込めない
-- (dataproduct-inventory-analytics で判明した制約と同じ)。そのため Iceberg 側は
-- 集計済みの「現在プロファイル」ではなく、ORDER_PLACED イベントをそのまま
-- append する履歴テーブルとする。「現在プロファイル」は Trino ビュー側で
-- window 関数により履歴から都度算出する (dataproducts.customer_360.profile_full /
-- profile_masked を参照)。
CREATE TABLE IF NOT EXISTS iceberg.dataproducts.customer_events_history (
    customer_name     VARCHAR,
    loyalty_member_id VARCHAR,
    location          VARCHAR,
    order_id          VARCHAR,
    event_timestamp   TIMESTAMP(3)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(event_timestamp)']
);
