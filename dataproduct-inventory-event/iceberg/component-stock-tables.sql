CREATE DATABASE IF NOT EXISTS iceberg.dataproducts;

CREATE TABLE IF NOT EXISTS iceberg.dataproducts.component_stock_events (
    event_id        VARCHAR,
    sku_id          VARCHAR,
    item            VARCHAR,
    event_type      VARCHAR,
    event_timestamp TIMESTAMP(3),
    source_topic    VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(event_timestamp)']
);

CREATE TABLE IF NOT EXISTS iceberg.dataproducts.component_stock_current (
    sku_id         VARCHAR,
    item           VARCHAR,
    latest_status  VARCHAR,
    last_event_at  TIMESTAMP(3)
)
WITH (
    format = 'PARQUET'
);
