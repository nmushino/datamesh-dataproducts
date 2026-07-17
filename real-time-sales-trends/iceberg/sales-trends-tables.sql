CREATE DATABASE IF NOT EXISTS iceberg.dataproducts;

CREATE TABLE IF NOT EXISTS iceberg.dataproducts.sales_trends_5m (
    item          VARCHAR,
    window_start  TIMESTAMP(3),
    window_end    TIMESTAMP(3),
    order_count   BIGINT,
    revenue       DECIMAL(12, 2),
    assembly_line VARCHAR,
    location      VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(window_start)']
);

CREATE TABLE IF NOT EXISTS iceberg.dataproducts.sales_trends_daily (
    item          VARCHAR,
    window_start  TIMESTAMP(3),
    window_end    TIMESTAMP(3),
    order_count   BIGINT,
    revenue       DECIMAL(12, 2),
    assembly_line VARCHAR,
    location      VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['month(window_start)']
);
