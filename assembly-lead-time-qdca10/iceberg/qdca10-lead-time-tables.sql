CREATE DATABASE IF NOT EXISTS iceberg.dataproducts;

CREATE TABLE IF NOT EXISTS iceberg.dataproducts.qdca10_lead_time (
    item_id           VARCHAR,
    order_id          VARCHAR,
    item              VARCHAR,
    placed_at         TIMESTAMP(3),
    fulfilled_at      TIMESTAMP(3),
    lead_time_seconds BIGINT,
    assembly_line     VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(fulfilled_at)']
);
