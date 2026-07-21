CREATE DATABASE IF NOT EXISTS iceberg.dataproducts;

CREATE TABLE IF NOT EXISTS iceberg.dataproducts.inventory_turnover (
    item                     VARCHAR,
    window_start             TIMESTAMP(3),
    window_end               TIMESTAMP(3),
    restock_requested_count  BIGINT,
    stockout_cancelled_count BIGINT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(window_start)']
);

CREATE TABLE IF NOT EXISTS iceberg.dataproducts.stockout_risk (
    item                    VARCHAR,
    at_risk                 BOOLEAN,
    last_stockout_event_at  TIMESTAMP(3)
)
WITH (
    format = 'PARQUET'
);
