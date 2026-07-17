CREATE DATABASE IF NOT EXISTS iceberg.dataproducts;

CREATE TABLE IF NOT EXISTS iceberg.dataproducts.customer_360 (
    customer_id     VARCHAR,
    customer_name   VARCHAR,
    email           VARCHAR,
    loyalty_points  DOUBLE,
    last_reward_at  TIMESTAMP(3),
    last_order_id   VARCHAR,
    last_order_at   TIMESTAMP(3),
    total_orders    BIGINT,
    updated_at      TIMESTAMP(3)
)
WITH (
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS iceberg.dataproducts.customer_events_history (
    source_topic    VARCHAR,
    customer_id     VARCHAR,
    payload         VARCHAR,
    event_timestamp TIMESTAMP(3)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(event_timestamp)']
);
