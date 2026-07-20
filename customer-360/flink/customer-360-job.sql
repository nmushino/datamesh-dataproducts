-- Customer 360 Flink Job
-- Postgres CDC(customers) をベースに、loyalty-updates / rewards / order_events(OrderEvents) を
-- customer_id (= loyaltyMemberId) で突き合わせ、customer_360 (upsert) として再公開する。

CREATE TABLE customers_cdc (
    customer_id STRING,
    name        STRING,
    email       STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'postgresql-prod.dronedb.public.customers',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'customer-360-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

CREATE TABLE rewards_src (
    customerName STRING,
    orderId      STRING,
    rewardAmount DOUBLE,
    event_time   TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'rewards',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'customer-360-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE order_events_src (
    orderId         STRING,
    eventType       STRING,
    eventTimestamp  TIMESTAMP(3),
    loyaltyMemberId STRING,
    WATERMARK FOR eventTimestamp AS eventTimestamp - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'dataproduct-order-events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'customer-360-flink',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'order-events-value'
);

CREATE TABLE customer_360 (
    customerId     STRING,
    customerName   STRING,
    email          STRING,
    loyaltyPoints  DOUBLE,
    lastRewardAt   TIMESTAMP(3),
    lastOrderId    STRING,
    lastOrderAt    TIMESTAMP(3),
    totalOrders    BIGINT,
    updatedAt      TIMESTAMP(3),
    PRIMARY KEY (customerId) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'customer_360',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'key.format' = 'raw',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'customer-360-value'
);

-- 顧客マスタをベースに、直近の注文(loyaltyMemberId で紐付け)を付加する。
-- rewards / loyalty-updates によるポイント更新は別途 CEP/集計ジョブで loyaltyPoints を積み上げる
-- (本ジョブでは直近の付与実績のみを反映する簡易版とする)。
INSERT INTO customer_360
SELECT
    c.customer_id                          AS customerId,
    c.name                                 AS customerName,
    c.email                                AS email,
    COALESCE(r.rewardAmount, 0.0)          AS loyaltyPoints,
    r.event_time                           AS lastRewardAt,
    o.orderId                              AS lastOrderId,
    o.eventTimestamp                       AS lastOrderAt,
    o.totalOrders                          AS totalOrders,
    CURRENT_TIMESTAMP                      AS updatedAt
FROM customers_cdc c
LEFT JOIN rewards_src r
    ON c.name = r.customerName
LEFT JOIN (
    SELECT
        loyaltyMemberId,
        LAST_VALUE(orderId) AS orderId,
        MAX(eventTimestamp) AS eventTimestamp,
        COUNT(DISTINCT orderId) AS totalOrders
    FROM order_events_src
    WHERE eventType = 'ORDER_PLACED' AND loyaltyMemberId IS NOT NULL
    GROUP BY loyaltyMemberId
) o
    ON c.customer_id = o.loyaltyMemberId;
