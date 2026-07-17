-- OrderEvents Flink Job
-- ドメインの生イベント(orders-in / orders-up / eighty-six)を正規化し、
-- `order_events` トピック(Avro, Apicurio 登録)へ再公開する。
--
-- 前提: Apicurio Service Registry に schema/order-event.avsc を
--   group=dataproducts, artifactId=order-events-value として登録済みであること。

-- =========================================================
-- ソース: 各ドメインの生トピック
-- =========================================================

CREATE TABLE orders_in (
    id             STRING,
    orderSource    STRING,
    location       STRING,
    rewardsId      STRING,
    QDCA10Items    ARRAY<ROW<itemId STRING, item STRING, name STRING, price DECIMAL(10,2)>>,
    QDCA10ProItems ARRAY<ROW<itemId STRING, item STRING, name STRING, price DECIMAL(10,2)>>,
    event_time     TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders-in',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'order-events-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE orders_up (
    orderId        STRING,
    itemId         STRING,
    lineItemStatus STRING,
    assemblyLine   STRING,
    event_time     TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders-up',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'order-events-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE eighty_six (
    orderId    STRING,
    item       STRING,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'eighty-six',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'order-events-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- =========================================================
-- シンク: order_events (Avro, Apicurio Service Registry)
-- =========================================================

CREATE TABLE order_events (
    eventId         STRING,
    orderId         STRING,
    eventType       STRING,
    eventTimestamp  TIMESTAMP(3),
    orderSource     STRING,
    location        STRING,
    loyaltyMemberId STRING,
    orderStatus     STRING,
    lineItem        ROW<
        itemId STRING,
        item STRING,
        name STRING,
        price DECIMAL(10,2),
        lineItemStatus STRING,
        assemblyLine STRING
    >,
    sourceDomain    STRING,
    sourceTopic     STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'order_events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'key.format' = 'raw',
    'key.fields' = 'orderId',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'order-events-value'
);

-- =========================================================
-- 1. ORDER_PLACED (ヘッダー) : orders-in から
-- =========================================================
INSERT INTO order_events
SELECT
    UUID()                                             AS eventId,
    o.id                                                AS orderId,
    'ORDER_PLACED'                                      AS eventType,
    o.event_time                                        AS eventTimestamp,
    o.orderSource                                       AS orderSource,
    o.location                                          AS location,
    o.rewardsId                                         AS loyaltyMemberId,
    'PLACED'                                             AS orderStatus,
    CAST(NULL AS ROW<itemId STRING, item STRING, name STRING, price DECIMAL(10,2), lineItemStatus STRING, assemblyLine STRING>) AS lineItem,
    'counter'                                           AS sourceDomain,
    'orders-in'                                         AS sourceTopic
FROM orders_in AS o;

-- =========================================================
-- 2. LINE_ITEM_STATUS_CHANGED (明細) : orders-up から
-- =========================================================
INSERT INTO order_events
SELECT
    UUID()                                              AS eventId,
    u.orderId                                           AS orderId,
    'LINE_ITEM_STATUS_CHANGED'                          AS eventType,
    u.event_time                                        AS eventTimestamp,
    CAST(NULL AS STRING)                                AS orderSource,
    CAST(NULL AS STRING)                                AS location,
    CAST(NULL AS STRING)                                AS loyaltyMemberId,
    u.lineItemStatus                                    AS orderStatus,
    ROW(u.itemId, CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS DECIMAL(10,2)), u.lineItemStatus, u.assemblyLine) AS lineItem,
    LOWER(u.assemblyLine)                               AS sourceDomain,
    'orders-up'                                         AS sourceTopic
FROM orders_up AS u;

-- =========================================================
-- 3. ORDER_CANCELLED (欠品) : eighty-six から
-- =========================================================
INSERT INTO order_events
SELECT
    UUID()                                              AS eventId,
    e.orderId                                           AS orderId,
    'ORDER_CANCELLED'                                   AS eventType,
    e.event_time                                        AS eventTimestamp,
    CAST(NULL AS STRING)                                AS orderSource,
    CAST(NULL AS STRING)                                AS location,
    CAST(NULL AS STRING)                                AS loyaltyMemberId,
    'CANCELLED'                                          AS orderStatus,
    ROW(CAST(NULL AS STRING), e.item, CAST(NULL AS STRING), CAST(NULL AS DECIMAL(10,2)), CAST(NULL AS STRING), CAST(NULL AS STRING)) AS lineItem,
    'qdca10'                                            AS sourceDomain,
    'eighty-six'                                        AS sourceTopic
FROM eighty_six AS e;
