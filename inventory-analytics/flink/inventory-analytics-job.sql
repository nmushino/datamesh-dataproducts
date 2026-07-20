-- Inventory Analytics Flink Job
-- component_stock_events (Drone Component Stock) と order_events (OrderEvents) を入力とする。
-- ドメイン内部の Postgres には一切アクセスしない。

CREATE TABLE component_stock_events_src (
    eventId        STRING,
    skuId          STRING,
    item           STRING,
    eventType      STRING,
    eventTimestamp TIMESTAMP(3),
    sourceTopic    STRING,
    WATERMARK FOR eventTimestamp AS eventTimestamp - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'component_stock_events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'inventory-analytics-flink',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'component-stock-events-value'
);

CREATE TABLE order_events_src (
    eventId         STRING,
    orderId         STRING,
    eventType       STRING,
    eventTimestamp  TIMESTAMP(3),
    orderStatus     STRING,
    lineItem        ROW<itemId STRING, item STRING, name STRING, price DECIMAL(10,2), lineItemStatus STRING, assemblyLine STRING>,
    WATERMARK FOR eventTimestamp AS eventTimestamp - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'dataproduct-order-events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'inventory-analytics-flink',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'order-events-value'
);

CREATE TABLE inventory_turnover (
    item                   STRING,
    windowStart            TIMESTAMP(3),
    windowEnd              TIMESTAMP(3),
    restockRequestedCount  BIGINT,
    stockoutCancelledCount BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'inventory_turnover',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'inventory-turnover-value'
);

CREATE TABLE stockout_risk (
    item                STRING,
    atRisk              BOOLEAN,
    lastStockoutEventAt TIMESTAMP(3),
    PRIMARY KEY (item) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'stockout_risk',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'key.format' = 'raw',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'stockout-risk-value'
);

-- =========================================================
-- 1時間窓: item 別 補充要求数 / 欠品キャンセル数
-- =========================================================
INSERT INTO inventory_turnover
SELECT
    COALESCE(r.item, c.item)          AS item,
    COALESCE(r.window_start, c.window_start) AS windowStart,
    COALESCE(r.window_end, c.window_end)     AS windowEnd,
    COALESCE(r.restockRequestedCount, 0)     AS restockRequestedCount,
    COALESCE(c.stockoutCancelledCount, 0)    AS stockoutCancelledCount
FROM (
    SELECT item, window_start, window_end, COUNT(*) AS restockRequestedCount
    FROM TABLE(TUMBLE(TABLE component_stock_events_src, DESCRIPTOR(eventTimestamp), INTERVAL '1' HOUR))
    WHERE eventType = 'RESTOCK_REQUESTED'
    GROUP BY window_start, window_end, item
) r
FULL OUTER JOIN (
    SELECT lineItem.item AS item, window_start, window_end, COUNT(*) AS stockoutCancelledCount
    FROM TABLE(TUMBLE(TABLE order_events_src, DESCRIPTOR(eventTimestamp), INTERVAL '1' HOUR))
    WHERE eventType = 'ORDER_CANCELLED'
    GROUP BY window_start, window_end, lineItem.item
) c
ON r.item = c.item AND r.window_start = c.window_start;

-- =========================================================
-- item ごとの欠品リスク (upsert): 直近1時間に欠品キャンセルがあれば atRisk=true
-- =========================================================
INSERT INTO stockout_risk
SELECT
    lineItem.item AS item,
    TRUE          AS atRisk,
    MAX(eventTimestamp) AS lastStockoutEventAt
FROM order_events_src
WHERE eventType = 'ORDER_CANCELLED'
GROUP BY lineItem.item;
