-- Real-time Sales Trends Flink Job
-- OrderEvents (order_events トピック) の FULFILLED 明細を Item 別にウィンドウ集計する。
--
-- 前提: order-events-job.sql が稼働しており、order_events トピックが存在すること。
-- schema/sales-trend.avsc を Apicurio に group=dataproducts, artifactId=sales-trends-value として登録すること。

CREATE TABLE order_events_src (
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
    sourceTopic     STRING,
    WATERMARK FOR eventTimestamp AS eventTimestamp - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'order_events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'sales-trends-flink',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'order-events-value'
);

CREATE TABLE sales_trends_5m (
    item         STRING,
    windowStart  TIMESTAMP(3),
    windowEnd    TIMESTAMP(3),
    orderCount   BIGINT,
    revenue      DECIMAL(12,2),
    assemblyLine STRING,
    location     STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'sales_trends_5m',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'sales-trends-value'
);

CREATE TABLE sales_trends_daily (
    item         STRING,
    windowStart  TIMESTAMP(3),
    windowEnd    TIMESTAMP(3),
    orderCount   BIGINT,
    revenue      DECIMAL(12,2),
    assemblyLine STRING,
    location     STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'sales_trends_daily',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'sales-trends-value'
);

-- 5分 Tumbling Window
INSERT INTO sales_trends_5m
SELECT
    lineItem.item                          AS item,
    window_start                           AS windowStart,
    window_end                             AS windowEnd,
    COUNT(*)                               AS orderCount,
    CAST(SUM(lineItem.price) AS DECIMAL(12,2)) AS revenue,
    lineItem.assemblyLine                  AS assemblyLine,
    location                               AS location
FROM TABLE(
    TUMBLE(TABLE order_events_src, DESCRIPTOR(eventTimestamp), INTERVAL '5' MINUTES)
)
WHERE eventType = 'LINE_ITEM_STATUS_CHANGED' AND orderStatus = 'FULFILLED'
GROUP BY window_start, window_end, lineItem.item, lineItem.assemblyLine, location;

-- 日次 Tumbling Window
INSERT INTO sales_trends_daily
SELECT
    lineItem.item                          AS item,
    window_start                           AS windowStart,
    window_end                             AS windowEnd,
    COUNT(*)                               AS orderCount,
    CAST(SUM(lineItem.price) AS DECIMAL(12,2)) AS revenue,
    lineItem.assemblyLine                  AS assemblyLine,
    location                               AS location
FROM TABLE(
    TUMBLE(TABLE order_events_src, DESCRIPTOR(eventTimestamp), INTERVAL '1' DAY)
)
WHERE eventType = 'LINE_ITEM_STATUS_CHANGED' AND orderStatus = 'FULFILLED'
GROUP BY window_start, window_end, lineItem.item, lineItem.assemblyLine, location;
