-- Real-time Sales Trends Flink Job
-- OrderEvents (${ORDER_EVENTS_TOPIC}) の FULFILLED 明細を Item 別にウィンドウ集計する。
--
-- 前提: order-events-job.sql が稼働しており、dataproduct-order-events トピックが存在すること。
-- schema/sales-trend.avsc を Apicurio に group=dataproducts, artifactId=sales-trends-value として登録すること。
-- Kafka sink (dataproduct-sales-trends-5m / dataproduct-sales-trends-daily) は
-- shop-cluster が auto.create.topics.enable: false のため、ジョブ投入前に
-- openshift/dataproducts/dataproduct-sales-trends-topic.yaml で明示的に作成しておく必要がある。

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
    'topic' = '${ORDER_EVENTS_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'sales-trends-flink',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'avro-confluent',
    -- order-events は asite の Apicurio Registry でシリアライズされている。
    -- MirrorMaker2 はレコードのバイト列をそのままミラーするだけで
    -- schema-id は各サイトの Registry 間で共有されないため、デシリアライズには
    -- 実際にシリアライズした asite の Registry URL を使う必要がある
    -- (ミラー先サイト自身の Registry を使うと schema-id の意味が変わり壊れる)。
    'value.avro-confluent.url' = '${ORDER_EVENTS_REGISTRY_URL}/apis/ccompat/v6',
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
    'topic' = 'dataproduct-sales-trends-5m',
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
    'topic' = 'dataproduct-sales-trends-daily',
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

-- =========================================================
-- Iceberg (Lakekeeper REST Catalog) への書き込み。
-- 上の sales_trends_5m / sales_trends_daily (Kafka) と同じ計算結果を
-- Iceberg テーブル (iceberg/sales-trends-tables.sql で事前に
-- Trino から CREATE TABLE 済みであること) へも書き込み、Trino から
-- SQL でクエリ可能にする (trino/sales_trends_views.sql が参照する実体)。
-- 同じソーストピックを再度読むため別の consumer group
-- (sales-trends-flink-iceberg) を使う。
-- =========================================================

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = '${ICEBERG_REST_CATALOG_URL}',
    'warehouse' = 'dataproducts'
);

CREATE TABLE order_events_src_for_iceberg (
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
    'topic' = '${ORDER_EVENTS_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'sales-trends-flink-iceberg',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${ORDER_EVENTS_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'order-events-value'
);

INSERT INTO iceberg_catalog.dataproducts.sales_trends_5m
SELECT
    lineItem.item                              AS item,
    window_start,
    window_end,
    CAST(COUNT(*) AS BIGINT)                   AS order_count,
    CAST(SUM(lineItem.price) AS DECIMAL(12,2)) AS revenue,
    lineItem.assemblyLine                      AS assembly_line,
    location
FROM TABLE(
    TUMBLE(TABLE order_events_src_for_iceberg, DESCRIPTOR(eventTimestamp), INTERVAL '5' MINUTES)
)
WHERE eventType = 'LINE_ITEM_STATUS_CHANGED' AND orderStatus = 'FULFILLED'
GROUP BY window_start, window_end, lineItem.item, lineItem.assemblyLine, location;

INSERT INTO iceberg_catalog.dataproducts.sales_trends_daily
SELECT
    lineItem.item                              AS item,
    window_start,
    window_end,
    CAST(COUNT(*) AS BIGINT)                   AS order_count,
    CAST(SUM(lineItem.price) AS DECIMAL(12,2)) AS revenue,
    lineItem.assemblyLine                      AS assembly_line,
    location
FROM TABLE(
    TUMBLE(TABLE order_events_src_for_iceberg, DESCRIPTOR(eventTimestamp), INTERVAL '1' DAY)
)
WHERE eventType = 'LINE_ITEM_STATUS_CHANGED' AND orderStatus = 'FULFILLED'
GROUP BY window_start, window_end, lineItem.item, lineItem.assemblyLine, location;
