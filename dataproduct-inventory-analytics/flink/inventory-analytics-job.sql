-- Inventory Analytics Flink Job
-- dataproduct-inventory-events (Drone Component Stock, 旧名 component_stock_events) と
-- order_events (OrderEvents) を入力とする。ドメイン内部の Postgres には一切アクセスしない。

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
    'topic' = 'dataproduct-inventory-events',
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
--
-- 【修正 (2026-07-21)】以前は FULL OUTER JOIN で1item・1windowにつき1行へ
-- 結合してから 'kafka' (append-only専用) シンクへ INSERT していたが、
-- Flink の通常 JOIN オペレータは (入力がどちらも TUMBLE 集計後の append-only
-- ストリームであっても) OUTER JOIN である限り retract ストリームを生成するため、
-- "Table sink ... doesn't support consuming update and delete changes" で
-- ジョブがプラン化にすら失敗し、このデータプロダクトは一度も正常稼働していなかった。
-- JOIN をやめ、item・window ごとに「補充要求数の行」と「欠品キャンセル数の行」を
-- 別々に (もう片方は 0 で) append する形に変更し、append-only 制約を回避する。
-- 集計 (2種の部分行を item・window で合算) は下流 (Trino ビュー) で行う。
-- =========================================================
INSERT INTO inventory_turnover
SELECT
    item,
    window_start  AS windowStart,
    window_end    AS windowEnd,
    CAST(COUNT(*) AS BIGINT) AS restockRequestedCount,
    CAST(0 AS BIGINT)        AS stockoutCancelledCount
FROM TABLE(TUMBLE(TABLE component_stock_events_src, DESCRIPTOR(eventTimestamp), INTERVAL '1' HOUR))
WHERE eventType = 'RESTOCK_REQUESTED'
GROUP BY window_start, window_end, item;

INSERT INTO inventory_turnover
SELECT
    lineItem.item AS item,
    window_start  AS windowStart,
    window_end    AS windowEnd,
    CAST(0 AS BIGINT)        AS restockRequestedCount,
    CAST(COUNT(*) AS BIGINT) AS stockoutCancelledCount
FROM TABLE(TUMBLE(TABLE order_events_src, DESCRIPTOR(eventTimestamp), INTERVAL '1' HOUR))
WHERE eventType = 'ORDER_CANCELLED'
GROUP BY window_start, window_end, lineItem.item;

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

-- =========================================================
-- Iceberg (Lakekeeper REST Catalog) への書き込み。
-- 上の inventory_turnover / stockout_risk (Kafka) と同じ計算結果を
-- Iceberg テーブル (iceberg/inventory-analytics-tables.sql で事前に
-- Trino から CREATE TABLE 済みであること) へも書き込み、Trino から
-- SQL でクエリ可能にする。同じソーストピックを再度読むため
-- 別の consumer group (inventory-analytics-flink-iceberg) を使う。
-- =========================================================

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = '${ICEBERG_REST_CATALOG_URL}',
    'warehouse' = 'dataproducts'
);

CREATE TABLE component_stock_events_src_for_iceberg (
    eventId        STRING,
    skuId          STRING,
    item           STRING,
    eventType      STRING,
    eventTimestamp TIMESTAMP(3),
    sourceTopic    STRING,
    WATERMARK FOR eventTimestamp AS eventTimestamp - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'dataproduct-inventory-events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'inventory-analytics-flink-iceberg',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'component-stock-events-value'
);

CREATE TABLE order_events_src_for_iceberg (
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
    'properties.group.id' = 'inventory-analytics-flink-iceberg',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'order-events-value'
);

-- Kafka 側と同じ理由 (FULL OUTER JOIN は retract ストリームを生成し、
-- 素の Iceberg 追記シンクでは受け付けられない) で、JOIN せず部分行を
-- 2本の INSERT で append する。
INSERT INTO iceberg_catalog.dataproducts.inventory_turnover
SELECT
    item,
    window_start,
    window_end,
    CAST(COUNT(*) AS BIGINT) AS restock_requested_count,
    CAST(0 AS BIGINT)        AS stockout_cancelled_count
FROM TABLE(TUMBLE(TABLE component_stock_events_src_for_iceberg, DESCRIPTOR(eventTimestamp), INTERVAL '1' HOUR))
WHERE eventType = 'RESTOCK_REQUESTED'
GROUP BY window_start, window_end, item;

INSERT INTO iceberg_catalog.dataproducts.inventory_turnover
SELECT
    lineItem.item AS item,
    window_start,
    window_end,
    CAST(0 AS BIGINT)        AS restock_requested_count,
    CAST(COUNT(*) AS BIGINT) AS stockout_cancelled_count
FROM TABLE(TUMBLE(TABLE order_events_src_for_iceberg, DESCRIPTOR(eventTimestamp), INTERVAL '1' HOUR))
WHERE eventType = 'ORDER_CANCELLED'
GROUP BY window_start, window_end, lineItem.item;

-- stockout_risk (Kafka 側) は upsert-kafka で無限 GROUP BY の更新ストリームを
-- そのまま扱えるが、iceberg/inventory-analytics-tables.sql の stockout_risk は
-- upsert 設定 (primary key + write.upsert.enabled) をしていない素の Iceberg
-- テーブルのため、更新/削除を含む changelog はそのまま書き込めない。
-- 1時間窓で区切って append-only にした上で書き込む
-- (窓内で欠品キャンセルが1件でもあれば at_risk=true の行を1件 append する)。
INSERT INTO iceberg_catalog.dataproducts.stockout_risk
SELECT
    lineItem.item       AS item,
    TRUE                 AS at_risk,
    MAX(eventTimestamp) AS last_stockout_event_at
FROM TABLE(TUMBLE(TABLE order_events_src_for_iceberg, DESCRIPTOR(eventTimestamp), INTERVAL '1' HOUR))
WHERE eventType = 'ORDER_CANCELLED'
GROUP BY window_start, window_end, lineItem.item;
