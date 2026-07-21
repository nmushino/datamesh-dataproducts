-- Customer 360 Flink Job
--
-- 【2026-07-21 再設計】旧版は postgresql-prod.dronedb.public.customers (Debezium CDC) /
-- rewards トピックを前提としていたが、いずれもこのリポジトリには実装されていない
-- (プロデューサが存在しない架空のトピックだった) ため、このジョブは一度も正常稼働
-- していなかった。実際に稼働している dataproduct-order-events (OrderEvents) のみを
-- ソースとして再設計する。dataproduct-order-events は asite (Web/Counter 発注) と
-- bsite (QDCA10/QDCA10pro 組立完了) の両方で発生したイベントを order-events Flink
-- ジョブが既に統合したハブであり、これ自体が「Aサイト・Bサイトで発生するイベントから
-- リアルタイムに顧客情報を作成する」という要件を満たす唯一の実在ソースである。
--
-- 顧客の識別子は loyaltyMemberId が入力されないことが多い (Web の rewardsId は
-- Optional) ため、customerName (注文者名) を実質的な統合キーとして使う。

CREATE TABLE order_events_src (
    orderId         STRING,
    eventType       STRING,
    eventTimestamp  TIMESTAMP(3),
    location        STRING,
    loyaltyMemberId STRING,
    lineItem        ROW<itemId STRING, item STRING, name STRING, price DECIMAL(10,2), lineItemStatus STRING, assemblyLine STRING>,
    WATERMARK FOR eventTimestamp AS eventTimestamp - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    -- order-events は asite でのみ稼働している。bsite/csite からは MirrorMaker2 で
    -- ミラーされたトピック名を参照する必要があるため、ジョブ投入時に
    -- ocpdeploy.sh がサイトに応じて解決する ORDER_EVENTS_TOPIC を使う
    -- (assembly-lead-time-qdca10 等と同じパターン)。
    'topic' = '${ORDER_EVENTS_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'customer-360-flink',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${ORDER_EVENTS_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'order-events-value'
);

CREATE TABLE customer_360 (
    customerName    STRING,
    loyaltyMemberId STRING,
    lastLocation    STRING,
    lastOrderId     STRING,
    lastOrderAt     TIMESTAMP(3),
    totalOrders     BIGINT,
    updatedAt       TIMESTAMP(3),
    PRIMARY KEY (customerName) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'dataproduct-customer-360',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'key.format' = 'raw',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'customer-360-value'
);

-- ORDER_PLACED イベントが来るたびに customerName ごとの統合プロファイルを
-- 更新する (無限 GROUP BY → upsert-kafka で受ける)。
INSERT INTO customer_360
SELECT
    lineItem.name                        AS customerName,
    -- loyaltyMemberId は同一顧客の注文間で NULL/値ありが混在しうるため、
    -- 直近 (最新イベント時刻) の非NULL値を優先する。
    LAST_VALUE(loyaltyMemberId)          AS loyaltyMemberId,
    LAST_VALUE(location)                 AS lastLocation,
    LAST_VALUE(orderId)                  AS lastOrderId,
    MAX(eventTimestamp)                  AS lastOrderAt,
    COUNT(DISTINCT orderId)              AS totalOrders,
    CURRENT_TIMESTAMP                    AS updatedAt
FROM order_events_src
WHERE eventType = 'ORDER_PLACED' AND lineItem.name IS NOT NULL AND lineItem.name <> ''
GROUP BY lineItem.name;

-- =========================================================
-- Iceberg (Lakekeeper REST Catalog) への書き込み。
-- upsert-kafka (上記) が生成する更新/削除ストリームはそのまま追記専用の
-- Iceberg シンクへは書けないため、ORDER_PLACED イベントをそのまま append する
-- 履歴テーブルとする (dataproducts.customer_360.profile_full/masked で
-- 「現在プロファイル」を window 関数により算出する)。
-- 同じソーストピックを再度読むため別の consumer group を使う。
-- =========================================================

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = '${ICEBERG_REST_CATALOG_URL}',
    'warehouse' = 'dataproducts'
);

CREATE TABLE order_events_src_for_iceberg (
    orderId         STRING,
    eventType       STRING,
    eventTimestamp  TIMESTAMP(3),
    location        STRING,
    loyaltyMemberId STRING,
    lineItem        ROW<itemId STRING, item STRING, name STRING, price DECIMAL(10,2), lineItemStatus STRING, assemblyLine STRING>,
    WATERMARK FOR eventTimestamp AS eventTimestamp - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${ORDER_EVENTS_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'customer-360-flink-iceberg',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${ORDER_EVENTS_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'order-events-value'
);

INSERT INTO iceberg_catalog.dataproducts.customer_events_history
SELECT
    lineItem.name   AS customer_name,
    loyaltyMemberId AS loyalty_member_id,
    location,
    orderId         AS order_id,
    eventTimestamp  AS event_timestamp
FROM order_events_src_for_iceberg
WHERE eventType = 'ORDER_PLACED' AND lineItem.name IS NOT NULL AND lineItem.name <> '';
