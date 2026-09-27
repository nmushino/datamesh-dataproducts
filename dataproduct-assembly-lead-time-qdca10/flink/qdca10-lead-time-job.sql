-- Assembly Lead Time QDCA10 Flink Job
-- order_events (OrderEvents) の LINE_ITEM_STATUS_CHANGED を itemId 単位に
-- MATCH_RECOGNIZE で PLACED → FULFILLED のリードタイムを算出する。

CREATE TABLE order_events_src (
    eventId         STRING,
    orderId         STRING,
    eventType       STRING,
    eventTimestamp  TIMESTAMP(3),
    orderStatus     STRING,
    lineItem        ROW<itemId STRING, item STRING, name STRING, price DECIMAL(10,2), lineItemStatus STRING, assemblyLine STRING>,
    WATERMARK FOR eventTimestamp AS eventTimestamp - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${ORDER_EVENTS_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'qdca10-lead-time-flink',
    'scan.startup.mode' = 'earliest-offset',
    -- データが疎らな(あるいは全く来ない)パーティションが1つでもあると watermark
    -- 全体がそこでブロックされ MATCH_RECOGNIZE が永久に emit されなくなる
    -- (sales-trends-job.sql / orders-events-job.sql と同じ既知の問題)。
    'scan.watermark.idle-timeout' = '30s',
    'value.format' = 'avro-confluent',
    -- order-events は asite の Apicurio Registry でシリアライズされている。
    -- MirrorMaker2 はレコードのバイト列をそのままミラーするだけで
    -- schema-id は各サイトの Registry 間で共有されないため、デシリアライズには
    -- 実際にシリアライズした asite の Registry URL を使う必要がある
    -- (ミラー先サイト自身の Registry を使うと schema-id の意味が変わり壊れる)。
    'value.avro-confluent.url' = '${ORDER_EVENTS_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'order-events-value'
);

CREATE TABLE qdca10_lead_time (
    itemId          STRING,
    orderId         STRING,
    item            STRING,
    placedAt        TIMESTAMP(3),
    fulfilledAt     TIMESTAMP(3),
    leadTimeSeconds BIGINT,
    assemblyLine    STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'dataproduct-assembly-lead-time-qdca10',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'qdca10-lead-time-value'
);

INSERT INTO qdca10_lead_time
SELECT
    itemId,
    orderId,
    item,
    placedAt,
    fulfilledAt,
    TIMESTAMPDIFF(SECOND, placedAt, fulfilledAt) AS leadTimeSeconds,
    'QDCA10' AS assemblyLine
-- MATCH_RECOGNIZE 内でのネストした ROW フィールド (lineItem.itemId 等) への
-- ドット参照は Calcite のパーサ/バリデータで解決に失敗することがあるため、
-- 事前にサブクエリでトップレベルの列へフラット化してから MATCH_RECOGNIZE する。
FROM (
    SELECT
        orderId,
        eventType,
        orderStatus,
        lineItem.itemId       AS liItemId,
        lineItem.item         AS liItem,
        lineItem.assemblyLine AS liAssemblyLine,
        eventTimestamp
    FROM order_events_src
)
    MATCH_RECOGNIZE (
        PARTITION BY liItemId
        ORDER BY eventTimestamp
        MEASURES
            P.liItemId AS itemId,
            P.orderId AS orderId,
            P.liItem AS item,
            P.eventTimestamp AS placedAt,
            F.eventTimestamp AS fulfilledAt
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (P I? F)
        -- 【2026-08-04 修正】orders-events-job.sql が実際に発行するイベントは
        -- ORDER_PLACED (eventType) / PLACED (orderStatus) であり、
        -- LINE_ITEM_STATUS_CHANGED になるのは FULFILLED (orders-up 由来) のみ。
        -- P の条件が eventType='LINE_ITEM_STATUS_CHANGED' のままだと永久に
        -- マッチせず、Average OrderUp Time が常に空になっていた。
        DEFINE
            P AS P.eventType = 'ORDER_PLACED' AND P.orderStatus = 'PLACED' AND P.liAssemblyLine = 'QDCA10',
            I AS I.eventType = 'LINE_ITEM_STATUS_CHANGED' AND I.orderStatus = 'IN_PROGRESS' AND I.liAssemblyLine = 'QDCA10',
            F AS F.eventType = 'LINE_ITEM_STATUS_CHANGED' AND F.orderStatus = 'FULFILLED' AND F.liAssemblyLine = 'QDCA10'
    );
