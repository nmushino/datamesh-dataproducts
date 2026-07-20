-- Assembly Lead Time QDCA10pro Flink Job
-- order_events (OrderEvents) の LINE_ITEM_STATUS_CHANGED を itemId 単位に
-- MATCH_RECOGNIZE で PLACED → FULFILLED のリードタイムを算出する (QDCA10pro 版)。

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
    'properties.group.id' = 'qdca10pro-lead-time-flink',
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

CREATE TABLE qdca10pro_lead_time (
    itemId          STRING,
    orderId         STRING,
    item            STRING,
    placedAt        TIMESTAMP(3),
    fulfilledAt     TIMESTAMP(3),
    leadTimeSeconds BIGINT,
    assemblyLine    STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'qdca10pro_lead_time',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'qdca10pro-lead-time-value'
);

INSERT INTO qdca10pro_lead_time
SELECT
    itemId,
    orderId,
    item,
    placedAt,
    fulfilledAt,
    TIMESTAMPDIFF(SECOND, placedAt, fulfilledAt) AS leadTimeSeconds,
    'QDCA10PRO' AS assemblyLine
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
        DEFINE
            P AS P.eventType = 'LINE_ITEM_STATUS_CHANGED' AND P.orderStatus = 'PLACED' AND P.liAssemblyLine = 'QDCA10PRO',
            I AS I.eventType = 'LINE_ITEM_STATUS_CHANGED' AND I.orderStatus = 'IN_PROGRESS' AND I.liAssemblyLine = 'QDCA10PRO',
            F AS F.eventType = 'LINE_ITEM_STATUS_CHANGED' AND F.orderStatus = 'FULFILLED' AND F.liAssemblyLine = 'QDCA10PRO'
    );
