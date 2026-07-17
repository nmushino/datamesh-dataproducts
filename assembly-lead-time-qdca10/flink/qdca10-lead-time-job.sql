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
    'topic' = 'order_events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'qdca10-lead-time-flink',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
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
    'topic' = 'qdca10_lead_time',
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
FROM order_events_src
    MATCH_RECOGNIZE (
        PARTITION BY lineItem.itemId
        ORDER BY eventTimestamp
        MEASURES
            lineItem.itemId AS itemId,
            orderId AS orderId,
            lineItem.item AS item,
            P.eventTimestamp AS placedAt,
            F.eventTimestamp AS fulfilledAt
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (P I? F)
        DEFINE
            P AS P.eventType = 'LINE_ITEM_STATUS_CHANGED' AND P.orderStatus = 'PLACED' AND P.lineItem.assemblyLine = 'QDCA10',
            I AS I.eventType = 'LINE_ITEM_STATUS_CHANGED' AND I.orderStatus = 'IN_PROGRESS' AND I.lineItem.assemblyLine = 'QDCA10',
            F AS F.eventType = 'LINE_ITEM_STATUS_CHANGED' AND F.orderStatus = 'FULFILLED' AND F.lineItem.assemblyLine = 'QDCA10'
    );
