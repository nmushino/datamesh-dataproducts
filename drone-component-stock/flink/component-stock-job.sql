-- Drone Component Stock Flink Job
-- inventory-out (RestockRequestedEvent / RestockCompletedEvent) を skuId 単位に正規化し、
-- component_stock_events トピックへ再公開する。

CREATE TABLE inventory_out (
    skuId      STRING,
    item       STRING,
    eventType  STRING, -- RESTOCK_REQUESTED_EVENT / RESTOCK_COMPLETED_EVENT
    event_time TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'inventory-out',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'component-stock-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE component_stock_events (
    eventId        STRING,
    skuId          STRING,
    item           STRING,
    eventType      STRING,
    eventTimestamp TIMESTAMP(3),
    sourceTopic    STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'component_stock_events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'key.format' = 'raw',
    'key.fields' = 'skuId',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'component-stock-events-value'
);

INSERT INTO component_stock_events
SELECT
    UUID()                                                       AS eventId,
    i.skuId                                                      AS skuId,
    i.item                                                       AS item,
    CASE
        WHEN i.eventType = 'RESTOCK_REQUESTED_EVENT' THEN 'RESTOCK_REQUESTED'
        WHEN i.eventType = 'RESTOCK_COMPLETED_EVENT' THEN 'RESTOCK_COMPLETED'
        ELSE i.eventType
    END                                                           AS eventType,
    i.event_time                                                 AS eventTimestamp,
    'inventory-out'                                              AS sourceTopic
FROM inventory_out AS i;

-- =========================================================
-- component_stock_current : skuId ごとの最新補充ステータス (upsert)
-- Iceberg 側は write.upsert.enabled=true, primary key = skuId
-- =========================================================

CREATE TABLE component_stock_current (
    skuId          STRING,
    item           STRING,
    latestStatus   STRING,
    lastEventAt    TIMESTAMP(3),
    PRIMARY KEY (skuId) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'component_stock_current',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'key.format' = 'raw',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'component-stock-current-value'
);

INSERT INTO component_stock_current
SELECT
    skuId,
    item,
    CASE
        WHEN eventType = 'RESTOCK_REQUESTED_EVENT' THEN 'RESTOCK_REQUESTED'
        WHEN eventType = 'RESTOCK_COMPLETED_EVENT' THEN 'RESTOCK_COMPLETED'
        ELSE eventType
    END AS latestStatus,
    event_time AS lastEventAt
FROM inventory_out;
