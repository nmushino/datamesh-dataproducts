-- Drone Component Stock Flink Job
-- inventory-out (RestockRequestedEvent / RestockCompletedEvent) を skuId 単位に正規化し、
-- dataproduct-inventory-events トピックへ再公開する。

CREATE TABLE inventory_out (
    skuId      STRING,
    item       STRING,
    eventType  STRING, -- RESTOCK_REQUESTED_EVENT / RESTOCK_COMPLETED_EVENT
    quantity   BIGINT, -- RestockCompletedEvent 発行時点の実在庫数 (inventory ドメイン側で更新)
    event_time TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'inventory-out',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'component-stock-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    -- inventory-out には過去、InventoryService の不具合により command.toString() (非JSON) が
    -- 混入していた (2026-07-21 修正済み)。過去分の不正レコードでジョブ全体がクラッシュし
    -- 続けないよう、パース失敗レコードは読み飛ばす。
    'json.ignore-parse-errors' = 'true'
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
    -- dataproduct.* 命名 (MirrorMaker2 のクロスサイト購読対象パターンに合致させるため)。
    -- 旧名は component_stock_events。
    'topic' = 'dataproduct-inventory-events',
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

-- =========================================================
-- component_stock_quantity : item (ドローン品目) ごとの現在庫数 (upsert)
-- QDCA10/QDCA10pro など、同期的な Trino クエリではなくローカルキャッシュを
-- Kafka 経由で維持したい下流サービス向けの公開トピック。
-- skuId(ProductMaster の内部UUID)ではなく item をキーにしているのは、
-- QDCA10/QDCA10pro が skuId を一切知らず item コードだけで在庫を判断するため。
-- RESTOCK_COMPLETED_EVENT (実際に inStockQuantity が更新されたタイミング) のみを
-- 反映する (REQUESTED は数量が未確定のため対象外)。
-- =========================================================

CREATE TABLE component_stock_quantity (
    item              STRING,
    quantity          BIGINT,
    lastRestockedAt   TIMESTAMP(3),
    PRIMARY KEY (item) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'dataproduct-component-stock-quantity',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'key.format' = 'raw',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'component-stock-quantity-value'
);

INSERT INTO component_stock_quantity
SELECT
    item,
    quantity,
    event_time AS lastRestockedAt
FROM inventory_out
WHERE eventType = 'RESTOCK_COMPLETED_EVENT';
