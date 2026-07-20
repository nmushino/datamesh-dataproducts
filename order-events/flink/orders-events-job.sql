-- OrderEvents Flink Job
-- ドメインの生イベント(orders-in / orders-up / eighty-six)を正規化し、
-- `dataproduct-order-events` トピック(Avro, Apicurio 登録)へ再公開する。
--
-- 前提: Apicurio Service Registry に schema/order-event.avsc を
--   group=dataproducts, artifactId=order-events-value として登録済みであること。
--
-- 配置先について: このジョブは asite/bsite/csite いずれのサイトにも投入できる
-- (`./script/ocpdeploy.sh dataproducts deploy --site <asite|bsite|csite> order-events`)。
-- orders-in は asite (counter) が、orders-up / eighty-six は bsite
-- (qdca10 / qdca10pro) がそれぞれの発行元であり、投入先サイトによって
-- 「自分自身が発行元のトピック (無 prefix)」と「MirrorMaker2 でミラーされた
-- トピック (shop-<site>. prefix)」の組み合わせが変わるため、実際のトピック名は
-- ORDERS_IN_TOPIC / ORDERS_UP_TOPIC / EIGHTY_SIX_TOPIC として
-- 投入時に site ごとの値を envsubst で埋め込む。

-- Checkpoint を有効化し、Kafka sink を exactly-once にすることで、
-- ジョブ再起動時に order_events_history へ重複行が積まれるのを防ぐ。
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- =========================================================
-- ソース: 各ドメインの生トピック
-- =========================================================

-- 実際の orders-in JSON (PlaceOrderCommand の @JsonProperty) に合わせる。
-- 例: {"id":"...","orderSource":"WEB","location":"TOKYO","loyaltyMemberId":"",
--      "qdca10LineItems":[{"itemId":"...","item":"QDC_A104_AT","price":305.75,"name":"..."}],
--      "qdca10proLineItems":[]}
CREATE TABLE orders_in (
    id                 STRING,
    orderSource        STRING,
    location           STRING,
    loyaltyMemberId    STRING,
    qdca10LineItems    ARRAY<ROW<itemId STRING, item STRING, name STRING, price DECIMAL(10,2)>>,
    qdca10proLineItems ARRAY<ROW<itemId STRING, item STRING, name STRING, price DECIMAL(10,2)>>,
    event_time         TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${ORDERS_IN_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'order-events-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- QDCA10/QDCA10pro (io.quarkusdroneshop.domain.valueobjects.OrderUp) が実際に
-- publish する JSON フィールドに合わせる。lineItemStatus / assemblyLine という
-- フィールドは存在せず、'orders-up' へのメッセージ到達自体が FULFILLED を意味する
-- (madeBy に qdca10/qdca10pro のホスト名が入るので、そこから assemblyLine を判定する)。
CREATE TABLE orders_up (
    orderId    STRING,
    lineItemId STRING,
    item       STRING,
    name       STRING,
    madeBy     STRING,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${ORDERS_UP_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'order-events-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE eighty_six (
    orderId    STRING,
    item       STRING,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${EIGHTY_SIX_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'order-events-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- =========================================================
-- シンク: order_events (Avro, Apicurio Service Registry)
-- =========================================================

-- Flink Kafka Sink の transactional-id はデフォルトで
-- (transactional-id-prefix + subtaskIndex) から決まり、オペレータ UID は
-- 考慮されない。3つの INSERT を STATEMENT SET で 1 ジョブにまとめても、
-- 各 INSERT の sink は同じテーブル定義 (= 同じ prefix) を共有し、かつ
-- いずれも並列度1 (subtask 0) のため、同じ transactional id を奪い合って
-- 互いの initTransactions() をフェンシングし合い、永遠に INITIALIZING の
-- まま進まなくなる。そのため sink テーブルを INSERT ごとに分け、
-- transactional-id-prefix をそれぞれ変えることで衝突を避ける
-- (トピック / スキーマは全て同じ dataproduct-order-events を指す)。
CREATE TABLE order_events_from_orders_in_qdca10 (
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
        assemblyLine STRING,
        madeBy STRING
    >,
    sourceDomain    STRING,
    sourceTopic     STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'dataproduct-order-events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'key.format' = 'raw',
    'key.fields' = 'orderId',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'order-events-value',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = 'order-events-sink-orders-in-qdca10',
    -- Flink Kafka connector のデフォルト transaction.timeout.ms (1時間) は
    -- Strimzi Kafka broker の transaction.max.timeout.ms (デフォルト15分) を
    -- 超えており InitProducerIdResponse が失敗する。broker の上限内に収める。
    'properties.transaction.timeout.ms' = '60000'
);

-- QDCA10Items と QDCA10ProItems を同じ sink テーブルに書くと、STATEMENT SET 内で
-- 両方が subtask 0 の同一 transactional-id を奪い合ってフェンシングし合う
-- (このファイル冒頭のコメント参照)。そのため明細の発行元 (QDCA10/QDCA10PRO)
-- ごとに sink テーブルを分ける。
CREATE TABLE order_events_from_orders_in_qdca10pro (
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
        assemblyLine STRING,
        madeBy STRING
    >,
    sourceDomain    STRING,
    sourceTopic     STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'dataproduct-order-events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'key.format' = 'raw',
    'key.fields' = 'orderId',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'order-events-value',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = 'order-events-sink-orders-in-qdca10pro',
    'properties.transaction.timeout.ms' = '60000'
);

CREATE TABLE order_events_from_orders_up (
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
        assemblyLine STRING,
        madeBy STRING
    >,
    sourceDomain    STRING,
    sourceTopic     STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'dataproduct-order-events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'key.format' = 'raw',
    'key.fields' = 'orderId',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'order-events-value',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = 'order-events-sink-orders-up',
    'properties.transaction.timeout.ms' = '60000'
);

CREATE TABLE order_events_from_eighty_six (
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
        assemblyLine STRING,
        madeBy STRING
    >,
    sourceDomain    STRING,
    sourceTopic     STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'dataproduct-order-events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'key.format' = 'raw',
    'key.fields' = 'orderId',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${APICURIO_REGISTRY_URL}/apis/ccompat/v6',
    'value.avro-confluent.subject' = 'order-events-value',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = 'order-events-sink-eighty-six',
    'properties.transaction.timeout.ms' = '60000'
);

-- eventId は UUID() (非決定的) ではなく、ソースイベントの内容から決定論的に
-- 生成する。ジョブ再起動でチェックポイントより前から再処理された場合でも
-- 同一の eventId が得られるため、order_events_history 側で重複排除しやすい。
BEGIN STATEMENT SET;

-- =========================================================
-- 1. ORDER_PLACED (明細単位) : orders-in から
-- =========================================================
-- QDCA10/QDCA10pro が dataproduct-order-events だけを見て「自分宛ての注文か」を
-- 判定できるように、注文ヘッダー1件ではなく明細 (QDCA10Items/QDCA10ProItems) を
-- UNNEST して1明細=1イベントとして発行する。assemblyLine とステータス(PLACED)を
-- 明細側に持たせることで、下流は lineItem.assemblyLine / orderStatus='PLACED' で
-- フィルタするだけで自分宛ての作業を拾える。
INSERT INTO order_events_from_orders_in_qdca10
SELECT
    MD5(CONCAT(o.id, '|', t.itemId, '|ORDER_PLACED|', CAST(o.event_time AS STRING))) AS eventId,
    o.id                                                AS orderId,
    'ORDER_PLACED'                                      AS eventType,
    o.event_time                                        AS eventTimestamp,
    o.orderSource                                       AS orderSource,
    o.location                                          AS location,
    o.loyaltyMemberId                                   AS loyaltyMemberId,
    'PLACED'                                             AS orderStatus,
    ROW(t.itemId, t.item, t.name, t.price, 'PLACED', 'QDCA10', CAST(NULL AS STRING)) AS lineItem,
    'counter'                                           AS sourceDomain,
    'orders-in'                                         AS sourceTopic
FROM orders_in AS o
CROSS JOIN UNNEST(o.qdca10LineItems) AS t(itemId, item, name, price);

INSERT INTO order_events_from_orders_in_qdca10pro
SELECT
    MD5(CONCAT(o.id, '|', t.itemId, '|ORDER_PLACED|', CAST(o.event_time AS STRING))) AS eventId,
    o.id                                                AS orderId,
    'ORDER_PLACED'                                      AS eventType,
    o.event_time                                        AS eventTimestamp,
    o.orderSource                                       AS orderSource,
    o.location                                          AS location,
    o.loyaltyMemberId                                   AS loyaltyMemberId,
    'PLACED'                                             AS orderStatus,
    ROW(t.itemId, t.item, t.name, t.price, 'PLACED', 'QDCA10PRO', CAST(NULL AS STRING)) AS lineItem,
    'counter'                                           AS sourceDomain,
    'orders-in'                                         AS sourceTopic
FROM orders_in AS o
CROSS JOIN UNNEST(o.qdca10proLineItems) AS t(itemId, item, name, price);

-- =========================================================
-- 2. LINE_ITEM_STATUS_CHANGED (明細) : orders-up から
-- =========================================================
-- orders-up への到達自体が「完了」を意味し、明示的なステータスフィールドは
-- 存在しない (OrderUp.java 参照)。assemblyLine は madeBy (ホスト名prefix) から判定する。
INSERT INTO order_events_from_orders_up
SELECT
    MD5(CONCAT(u.orderId, '|', u.lineItemId, '|FULFILLED|', CAST(u.event_time AS STRING))) AS eventId,
    u.orderId                                           AS orderId,
    'LINE_ITEM_STATUS_CHANGED'                          AS eventType,
    u.event_time                                        AS eventTimestamp,
    CAST(NULL AS STRING)                                AS orderSource,
    CAST(NULL AS STRING)                                AS location,
    CAST(NULL AS STRING)                                AS loyaltyMemberId,
    'FULFILLED'                                         AS orderStatus,
    ROW(
        u.lineItemId,
        u.item,
        u.name,
        CAST(NULL AS DECIMAL(10,2)),
        'FULFILLED',
        CASE
            WHEN u.madeBy LIKE 'qdca10pro%' THEN 'QDCA10PRO'
            WHEN u.madeBy LIKE 'qdca10%' THEN 'QDCA10'
            ELSE CAST(NULL AS STRING)
        END,
        u.madeBy
    ) AS lineItem,
    CASE
        WHEN u.madeBy LIKE 'qdca10pro%' THEN 'qdca10pro'
        WHEN u.madeBy LIKE 'qdca10%' THEN 'qdca10'
        ELSE CAST(NULL AS STRING)
    END                                                  AS sourceDomain,
    'orders-up'                                         AS sourceTopic
FROM orders_up AS u;

-- =========================================================
-- 3. ORDER_CANCELLED (欠品) : eighty-six から
-- =========================================================
INSERT INTO order_events_from_eighty_six
SELECT
    MD5(CONCAT(e.orderId, '|', e.item, '|ORDER_CANCELLED|', CAST(e.event_time AS STRING))) AS eventId,
    e.orderId                                           AS orderId,
    'ORDER_CANCELLED'                                   AS eventType,
    e.event_time                                        AS eventTimestamp,
    CAST(NULL AS STRING)                                AS orderSource,
    CAST(NULL AS STRING)                                AS location,
    CAST(NULL AS STRING)                                AS loyaltyMemberId,
    'CANCELLED'                                          AS orderStatus,
    ROW(CAST(NULL AS STRING), e.item, CAST(NULL AS STRING), CAST(NULL AS DECIMAL(10,2)), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING)) AS lineItem,
    'qdca10'                                            AS sourceDomain,
    'eighty-six'                                        AS sourceTopic
FROM eighty_six AS e;

END;
