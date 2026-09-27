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
    -- orders_up との INTERVAL JOIN (location 補完用) は両テーブルの watermark に
    -- 依存する。データが疎らなパーティションが1つでもあると watermark 全体が
    -- そこでブロックされ join が永久に emit されなくなる (sales-trends-job.sql と
    -- 同じ既知の問題)。一定時間データが来ないパーティションは除外する。
    'scan.watermark.idle-timeout' = '30s',
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
    'scan.watermark.idle-timeout' = '30s',
    'format' = 'json'
);

-- 【経緯】当初 QDCA10/QDCA10pro は eighty-six へ item 名の素の文字列
-- (例: "QDC_A104_AC") のみを送っており、orderId を含まなかったため、
-- 一時的に 'format'='raw' で読み、下流の ORDER_CANCELLED にはプレースホルダの
-- orderId を補っていた (欠品がどの注文・明細のものか特定できなかった)。
-- 【2026-07-21 修正】qdca10/qdca10pro に EightySixMessage を導入し、
-- orderId/lineItemId/item を含む JSON を送るように変更したため、
-- ここも 'format'='json' で読み、実在の orderId/lineItemId を
-- ORDER_CANCELLED イベントに使えるようにする。
CREATE TABLE eighty_six (
    orderId    STRING,
    lineItemId STRING,
    item       STRING,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${EIGHTY_SIX_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_URLS}',
    'properties.group.id' = 'order-events-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    -- 【2026-07-21 修正】eighty-six を JSON 化する以前 (品目名の素の文字列のみ送信
    -- していた頃) に publish された旧形式メッセージがトピックに残っており、
    -- 'earliest-offset' で読み直すたびに JSON パースに失敗してタスクが FAILED になり、
    -- ジョブ全体が無限リスタートループに陥っていた (チェックポイントも一度も成功しない
    -- ため、orders_in/orders_up 側の処理も不安定になっていた)。パース失敗レコードは
    -- スキップして処理を継続する。
    'json.ignore-parse-errors' = 'true'
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
    'sink.transactional-id-prefix' = 'order-events-sink-orders-in-qdca10-v3',
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
    'sink.transactional-id-prefix' = 'order-events-sink-orders-in-qdca10pro-v3',
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
    'sink.transactional-id-prefix' = 'order-events-sink-orders-up-v3',
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
    -- 2026-07-21: 過去の度重なる再デプロイで蓄積した Kafka 側の transactional
    -- state が原因と見られる InitProducerId の無限ループ (INITIALIZING のまま
    -- 進まずチェックポイントが永久に失敗する) が発生したため、prefix を
    -- 変更してクリーンな transactional-id を強制する。
    'sink.transactional-id-prefix' = 'order-events-sink-eighty-six-v3',
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
-- orders-up (OrderUp.java: {orderId, lineItemId, item, name, timestamp, madeBy}) には
-- location/orderSource/loyaltyMemberId が含まれない。以前はここを一律 NULL にしていたが、
-- そのせいで sales-trends-job.sql の日次集計から生成される SalesTrend には location が
-- 一切残らず、下流の getStoreServerSalesByDate (Store Sales ダッシュボード) が
-- 「location が NULL の行は除外する」フィルタにより常に 0 件を返す構造的なバグに
-- なっていた。同一 orderId の ORDER_PLACED (orders_in) と JOIN してヘッダー情報を
-- 補完することで、集計まで正しく location/orderSource/loyaltyMemberId を伝播させる。
INSERT INTO order_events_from_orders_up
SELECT
    MD5(CONCAT(u.orderId, '|', u.lineItemId, '|FULFILLED|', CAST(u.event_time AS STRING))) AS eventId,
    u.orderId                                           AS orderId,
    'LINE_ITEM_STATUS_CHANGED'                          AS eventType,
    u.event_time                                        AS eventTimestamp,
    oi.orderSource                                      AS orderSource,
    oi.location                                         AS location,
    oi.loyaltyMemberId                                  AS loyaltyMemberId,
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
FROM orders_up AS u
-- 通常の JOIN は Kafka sink (append-only) が非対応の update/delete 変更を生成する
-- (Table sink '...' doesn't support consuming update and delete changes) ため、
-- append-only な結果になる INTERVAL JOIN を使う。orders-in → orders-up は通常
-- 数分〜数日で到達するため、7日を安全側の上限とする (これを超えて到達した
-- orders-up はマッチせず除外される。ロス上等の実害の少ないダッシュボード用途)。
JOIN orders_in AS oi
    ON u.orderId = oi.id
    AND u.event_time BETWEEN oi.event_time AND oi.event_time + INTERVAL '7' DAY
-- orders-up には疎通確認用の ping メッセージ ({"test":"ping-..."}) が
-- 混じることがある。必須フィールドが揃わない JSON は Flink の json フォーマットで
-- 全カラム NULL の行に変換されてしまい、下流の sales-trends 集計へ item=null の
-- ゴミレコードとして流れ込み、homeoffice-backend の変換で NPE を起こしていた。
-- 実注文由来のメッセージだけを通す。
WHERE u.orderId IS NOT NULL AND u.item IS NOT NULL;

-- =========================================================
-- 3. ORDER_CANCELLED (欠品) : eighty-six から
-- =========================================================
INSERT INTO order_events_from_eighty_six
SELECT
    MD5(CONCAT(e.orderId, '|', e.lineItemId, '|ORDER_CANCELLED|', CAST(e.event_time AS STRING))) AS eventId,
    e.orderId                                           AS orderId,
    'ORDER_CANCELLED'                                   AS eventType,
    e.event_time                                        AS eventTimestamp,
    CAST(NULL AS STRING)                                AS orderSource,
    CAST(NULL AS STRING)                                AS location,
    CAST(NULL AS STRING)                                AS loyaltyMemberId,
    'CANCELLED'                                          AS orderStatus,
    ROW(e.lineItemId, e.item, CAST(NULL AS STRING), CAST(NULL AS DECIMAL(10,2)), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING)) AS lineItem,
    'qdca10'                                            AS sourceDomain,
    'eighty-six'                                        AS sourceTopic
FROM eighty_six AS e
-- orders-up と同様、疎通確認用の ping メッセージを除外する。
WHERE e.orderId IS NOT NULL AND e.item IS NOT NULL;

END;
