# Dataproduct Inventory Event

(旧名: Drone Component Stock)

quarkusdroneshop-inventory の在庫増減イベント(`inventory-in` / `inventory-out`)から、部品(SKU)別の現在庫・在庫履歴を再構成するデータプロダクトである。

## ソース

| トピック | 内容 |
|---|---|
| `inventory-in` | 補充要求(`RestockItemCommand`) |
| `inventory-out` | 在庫確定イベント(`RestockRequestedEvent` / `RestockCompletedEvent`。Debezium Outbox 経由) |
| `inventory-live` | 現在庫のライブビュー(内部用) |

Inventory ドメイン内部の Postgres(`ProductMaster` / `Inventory` テーブル)には他ドメインから直接アクセスしない。本データプロダクトがこれらのイベントのみから在庫状態を再構成する。

## 前提インフラ: inventory の Debezium Outbox キャプチャ

`quarkusdroneshop-inventory` は `RestockRequestedEvent`/`RestockCompletedEvent` を [Debezium Outbox パターン](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html) (`droneshop.outboxevent` テーブルへの書き込み → Debezium が CDC で捕捉 → `inventory-out` へルーティング) で発行している。これを実際に Kafka へ届けるための Kafka Connect 基盤が [`inventory-outbox-connect.yaml`](../../quarkusdroneshop-ansible/openshift/dataproducts/inventory-outbox-connect.yaml) (`KafkaConnect` + `KafkaConnector`) で、コネクタイメージは [`kafka-connect/Dockerfile`](../kafka-connect/Dockerfile) (Strimzi/AMQ Streams の `kafka-41-rhel9` ベース + Debezium PostgreSQL コネクタプラグイン) でビルドする。ビルド・登録は `./script/ocpdeploy.sh dataproducts debezium` に統合済み。詳細は [kafka-connect/README.md](../kafka-connect/README.md) を参照。

新規サイトに inventory をデプロイする際は、以下の順序で実行すること (デプロイ順序を誤ると `droneshop.outboxevent` が存在しない状態でコネクタが起動しようとして失敗する)。

1. `./script/ocpdeploy.sh dataproducts setup`
2. inventory サービスをデプロイ (`droneshop.outboxevent` テーブルが作成される)
3. `./script/ocpdeploy.sh dataproducts debezium`
4. `./script/ocpdeploy.sh dataproducts deploy dataproduct-inventory-event`

ハマりどころ (2026-07-21 対応):
- `InventoryService` が以前 `inventory-out` に `RestockInventoryCommand.toString()` (非JSON) を直接書き込んでいたため、JSON コンシューマ (このジョブ) がクラッシュしていた。該当コードは削除済み。
- Debezium の `EventRouter` はデフォルトで `timestamp` フィールドを INT64 (epoch) として読もうとするが、`debezium-quarkus-outbox` の `timestamp` 列は `TIMESTAMP WITH TIME ZONE` (ZonedTimestamp = 文字列) のため型不一致で失敗する。`ReplaceField` SMT で `EventRouter` に渡す前にこのフィールドを除去している。
- `payload` 列は既に JSON 文字列なので、Connect の `value.converter` に `JsonConverter` を使うと二重エンコードされる。`StringConverter` を使うこと。
- `eventType` は `outboxevent.type` 列にのみ存在し、Debezium Outbox の標準構成では `payload` 内に自動転記されない。本データプロダクトの Flink ジョブが `eventType` をフラットなトップレベルフィールドとして必要とするため、`RestockRequestedEvent`/`RestockCompletedEvent` のペイロード生成時に `eventType` を明示的に含めている。
- `quarkusdroneshop-inventory` は `quarkus.hibernate-orm.database.generation=drop-and-create` のため、再デプロイのたびに `droneshop.outboxevent` の OID が変わる。`publication.autocreate.mode=filtered` (OID 固定) だと再作成後の変更を拾えなくなるため `all_tables` を使うこと。

## Flink 処理

`flink/component-stock-job.sql` にて:

1. `inventory-out` の `RESTOCK_REQUESTED_EVENT` / `RESTOCK_COMPLETED_EVENT` を `skuId` で正規化し、`dataproduct-inventory-events`(history、旧名 `component_stock_events`)へ append する。
2. `skuId` ごとに最新イベントを `component_stock_current`(upsert)として保持する(`REQUESTED` → `COMPLETED` の状態遷移を追跡)。
3. `item`(ドローン品目コード)ごとの実在庫数を `component_stock_quantity`(upsert)として `dataproduct-component-stock-quantity` トピックへ公開する。`RESTOCK_COMPLETED_EVENT`(inventory ドメイン側で実際に `inStockQuantity` が更新されたタイミング)のみを反映する。

在庫の実数(in-stock quantity)は inventory ドメイン側で `RestockCompletedEvent`/`RestockRequestedEvent` に `quantity` フィールドとして含まれるようになっており(補充完了時点の実数)、`component_stock_quantity` として `item` 単位に公開される。ただし発注(消費側)による減算は Inventory ドメインには存在しない(補充のみを扱うドメイン)。消費側の在庫判定は QDCA10/QDCA10pro が本トピックをローカルキャッシュのシード/補充シグナルとして購読し、注文消費分はそれぞれのサービス内でローカルに追跡する設計とする(同期的な Trino クエリを都度発行しない)。

## Iceberg テーブル

- `component_stock_events` — 補充イベント履歴(append)。
- `component_stock_current` — SKU ごとの最新補充ステータス(upsert)。
- `component_stock_quantity` — item ごとの実在庫数(upsert)。

## Kafka 公開トピック

- **`dataproduct-inventory-events`**(旧名 `component_stock_events`) — 補充イベント履歴 (Avro, subject `component-stock-events-value`)。`dataproduct.*` 命名のため MirrorMaker2 でクロスサイト購読可能。
- `component_stock_current` — SKU ごとの最新補充ステータス (upsert-kafka, subject `component-stock-current-value`)。
- **`dataproduct-component-stock-quantity`** — item ごとの実在庫数 (upsert-kafka, subject `component-stock-quantity-value`)。QDCA10/QDCA10pro など下流サービスがリアルタイムにローカルキャッシュを構築するための購読用トピック(`dataproduct.*` 命名のため MirrorMaker2 でクロスサイト購読可能)。

## Trino 公開ビュー

- `dataproducts.component_stock.stock_current`
- `dataproducts.component_stock.stock_events`

## 後続プロダクトからの利用

- **Inventory Analytics**: `dataproduct-inventory-events` と OrderEvents を突き合わせ、消費速度・欠品リスクを算出する。
- **QDCA10 / QDCA10pro**: `dataproduct-component-stock-quantity` を購読し、在庫判定(eighty-six ロジック)のローカルキャッシュを実データで初期化・補充する。
