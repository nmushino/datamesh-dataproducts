# Drone Component Stock

quarkusdroneshop-inventory の在庫増減イベント(`inventory-in` / `inventory-out`)から、部品(SKU)別の現在庫・在庫履歴を再構成するデータプロダクトである。

## ソース

| トピック | 内容 |
|---|---|
| `inventory-in` | 補充要求(`RestockItemCommand`) |
| `inventory-out` | 在庫確定イベント(`RestockRequestedEvent` / `RestockCompletedEvent`。Debezium Outbox 経由) |
| `inventory-live` | 現在庫のライブビュー(内部用) |

Inventory ドメイン内部の Postgres(`ProductMaster` / `Inventory` テーブル)には他ドメインから直接アクセスしない。Drone Component Stock がこれらのイベントのみから在庫状態を再構成する。

## Flink 処理

`flink/component-stock-job.sql` にて:

1. `inventory-out` の `RESTOCK_REQUESTED_EVENT` / `RESTOCK_COMPLETED_EVENT` を `skuId` で正規化し、`component_stock_events`(history)へ append する。
2. `skuId` ごとに最新イベントを `component_stock_current`(upsert)として保持する(`REQUESTED` → `COMPLETED` の状態遷移を追跡)。

在庫の実数(in-stock quantity)そのものは Inventory ドメインの内部実装(Postgres)に閉じているため、Drone Component Stock では **補充イベントの状態(要求中/完了)** をドメイン外に公開する形とする。実数量の可視化が必要になった場合は、Inventory ドメイン側で `inventory-out` に数量を含めるようスキーマ拡張を行い、本データプロダクトを追随させる。

## Iceberg テーブル

- `component_stock_events` — 補充イベント履歴(append)。
- `component_stock_current` — SKU ごとの最新補充ステータス(upsert)。

## Trino 公開ビュー

- `dataproducts.component_stock.stock_current`
- `dataproducts.component_stock.stock_events`

## 後続プロダクトからの利用

- **Inventory Analytics**: `component_stock_events` と OrderEvents を突き合わせ、消費速度・欠品リスクを算出する。
