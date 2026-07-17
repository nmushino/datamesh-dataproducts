# Inventory Analytics

[Drone Component Stock](../drone-component-stock/README.md) と [OrderEvents](../order-events/README.md) を突き合わせ、部品の消費速度・欠品リスクを分析するデータプロダクトである。ドメイン内部の Postgres には一切アクセスせず、他の 2 データプロダクトの公開データのみを入力とする。

## ソース

- `component_stock_events`(Drone Component Stock) — 補充イベント。
- `order_events`(OrderEvents) — `ORDER_CANCELLED`(`eighty-six` に由来する欠品キャンセル)を主に利用する。

## Flink 処理

`flink/inventory-analytics-job.sql` にて:

1. 1時間 Tumbling Window で `item` ごとの `RESTOCK_REQUESTED` 件数と `ORDER_CANCELLED`(欠品起因)件数を集計し `inventory_turnover` を生成する。
2. 直近ウィンドウで欠品キャンセルが 1 件でも発生した item を `stockout_risk`(現在の欠品リスクフラグ、upsert)としてマークする。

## Iceberg テーブル

- `inventory_turnover` — item 別・時間窓別の補充要求数/欠品キャンセル数(append)。
- `stockout_risk` — item ごとの現在の欠品リスク状態(upsert)。

## Trino 公開ビュー

- `dataproducts.inventory_analytics.turnover`
- `dataproducts.inventory_analytics.stockout_risk`

## 依存関係

Drone Component Stock, OrderEvents → Inventory Analytics。
