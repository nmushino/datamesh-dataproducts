# Real-time Sales Trends

[OrderEvents](../order-events/README.md) を土台に、商品(Item)別の売上をリアルタイムにウィンドウ集計するデータプロダクトである。

## ソース

- `dataproducts.order_events.order_events_history`(Trino経由)/ Flink からは `order_events` トピック(Kafka, Avro)を直接購読する。
  - 集計対象は `eventType = 'LINE_ITEM_STATUS_CHANGED'` かつ `lineItemStatus = 'FULFILLED'`(確定売上)。
  - ホームオフィスの `itemsales` / `productsales` / `storeserversales`(店舗・サーバ別内訳)は、ドメイン内部の集計ロジックの参考にしたが、Sales Trends はあくまで OrderEvents のみを正とする(homeoffice DB への直接依存は作らない)。

## Flink 処理

`flink/sales-trends-job.sql` にて、`order_events` を Item 別に Tumbling Window 集計する。

- 5分窓: `sales_trends_5m`(ほぼリアルタイムのダッシュボード用)
- 日次窓: `sales_trends_daily`(日次レポート用)

集計項目: `item`, `window_start`, `window_end`, `order_count`, `revenue`, `assembly_line`, `location`。

## Iceberg テーブル

- `sales_trends_5m` — 5分粒度の集計結果(append)。TTL運用は Iceberg のスナップショット retention で管理。
- `sales_trends_daily` — 日次集計(append)。BIレポート・OpenMetadataの品質スコア集計に利用。

## Trino 公開ビュー

`dataproducts.sales_trends.trends_5m` / `dataproducts.sales_trends.trends_daily` のみを公開する。

## 依存関係

OrderEvents → Real-time Sales Trends。OrderEvents のスキーマ(`order-event.avsc`)が変更される場合、本プロダクトの Flink ジョブも合わせて確認する。
