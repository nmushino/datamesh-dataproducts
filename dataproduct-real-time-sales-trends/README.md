# Real-time Sales Trends

[OrderEvents](../dataproduct-order-events/README.md) を土台に、商品(Item)別の売上をリアルタイムにウィンドウ集計するデータプロダクトである。

## ソース

- `dataproducts.order_events.order_events_history`(Trino経由で参照可能)の実体である `dataproduct-order-events` トピック(Kafka, Avro)を Flink から直接購読する(`ORDER_EVENTS_TOPIC` / `ORDER_EVENTS_REGISTRY_URL`。サイトによって asite 直参照 / MirrorMaker2 ミラー経由 `shop-asite.dataproduct-order-events` が切り替わる。詳細は order-events-job.sql と同じ規約)。
  - 集計対象は `eventType = 'LINE_ITEM_STATUS_CHANGED'` かつ `orderStatus = 'FULFILLED'`(確定売上。orders-up からの発行時点で明細の `lineItemStatus` にも同じ値が入る)。
  - ホームオフィスの `itemsales` / `productsales` / `storeserversales`(店舗・サーバ別内訳)は、ドメイン内部の集計ロジックの参考にしたが、Sales Trends はあくまで OrderEvents のみを正とする(homeoffice DB への直接依存は作らない)。

## Flink 処理

`flink/sales-trends-job.sql` にて、`dataproduct-order-events` を Item 別に Tumbling Window 集計し、Kafka(`dataproduct-sales-trends-5m` / `dataproduct-sales-trends-daily`)と Iceberg(Lakekeeper REST Catalog)の両方へ書き込む。Iceberg 側は別の consumer group (`sales-trends-flink-iceberg`) でソースを再購読し、Trino から参照可能な実体を直接生成する(inventory-analytics-job.sql と同じパターン)。

- 5分窓: `dataproduct-sales-trends-5m`(ほぼリアルタイムのダッシュボード用)
- 日次窓: `dataproduct-sales-trends-daily`(日次レポート用)

集計項目: `item`, `window_start`, `window_end`, `order_count`, `revenue`, `assembly_line`, `location`。

Kafka sink トピックは shop-cluster が `auto.create.topics.enable: false` のため、`openshift/dataproducts/dataproduct-sales-trends-topic.yaml` でジョブ投入前に明示的に作成する(`ocpdeploy.sh dataproducts deploy` が自動で適用)。

## Iceberg テーブル

- `sales_trends_5m` — 5分粒度の集計結果(append)。TTL運用は Iceberg のスナップショット retention で管理。
- `sales_trends_daily` — 日次集計(append)。BIレポート・OpenMetadataの品質スコア集計に利用。

## Trino 公開ビュー

`dataproducts.sales_trends.trends_5m` / `dataproducts.sales_trends.trends_daily` のみを公開する。

## 依存関係

OrderEvents → Real-time Sales Trends。OrderEvents のスキーマ(`order-event.avsc`)が変更される場合、本プロダクトの Flink ジョブも合わせて確認する。
