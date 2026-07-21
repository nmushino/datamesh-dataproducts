# Customer 360

Aサイト(Web/Counter での発注)・Bサイト(QDCA10/QDCA10pro での組立完了)双方で発生するイベントから、顧客プロファイルをリアルタイムに作成するデータプロダクトである。個人情報を含むため、Trino 側でのアクセス制御・マスキングを必須とする。

## 【2026-07-21 再設計】経緯

旧版は `postgresql-prod.dronedb.public.customers`(Debezium CDC)/ `rewards` トピックを前提としていたが、いずれもこのリポジトリには実装されていない架空のソース(プロデューサが存在しない)であり、**このデータプロダクトは一度も正常稼働していなかった**。実際に稼働している [OrderEvents](../dataproduct-order-events/README.md) (`dataproduct-order-events`) のみをソースとして再設計した。

OrderEvents は Web/Counter (asite) の発注イベントと QDCA10/QDCA10pro (bsite) の組立完了イベントを order-events Flink ジョブが既に統合したハブであるため、これ自体が「Aサイト・Bサイトで発生するイベントから顧客情報を作る」という要件を満たす唯一の実在ソースである。

## ソース

- `dataproduct-order-events`(OrderEvents)の `ORDER_PLACED` イベントのみを使用する。`loyaltyMemberId` は未入力 (Web の rewardsId が Optional) のことが多いため、`lineItem.name`(注文者名)を実質的な統合キーとする。

## Flink 処理

`flink/customer-360-job.sql` は常時稼働のストリーミングジョブで、`dataproduct-order-events` への `ORDER_PLACED` イベント到着がトリガーとなる (定期実行やバッチではない)。

1. **`dataproduct-customer-360`(Kafka, upsert-kafka)**: `customerName` ごとに無限 GROUP BY で集計し、直近の注文情報 (最終注文ID・最終注文時刻・累計注文数・直近の店舗ロケーション・ロイヤリティ会員ID) を upsert する。注文イベントが来るたびに即座に更新される。
2. **`customer_events_history`(Iceberg)**: upsert-kafka の更新/削除ストリームは追記専用の Iceberg シンクへ書き込めない (dataproduct-inventory-analytics で判明した制約と同じ) ため、こちらは集計をせず `ORDER_PLACED` イベントをそのまま append する履歴テーブルとする。「現在プロファイル」は Trino ビュー側で window 関数により履歴から都度算出する。

## Kafka 公開トピック

- **`dataproduct-customer-360`** — customerName ごとの現在プロファイル (upsert-kafka, subject `customer-360-value`)。`dataproduct.*` 命名のため MirrorMaker2 でクロスサイト購読可能。

## Iceberg テーブル

- `customer_events_history` — `ORDER_PLACED` イベントの履歴(append)。

## Trino 公開ビュー・アクセス制御

- `dataproducts.customer_360.profile_full` — 履歴から `ROW_NUMBER()` で直近状態を算出した PII 込みのフル版。顧客対応・CRM 用途に限定し、行/カラムレベルのアクセス制御で許可されたロールのみに公開する。
- `dataproducts.customer_360.profile_masked` — 一般ドメイン向け。氏名・ロイヤリティ会員IDをマスキング。

## 依存関係

OrderEvents(`dataproduct-order-events`) → Customer 360。他ドメインが顧客の購買状況を必要とする場合は、`dataproduct-customer-360` トピック、または Trino の公開ビュー経由でのみ取得する。
