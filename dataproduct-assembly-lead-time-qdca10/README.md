# Assembly Lead Time QDCA10

[OrderEvents](../dataproduct-order-events/README.md) の `LINE_ITEM_STATUS_CHANGED` イベント(`assemblyLine = QDCA10`)から、明細(itemId)単位の `PLACED → IN_PROGRESS → FULFILLED` のリードタイムを算出するデータプロダクトである。

## ソース

- `order_events`(OrderEvents)。QDCA10 の組立キュー(`qdca10-in` → `orders-up`)に由来するイベントのみを対象とする。QDCA10 ドメインサービスの内部実装には直接アクセスしない。

## Flink 処理

`flink/qdca10-lead-time-job.sql` にて `MATCH_RECOGNIZE` を用い、`itemId` ごとに `PLACED` → `FULFILLED` の到達時刻差分を計算する。`IN_PROGRESS` を経由しない(直接 `FULFILLED` になる)パターンも許容する。

## Iceberg テーブル

- `qdca10_lead_time` — itemId 単位のリードタイム記録(append)。`placed_at`, `fulfilled_at`, `lead_time_seconds` を保持。

## Kafka 公開トピック

- `dataproduct-assembly-lead-time-qdca10` — Iceberg テーブルと同内容を Avro (subject `qdca10-lead-time-value`) で公開する (`dataproduct.*` 命名のため MirrorMaker2 でクロスサイト購読可能)。

## Trino 公開ビュー

- `dataproducts.assembly_lead_time.qdca10`

## 比較用途

[Assembly Lead Time QDCA10pro](../dataproduct-assembly-lead-time-qdca10pro/README.md) と同一スキーマで出力しており、Trino 側で `UNION ALL` すれば機種間比較が可能。

## 依存関係

OrderEvents → Assembly Lead Time QDCA10。
