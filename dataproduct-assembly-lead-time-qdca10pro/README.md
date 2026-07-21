# Assembly Lead Time QDCA10pro

[OrderEvents](../dataproduct-order-events/README.md) の `LINE_ITEM_STATUS_CHANGED` イベント(`assemblyLine = QDCA10PRO`)から、明細(itemId)単位の `PLACED → IN_PROGRESS → FULFILLED` のリードタイムを算出するデータプロダクトである。[Assembly Lead Time QDCA10](../dataproduct-assembly-lead-time-qdca10/README.md) と同一スキーマ・同一パターンで、対象を QDCA10pro に絞ったもの。

## ソース

- `order_events`(OrderEvents)。QDCA10pro の組立キュー(`qdca10pro-in` → `orders-up`)に由来するイベントのみを対象とする。

## Flink 処理

`flink/qdca10pro-lead-time-job.sql` にて `MATCH_RECOGNIZE` を用い、`itemId` ごとに `PLACED` → `FULFILLED` の到達時刻差分を計算する。

## Iceberg テーブル

- `qdca10pro_lead_time` — itemId 単位のリードタイム記録(append)。

## Kafka 公開トピック

- `dataproduct-assembly-lead-time-qdca10pro` — Iceberg テーブルと同内容を Avro (subject `qdca10pro-lead-time-value`) で公開する (`dataproduct.*` 命名のため MirrorMaker2 でクロスサイト購読可能)。

## Trino 公開ビュー

- `dataproducts.assembly_lead_time.qdca10pro`
- 機種間比較用に `dataproducts.assembly_lead_time.qdca10` と `UNION ALL` したビューを別途用意することも可能。

## 依存関係

OrderEvents → Assembly Lead Time QDCA10pro。
