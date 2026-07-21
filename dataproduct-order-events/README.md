# OrderEvents

全ドメイン(Counter / QDCA10 / QDCA10pro / Homeoffice / Web)の注文関連イベントを正規化・統合した、データプロダクト基盤のハブとなるデータプロダクトである。

## ソース(既存ドメイントピック)

| トピック | 発行元 | 内容 |
|---|---|---|
| `orders-in` | quarkusdroneshop-web → quarkusdroneshop-counter | 注文受付(`PlaceOrderCommand`) |
| `qdca10-in` / `qdca10pro-in` | quarkusdroneshop-counter → qdca10 / qdca10pro | 機種別の組立キュー投入(`OrderIn`) |
| `orders-up` | qdca10 / qdca10pro → counter / web | 明細ステータス更新(`TicketUp`) |
| `eighty-six` | qdca10 / qdca10pro → web | 欠品による注文キャンセル(`EightySixEvent`) |
| `shop-asite.orders-in`, `shop-bsite.orders-up` | counter → homeoffice | 店舗跨ぎの注文集約用ミラー |

これらはドメインサービス間の直接連携であり、ドメイン外(Inventory Analytics, Customer 360 等)から直接購読しない。OrderEvents がこれらを正規化して再公開する。

## Flink 処理

`flink/order-events-job.sql` で以下を行う。

1. `orders-in` (`PlaceOrderCommand`) を受け、注文単位のヘッダーイベント `ORDER_PLACED` を生成する。
2. `orders-up` (`TicketUp`) を受け、明細単位のステータス変更イベント `LINE_ITEM_STATUS_CHANGED` を生成する。
3. `eighty-six` (`EightySixEvent`) を受け、`ORDER_CANCELLED` イベントを生成する。
4. 3 系統を `order_events` トピック(Avro, Apicurio 登録)に UNION ALL し、`order_id` をキーとする。
5. 同時に `order_events_current`(注文の最新ステータスを orderId で upsert)を計算する。

## Iceberg テーブル

- `order_events_history` — 上記イベントすべてを append-only で保持(監査・Time travel用)。
- `order_events_current` — `order_id` ごとの最新状態(upsert)。Real-time Sales Trends / Inventory Analytics / Customer 360 / Assembly Lead Time 系は基本的にこちらを参照する。

## Trino 公開ビュー

`trino/order_events_views.sql` にて `dataproducts.order_events` スキーマ配下に以下を公開する。他ドメインからのアクセスはこのビューのみを経由する(Iceberg テーブルへの直接アクセスは不可)。

- `dataproducts.order_events.orders_current`
- `dataproducts.order_events.order_events_history`

## スキーマ管理

- `schema/order-event.avsc` を Apicurio Service Registry にグループ `dataproducts`, アーティファクト ID `order-events-value` として登録する。
- 互換性モードは `BACKWARD_TRANSITIVE` とする。フィールド追加は default 値必須、削除・型変更は新規メジャーバージョンとして扱う。

## 今後のプロダクトからの利用

- **Real-time Sales Trends**: `order_events_history` の `ORDER_PLACED` を集計。
- **Inventory Analytics**: `order_events_history` の明細(item, quantity)と Drone Component Stock を突き合わせ。
- **Assembly Lead Time QDCA10 / QDCA10pro**: `LINE_ITEM_STATUS_CHANGED` の `PLACED → IN_PROGRESS → FULFILLED` 間の時間差を機種別に算出。
- **Customer 360**: `loyaltyMemberId` をキーに顧客プロファイルと join。
