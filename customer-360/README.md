# Customer 360

顧客に関する複数ドメインのイベント・CDC を `loyaltyMemberId` / `customerId` で統合し、顧客プロファイルの単一ビューを提供するデータプロダクトである。個人情報を含むため、Trino 側でのアクセス制御・マスキングを必須とする。

## ソース

| ソース | 内容 |
|---|---|
| `customer-events` | 顧客登録・更新イベント(datamesh-ai-agent-platform business-api が発行) |
| `loyalty-updates` | ロイヤリティポイント更新 |
| `rewards` | 特典付与イベント(`Reward`: customerName, orderId, rewardAmount) |
| `postgresql-prod.dronedb.public.customers`(Debezium CDC) | 顧客マスタの変更キャプチャ |
| `order_events`(OrderEvents) | `loyaltyMemberId` を含む注文イベント。購買履歴の紐付けに利用 |

## Flink 処理

`flink/customer-360-job.sql` にて:

1. `postgresql-prod.dronedb.public.customers` の CDC ストリームを `customer_id` をキーとするベースレコードとする。
2. `loyalty-updates` / `rewards` / OrderEvents(`loyaltyMemberId`)を `customer_id`(= `loyaltyMemberId`)で left join し、直近の購買・ロイヤリティ状態を付加する。
3. `customer_360`(upsert)として再公開する。

## Iceberg テーブル

- `customer_360` — 顧客ごとの統合プロファイル(upsert)。
- `customer_events_history` — 元イベントの監査ログ(append)。

## Trino 公開ビュー・アクセス制御

- `dataproducts.customer_360.profile_masked` — 一般ドメイン向け。氏名・連絡先等はカラムマスキング。
- `dataproducts.customer_360.profile_full` — 顧客対応・CRM 用途に限定し、行/カラムレベルのアクセス制御(Trino の `system-access-control` またはポリシーエンジン連携)で許可されたロールのみに公開する。

## 依存関係

OrderEvents, Postgres CDC(customers), loyalty-updates, rewards → Customer 360。他ドメインが顧客の購買・ロイヤリティ状況を必要とする場合は、この Customer 360 の Trino ビュー経由でのみ取得する(Postgres への直接接続は禁止)。
