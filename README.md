# Data Products

QuarkusDroneShop におけるドメイン間のイベント処理・データアクセスを、既存サービス間の直接連携(生 Kafka トピックの相互購読)から **データプロダクト経由**に切り替えるための基盤である。

## 方針

- **ドメインをまたぐデータ連携は、必ずここに定義するデータプロダクト経由とする。** ドメインサービス(inventory / qdca10 / qdca10pro / homeoffice / web / counter 等)が他ドメインのトピックや DB を直接購読・参照することは禁止とする。
- データプロダクトは **Kafka を中心**に据える。ドメインの生イベントを取り込み、加工した結果を Kafka トピック(および Iceberg テーブル)として再公開する。
- スキーマは **Apicurio Service Registry** で一元管理し、互換性モード `BACKWARD` (可能な範囲で `BACKWARD_TRANSITIVE`) を強制する。ドメインサービス内部の独自シリアライザ(例: `OrderRecordDeserializer` 等)はそのままでよいが、データプロダクトの境界を越えるスキーマは Apicurio 登録の Avro スキーマに統一する。
- 採用技術:
  - **Apache Flink** — Stream Processing。ドメインの生イベントを正規化・集計・join してデータプロダクトを生成する。
  - **Apache Iceberg** — Table Format。Flink の出力を Iceberg テーブルとして永続化し、Time travel・監査・バッチ/ストリーム両対応のストレージとする。
  - **Trino** — SQL Query Engine。他ドメイン・BI・RHDH・AI Agent からのデータアクセスはすべて Trino 経由(`dataproducts.*` スキーマの公開ビューのみ)とする。ドメイン内部の Iceberg テーブルは直接公開しない。

### 命名・テーブル規約

各データプロダクトは、次の 2 系統の Iceberg テーブルを持つことを基本とする。

- `<product>_current` — 最新状態(upsert)
- `<product>_history` — 変更履歴(append-only)

### ディレクトリ構成

```
dataproducts/
  <product-name>/
    schema/    Apicurio 登録用 Avro スキーマ
    flink/     Flink Job (SQL / DataStream)
    iceberg/   Iceberg テーブル DDL
    trino/     Trino 公開ビュー定義
    README.md  プロダクト個別の設計・ソース・SLA
```

## データプロダクト一覧

| データプロダクト | 状態 | 概要 | 主なソース(既存トピック) |
|---|---|---|---|
| [OrderEvents](order-events/README.md) | 実装済み(スキーマ・Flink・Iceberg・Trino雛形) | 全ドメイン共通の正規化済み注文イベント。他プロダクトの土台となるハブ | `orders-in`, `orders-up`(qdca10 / qdca10pro / counter) |
| [Real-time Sales Trends](real-time-sales-trends/README.md) | 実装済み(同上) | 売上のリアルタイム集計(5分・日次) | OrderEvents |
| [Drone Component Stock](drone-component-stock/README.md) | 実装済み(同上) | 部品補充の現在ステータス・履歴 | `inventory-in`, `inventory-out`, `inventory-live` |
| [Inventory Analytics](inventory-analytics/README.md) | 実装済み(同上) | 在庫消費速度・欠品リスク分析 | Drone Component Stock, OrderEvents |
| [Assembly Lead Time QDCA10](assembly-lead-time-qdca10/README.md) | 実装済み(同上) | QDCA10 組立工程のリードタイム | OrderEvents(qdca10 由来) |
| [Assembly Lead Time QDCA10pro](assembly-lead-time-qdca10pro/README.md) | 実装済み(同上) | QDCA10pro 組立工程のリードタイム | OrderEvents(qdca10pro 由来) |
| [Customer 360](customer-360/README.md) | 実装済み(同上) | 顧客プロファイルの統合ビュー | `loyalty-updates`, `rewards`, Postgres CDC(`postgresql-prod.dronedb.public.customers`), OrderEvents |

依存関係: OrderEvents(ハブ) → Real-time Sales Trends / Drone Component Stock(独立) → Inventory Analytics(Drone Component Stock + OrderEvents) / Assembly Lead Time QDCA10 / QDCA10pro / Customer 360。

「実装済み」は Flink SQL・Iceberg DDL・Trino ビュー・Apicurio 用 Avro スキーマの雛形が揃った状態を指す。実際の Flink クラスタ・Apicurio Service Registry・Trino への接続先(URL・認証情報)が未確定のため、デプロイ(`quarkusdroneshop-ansible` へのタスク追加)は別途行う。

## ガバナンス・カタログ連携

- 各データプロダクトのスキーマ・オーナー・SLA・品質ルールは OpenMetadata(`openmetadata-export`)に登録し、RHDH の `rhdh-plugin-data-catalog` から検索可能にする。
- 新規トピック作成は `rhdh-plugin-kafka-topic-request` の申請フローを経由し、ドメイン間で無断にトピックを作成しない。
- スキーマ変更承認は将来的に `datamesh-ai-agent-platform/agent/governance-agent` と連携させる想定。

## 認証・認可

- **認証は Keycloak(RHBK, 既存の共通コンポーネント)に一元化する。** Flink / Apicurio / Trino それぞれに個別の資格情報を作らず、Keycloak にサービスアカウントクライアント(`dataproducts-flink`, `dataproducts-registry`, `trino-coordinator` 等)を作成し、OIDC / Client Credentials で認証する。具体的な realm・client-id・secret・接続先URLは環境ごとに異なるため、実装時にコードへ埋め込まず `oc create secret` で都度注入する。
- **認可(誰がどのデータを見られるか)は Trino 側のアクセス制御で行う。** Keycloak は「誰であるか」を確定させるだけで、行/カラムマスキングのような認可判断はできないため、Trino の File-based System Access Control(`quarkusdroneshop-ansible/openshift/dataproducts/trino-access-control-rules.json`)でスキーマ・テーブル・カラム単位の権限とマスキングルールを定義する。将来的にルールが複雑化した場合は OPA(`access-control.name=OPA`)への切り替えを想定し、`datamesh-ai-agent-platform` のガードレール(OPA + Keycloak)と方式を揃える。
- Customer 360 の PII カラム(氏名・メール)は、`dataproducts-consumer` グループには自動マスキング、`dataproducts-customer360-full` グループ(CRM等)のみ非マスキングとなるようルール分離している。

## デプロイ手順

Operator が OperatorHub 上に存在するもの(Flink Kubernetes Operator)は `quarkusdroneshop-ansible/script/ocpdeploy.sh` にまとめている。Trino は公式 Operator が存在しないため Helm チャートで導入するが、同じく `ocpdeploy.sh` から実行する。

```
# 1. Flink Operator インストール + Trino (Helm) デプロイ
#    事前に Keycloak へ trino-coordinator クライアントを作成し、
#    Secret trino-oidc (KEYCLOAK_ISSUER_URL / TRINO_OIDC_CLIENT_ID / TRINO_OIDC_CLIENT_SECRET) を作成しておく
./script/ocpdeploy.sh dataproducts setup

# 2. スキーマ登録 (Apicurio, Keycloak OIDC 認証)
KEYCLOAK_TOKEN_URL=... REGISTRY_CLIENT_ID=... REGISTRY_CLIENT_SECRET=... APICURIO_REGISTRY_URL=... \
    ./script/ocpdeploy.sh dataproducts schemas

# 3. Flink Session Cluster 起動 + 依存順(OrderEvents → 後続)でジョブ投入
./script/ocpdeploy.sh dataproducts deploy
```

関連ファイル: [flink-operator.yaml](../quarkusdroneshop-ansible/openshift/flink-operator.yaml), [flink-session-cluster.yaml](../quarkusdroneshop-ansible/openshift/dataproducts/flink-session-cluster.yaml), [trino-values.yaml](../quarkusdroneshop-ansible/openshift/dataproducts/trino-values.yaml), [trino-access-control-rules.json](../quarkusdroneshop-ansible/openshift/dataproducts/trino-access-control-rules.json), [register-schemas.sh](../quarkusdroneshop-ansible/script/register-schemas.sh), [submit-flink-jobs.sh](../quarkusdroneshop-ansible/script/submit-flink-jobs.sh)。
