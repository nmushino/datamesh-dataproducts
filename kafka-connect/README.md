# Kafka Connect (Debezium)

quarkusdroneshop-inventory の [Debezium Outbox パターン](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html) (`droneshop.outboxevent` テーブル) を実際に Kafka へキャプチャするための Kafka Connect クラスタ用イメージ。

Strimzi (AMQ Streams) の `KafkaConnect` リソースは独自のエントリポイント/レイアウトを前提とするため、任意の Debezium 公式イメージをそのまま使うことはできない。このディレクトリの [`Dockerfile`](./Dockerfile) は、このクラスタで稼働中の AMQ Streams と同じベースイメージ (`registry.redhat.io/amq-streams/kafka-41-rhel9`) に Debezium PostgreSQL コネクタプラグインを追加してビルドする。

## デプロイ順序

新規サイトに inventory をデプロイする際は、以下の順序で実行すること。順序を誤ると (特に手順3を inventory デプロイ前に行うと) `droneshop.outboxevent` テーブルが存在しない状態でコネクタが起動を試みて失敗する。

1. `./script/ocpdeploy.sh dataproducts setup`
   Flink Kubernetes Operator / MinIO / Keycloak クライアント / Trino をセットアップする。
2. inventory サービスをデプロイする
   (`quarkus.hibernate-orm.database.generation=drop-and-create` により、起動時に `droneshop.outboxevent` を含む全テーブルが作成される)。
3. `./script/ocpdeploy.sh dataproducts debezium`
   このディレクトリの `Dockerfile` を OpenShift Docker Build でビルド・push し、[`inventory-outbox-connect.yaml`](../../quarkusdroneshop-ansible/openshift/dataproducts/inventory-outbox-connect.yaml) (`KafkaConnect` + `KafkaConnector`) を適用する。DB パスワード (`droneshopdb-pguser-droneshopadmin`) は実行時に Secret から取得して埋め込まれる (YAML 自体には含まれない)。
4. `./script/ocpdeploy.sh dataproducts deploy dataproduct-inventory-event`
   [Dataproduct Inventory Event](../dataproduct-inventory-event/README.md) (旧名 Drone Component Stock) の Flink ジョブを投入し、`inventory-out` → `dataproduct-component-stock-quantity` の変換を開始する。

状態確認: `oc get kafkaconnect,kafkaconnector -n quarkusdroneshop-demo`

## 冪等性に関する注意

`dataproducts debezium` は再実行しても安全 (イメージは毎回リビルドされ、CR は `oc apply` で差分適用される)。ただし以下のケースでは **手動対応が必要**:

- inventory を再デプロイして `droneshop.outboxevent` の OID が変わった場合、既存の Postgres レプリケーションスロット/パブリケーションが古い OID を指したままになることがある。[`inventory-outbox-connect.yaml`](../../quarkusdroneshop-ansible/openshift/dataproducts/inventory-outbox-connect.yaml) では `publication.autocreate.mode=all_tables` にしてこの問題を回避しているが、それでも WAL の位置 (LSN) と Kafka Connect 側に保存された前回オフセットが食い違い、コネクタタスクが `FAILED` になることがある。
- **`droneshopdb` の Pod が再作成された場合も同様の問題が起きる** (例: SCC 拒否や OOM 等で Pod が再スケジュールされた場合)。レプリケーションスロットは Pod 再作成時に失われることがあり、Kafka Connect 側に保存された古い LSN オフセットが「もう存在しない」状態になって `DebeziumException: ... but this is no longer available on the server` で `FAILED` になる (2026-07-21 に実際に発生・対応済み)。
- その場合は次の手順でリセットする (`vN` は現在の接尾辞、次は `vN+1` にする。現在の接尾辞は [`inventory-outbox-connect.yaml`](../../quarkusdroneshop-ansible/openshift/dataproducts/inventory-outbox-connect.yaml) の `metadata.name`/`slot.name`/`publication.name` を参照):
  1. `oc delete kafkaconnector -l strimzi.io/cluster=dataproducts-connect -n quarkusdroneshop-demo`
  2. Postgres 側のスロット/パブリケーションを削除
     (`select pg_drop_replication_slot('inventory_outbox_slot_vN');` / `drop publication inventory_outbox_publication_vN;`)
  3. `inventory-outbox-connect.yaml` の `slot.name`/`publication.name`/コネクタ名 (`vN` → `vN+1`) を変更し、`./script/ocpdeploy.sh dataproducts debezium` を再実行する。
     (Kafka Connect の内部オフセットストレージは接続名で紐付いているため、名前を変えずに古いオフセットのまま再起動すると同じエラーで失敗し続ける。)

## 過去にハマった問題 (実装時のトラブルシュート記録)

- `InventoryService` が以前 `inventory-out` に `RestockInventoryCommand.toString()` (非JSON) を直接書き込んでいたため、JSON コンシューマ (dataproduct-inventory-event の Flink ジョブ) がクラッシュしていた。該当コードは削除済み。
- Debezium の `EventRouter` はデフォルトで `timestamp` フィールドを INT64 (epoch) として読もうとするが、`debezium-quarkus-outbox` の `timestamp` 列は `TIMESTAMP WITH TIME ZONE` (Debezium 上は ZonedTimestamp = 文字列) のため型不一致で失敗する (`Field 'timestamp' is not of type INT64`)。`ReplaceField` SMT で `EventRouter` に渡す前にこのフィールド自体を除去している。
- `payload` 列は既にアプリ側で組み立て済みの JSON 文字列。Connect の `value.converter` に `JsonConverter` (schemas.enable=false) を使うと、この文字列がさらに JSON エンコードされ二重引用符文字列 (`"{\"skuId\":...}"`) になってしまう。`StringConverter` を使い生のバイト列としてそのまま書き出すこと。
- `eventType` は `outboxevent.type` 列にのみ存在し、Debezium Outbox の標準構成では `payload` 内に自動転記されない。下流の Flink ジョブが `eventType` をフラットなトップレベルフィールドとして必要とするため、`RestockRequestedEvent`/`RestockCompletedEvent` のペイロード生成時 (quarkusdroneshop-inventory 側) に `eventType` を明示的に含めている。
