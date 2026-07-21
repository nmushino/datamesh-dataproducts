# Assembly Line QDCA10pro

(旧名: Assembly Result QDCA10pro)

QDCA10pro ドメインサービスが発行する組立完了通知 (`orders-up` トピック) を、正式なデータプロダクトとしてガバナンス対象に位置づけたものである。

## この dataproduct のスコープ

- **プロデューサはこれまで通り QDCA10pro の Java サービス (`quarkusdroneshop-qdca10pro`)。** 在庫減算・組立処理といったドメインロジックや Kafka への発行コードは変更しない。
- ここで追加するのは、既存の `orders-up` トピックに対する **スキーマ登録 (Apicurio) によるガバナンス** のみ。Flink ジョブは持たない (`flink/` ディレクトリなし)。
- `orders-up` は現状 JSON (`ObjectMapperSerializer` 系) で発行されており、Avro スキーマは今回まだ enforce されない。まずはカタログ上で構造を自己記述化することが目的。

## スキーマ

- `schema/orders-up-event.avsc` — subject `orders-up-event-value`。
- [Assembly Line QDCA10](../dataproduct-assembly-line-qdca10/README.md) と同一スキーマ・同一 subject を共有する (両者とも同じ `orders-up` トピックに発行するため)。

## 下流での利用

- [OrderEvents](../dataproduct-order-events/README.md) の Flink ジョブが `orders-up` (QDCA10pro 発行分) を取り込み、`dataproduct-order-events` の `LINE_ITEM_STATUS_CHANGED` イベントへ変換・統合する。QDCA10pro の完了結果を他マイクロサービスへ伝搬する経路は、この OrderEvents 経由に統一されている (`orders-up` を他サービスが直接 subscribe することは想定しない)。

## 依存関係

QDCA10pro (ドメインサービス) → `orders-up` (このデータプロダクト) → OrderEvents。
