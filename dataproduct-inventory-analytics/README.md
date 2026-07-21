# Inventory Analytics

[Drone Component Stock](../dataproduct-inventory-event/README.md) と [OrderEvents](../dataproduct-order-events/README.md) を突き合わせ、部品の消費速度・欠品リスクを分析するデータプロダクトである。ドメイン内部の Postgres には一切アクセスせず、他の 2 データプロダクトの公開データのみを入力とする。

## ソース

- `dataproduct-inventory-events`(Drone Component Stock、旧名 `component_stock_events`) — 補充イベント。
- `order_events`(OrderEvents) — `ORDER_CANCELLED`(`eighty-six` に由来する欠品キャンセル)を主に利用する。

## Flink 処理

`flink/inventory-analytics-job.sql` は **常時稼働のストリーミングジョブ** であり、cron 等の定期実行やイベント単発起動ではない。`./script/ocpdeploy.sh dataproducts deploy dataproduct-inventory-analytics` で一度投入すると、Flink Session Cluster 上で無期限に稼働し続け、`dataproduct-inventory-events` / `dataproduct-order-events` に新着メッセージが来るたびに継続処理する。

### トリガーイベント

- **入力**: 上記2トピックへのメッセージ到着そのものが処理のトリガー。`scan.startup.mode = 'earliest-offset'` のため、ジョブ起動時点でトピック先頭から全履歴を一度読み込み、以降は新着分をリアルタイム消費する。
- **出力タイミング**:
  - **`inventory_turnover`**: `TUMBLE`(1時間の固定ウィンドウ)集計。壁時計時刻ではなく、実際に流れてくるイベントの `eventTimestamp` に基づく watermark が **ウィンドウの終端時刻を超えた瞬間** に、そのウィンドウの結果が1回だけ出力される。過去データを一気に読み込んだ直後は、複数時間分のウィンドウ結果が短時間にまとめて出力されることもある。
  - **`stockout_risk` (Kafka, upsert-kafka)**: ウィンドウなしの無限 GROUP BY。`ORDER_CANCELLED` イベントが来るたびに即座に更新 (upsert) される。
  - **`stockout_risk` (Iceberg)**: こちらは Iceberg の追記専用シンクが更新/削除ストリームを受け付けられないため、Kafka 版とは異なり 1時間 TUMBLE ウィンドウで区切って append する版になっている (下記「実装上の注意」参照)。

具体的な計算内容:

1. 1時間 Tumbling Window で `item` ごとの `RESTOCK_REQUESTED` 件数と `ORDER_CANCELLED`(欠品起因)件数を集計し `inventory_turnover` を生成する。
2. 欠品キャンセルが発生した item を `stockout_risk`(欠品リスクフラグ)としてマークする。

### 実装上の注意 (2026-07-21 修正)

当初は `FULL OUTER JOIN` で1item・1windowにつき1行へ結合してから `inventory_turnover` (通常の `kafka` コネクタ、append専用) へ書き込んでいたが、Flink の通常 JOIN オペレータは (入力がどちらも TUMBLE 集計後の append-only ストリームであっても) OUTER JOIN である限り retract ストリームを生成するため、"Table sink ... doesn't support consuming update and delete changes" でジョブが **プラン化の時点で毎回失敗しており、このデータプロダクトは一度も正常稼働していなかった**。

JOIN をやめ、item・window ごとに「補充要求数の部分行」と「欠品キャンセル数の部分行」(もう片方は 0) を **2本の別々の INSERT で append** する形に変更し、append-only 制約を回避した。集計 (2種の部分行を item・window 単位に合算) は Trino ビュー (`dataproducts.inventory_analytics.turnover`) 側の `GROUP BY` + `SUM` で行う。Iceberg 版の `inventory_turnover` / `stockout_risk` も同じ理由で JOIN を使わず、それぞれ append-only な形に設計してある。

## Kafka 公開トピック

- `inventory_turnover` — item・時間窓別の補充要求数/欠品キャンセル数の部分行 (append, avro-confluent, subject `inventory-turnover-value`)。
- `stockout_risk` — item ごとの現在の欠品リスク状態 (upsert-kafka, subject `stockout-risk-value`)。

## Iceberg テーブル

- `inventory_turnover` — item・時間窓別の補充要求数/欠品キャンセル数の部分行(append)。Trino ビュー側で item・window 単位に SUM 集計して公開する。
- `stockout_risk` — item・時間窓別の欠品リスクイベント(append)。

## Trino 公開ビュー

- `dataproducts.inventory_analytics.turnover`
- `dataproducts.inventory_analytics.stockout_risk`

## 依存関係

Drone Component Stock, OrderEvents → Inventory Analytics。
