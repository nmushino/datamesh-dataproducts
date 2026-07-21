CREATE SCHEMA IF NOT EXISTS dataproducts.inventory_analytics;

-- Flink 側は FULL OUTER JOIN による retract ストリームを Iceberg の追記のみ
-- シンクへ書き込めないため、item・window ごとに「補充要求数の部分行」と
-- 「欠品キャンセル数の部分行」を別々に (もう片方は 0 で) append している。
-- ここで item・window 単位に合算し、当初想定していた1行1item・window の
-- 形へ復元する。
CREATE OR REPLACE VIEW dataproducts.inventory_analytics.turnover AS
SELECT
    item,
    window_start,
    window_end,
    SUM(restock_requested_count)  AS restock_requested_count,
    SUM(stockout_cancelled_count) AS stockout_cancelled_count
FROM iceberg.dataproducts.inventory_turnover
GROUP BY item, window_start, window_end;

CREATE OR REPLACE VIEW dataproducts.inventory_analytics.stockout_risk AS
SELECT item, at_risk, last_stockout_event_at
FROM iceberg.dataproducts.stockout_risk;
