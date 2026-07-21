CREATE SCHEMA IF NOT EXISTS dataproducts.customer_360;

-- 履歴 (customer_events_history) から customer_name ごとの最新状態を算出する。
-- PII (customer_name / loyalty_member_id) を含むフル版。CRM 等、権限を持つロールのみに
-- 公開すること (Trino 側のアクセス制御ルールで別途制限する)。
CREATE OR REPLACE VIEW dataproducts.customer_360.profile_full AS
WITH ranked AS (
    SELECT
        customer_name,
        loyalty_member_id,
        location,
        order_id,
        event_timestamp,
        ROW_NUMBER() OVER (PARTITION BY customer_name ORDER BY event_timestamp DESC) AS rn,
        COUNT(*) OVER (PARTITION BY customer_name) AS total_orders
    FROM iceberg.dataproducts.customer_events_history
)
SELECT
    customer_name,
    loyalty_member_id,
    location        AS last_location,
    order_id        AS last_order_id,
    event_timestamp AS last_order_at,
    total_orders
FROM ranked
WHERE rn = 1;

-- 一般ドメイン向け。氏名・ロイヤリティ会員IDをマスキングした版。
CREATE OR REPLACE VIEW dataproducts.customer_360.profile_masked AS
SELECT
    CONCAT(SUBSTR(customer_name, 1, 1), '***')       AS customer_name,
    CASE WHEN loyalty_member_id IS NULL THEN NULL ELSE '***' END AS loyalty_member_id,
    last_location,
    last_order_id,
    last_order_at,
    total_orders
FROM dataproducts.customer_360.profile_full;
