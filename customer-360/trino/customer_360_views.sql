CREATE SCHEMA IF NOT EXISTS dataproducts.customer_360;

-- CRM / 顧客対応ロール限定。GRANT SELECT は customer360_full ロールにのみ付与する運用とする。
CREATE OR REPLACE VIEW dataproducts.customer_360.profile_full AS
SELECT
    customer_id,
    customer_name,
    email,
    loyalty_points,
    last_reward_at,
    last_order_id,
    last_order_at,
    total_orders,
    updated_at
FROM iceberg.dataproducts.customer_360;

-- 一般ドメイン向け。氏名・メールをマスキングし、購買・ロイヤリティ状態のみ公開する。
CREATE OR REPLACE VIEW dataproducts.customer_360.profile_masked AS
SELECT
    customer_id,
    CONCAT(SUBSTR(customer_name, 1, 1), '***')             AS customer_name,
    REGEXP_REPLACE(email, '(^.).*(@.*$)', '$1***$2')        AS email,
    loyalty_points,
    last_reward_at,
    last_order_id,
    last_order_at,
    total_orders,
    updated_at
FROM iceberg.dataproducts.customer_360;
