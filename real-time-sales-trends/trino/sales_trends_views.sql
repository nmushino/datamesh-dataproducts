CREATE SCHEMA IF NOT EXISTS dataproducts.sales_trends;

CREATE OR REPLACE VIEW dataproducts.sales_trends.trends_5m AS
SELECT item, window_start, window_end, order_count, revenue, assembly_line, location
FROM iceberg.dataproducts.sales_trends_5m;

CREATE OR REPLACE VIEW dataproducts.sales_trends.trends_daily AS
SELECT item, window_start, window_end, order_count, revenue, assembly_line, location
FROM iceberg.dataproducts.sales_trends_daily;
