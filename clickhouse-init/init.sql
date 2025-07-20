CREATE TABLE IF NOT EXISTS default.aggregated_analytics
(
    order_date       DateTime,
    total_sales      Float64,
    total_orders     UInt64,
    avg_order_value  Float64,
    top_product      String
)
ENGINE = MergeTree
ORDER BY order_date;
