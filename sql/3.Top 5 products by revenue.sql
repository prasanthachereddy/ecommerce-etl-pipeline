--Top 5 products by revenue
SELECT
    oi.product_name,
    COUNT(DISTINCT oi.order_id) AS times_ordered,
    SUM(oi.quantity) AS total_units_sold,
    ROUND(SUM(oi.quantity * oi.final_price), 2) AS total_revenue
FROM processed_order_items oi
JOIN processed_orders o ON oi.order_id = o.order_id
WHERE o.order_status = 'completed'
GROUP BY oi.product_name
ORDER BY total_revenue DESC
LIMIT 5;