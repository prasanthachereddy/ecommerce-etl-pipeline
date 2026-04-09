--Revenue by payment method
SELECT
    o.payment_method,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(oi.quantity * oi.final_price), 2) AS total_revenue,
    ROUND(AVG(oi.quantity * oi.final_price), 2) AS avg_order_value
FROM processed_orders o
JOIN processed_order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'completed'
GROUP BY o.payment_method
ORDER BY total_revenue DESC;