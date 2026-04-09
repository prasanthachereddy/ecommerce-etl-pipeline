-- Monthly revenue trend
SELECT
    DATE_TRUNC('month', CAST(o.order_date AS DATE)) AS order_month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(oi.quantity * oi.final_price), 2) AS monthly_revenue,
    round(sum(oi.quantity*oi.final_price)/1000000,2) as monthly_revenue_millions

FROM processed_orders o
JOIN processed_order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'completed'
GROUP BY DATE_TRUNC('month', CAST(o.order_date AS DATE))
ORDER BY order_month;