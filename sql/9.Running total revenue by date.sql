--Running total revenue by date
SELECT
    CAST(o.order_date AS DATE) AS order_date,
    ROUND(SUM(oi.quantity * oi.final_price), 2) AS daily_revenue,
    ROUND(SUM(SUM(oi.quantity * oi.final_price)) OVER (
        ORDER BY CAST(o.order_date AS DATE)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2) AS running_total
FROM processed_orders o
JOIN processed_order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'completed'
GROUP BY CAST(o.order_date AS DATE)
ORDER BY order_date;