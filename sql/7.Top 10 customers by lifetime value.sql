--Top 10 customers by lifetime value
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.city,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(oi.quantity * oi.final_price), 2) AS lifetime_value
FROM processed_customers c
JOIN processed_orders      o  ON c.customer_id = o.customer_id
JOIN processed_order_items oi ON o.order_id    = oi.order_id
WHERE o.order_status = 'completed'
GROUP BY c.customer_id, c.first_name, c.last_name, c.city
ORDER BY lifetime_value DESC
LIMIT 10;