--Total revenue by category
SELECT
    c.category_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity) AS total_units_sold,
    ROUND(SUM(oi.quantity * oi.final_price), 2) AS total_revenue,
    ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
FROM processed_order_items oi
JOIN processed_products p ON oi.product_id  = p.product_id
JOIN processed_categories c ON p.category_id  = c.category_id
JOIN processed_orders o ON oi.order_id = o.order_id
WHERE o.order_status = 'completed'
GROUP BY c.category_name
ORDER BY total_revenue DESC;