--Average discount by category
SELECT
    c.category_name,
    ROUND(AVG(oi.discount), 2)            AS avg_discount_pct,
    ROUND(SUM(oi.unit_price * oi.quantity)
        - SUM(oi.quantity * oi.final_price), 2) AS total_discount_given
FROM processed_order_items oi
JOIN processed_products   p ON oi.product_id = p.product_id
JOIN processed_categories c ON p.category_id = c.category_id
JOIN processed_orders     o ON oi.order_id   = o.order_id
WHERE o.order_status = 'completed'
GROUP BY c.category_name
ORDER BY avg_discount_pct DESC;