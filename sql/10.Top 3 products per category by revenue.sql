--Top 3 products per category by revenue
SELECT * FROM (
    SELECT
        c.category_name,
        oi.product_name,
        ROUND(SUM(oi.quantity * oi.final_price), 2) AS revenue,
        RANK() OVER (
            PARTITION BY c.category_name
            ORDER BY SUM(oi.quantity * oi.final_price) DESC
        ) AS rnk
    FROM processed_order_items oi
    JOIN processed_products   p ON oi.product_id = p.product_id
    JOIN processed_categories c ON p.category_id = c.category_id
    JOIN processed_orders     o ON oi.order_id   = o.order_id
    WHERE o.order_status = 'completed'
    GROUP BY c.category_name, oi.product_name
) ranked
WHERE rnk <= 3
ORDER BY category_name, rnk;