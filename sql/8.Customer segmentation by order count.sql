-- Customer segmentation by order count
SELECT
    CASE
        WHEN order_count = 1            THEN '1 order'
        WHEN order_count BETWEEN 2 AND 3 THEN '2-3 orders'
        WHEN order_count BETWEEN 4 AND 5 THEN '4-5 orders'
        ELSE '6+ orders'
    END AS segment,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 /
          SUM(COUNT(*)) OVER (), 2) AS percentage
FROM (
    SELECT customer_id, COUNT(DISTINCT order_id) AS order_count
    FROM processed_orders
    WHERE order_status = 'completed'
    GROUP BY customer_id
) t
GROUP BY
    CASE
        WHEN order_count = 1 THEN '1 order'
        WHEN order_count BETWEEN 2 AND 3 THEN '2-3 orders'
        WHEN order_count BETWEEN 4 AND 5 THEN '4-5 orders'
        ELSE '6+ orders'
    END
ORDER BY customer_count DESC;