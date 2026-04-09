--Order status breakdown
SELECT
    order_status,
    COUNT(*) AS total_orders,
    ROUND(COUNT(*) * 100.0 /
          SUM(COUNT(*)) OVER (), 2) AS percentage
FROM processed_orders
GROUP BY order_status
ORDER BY total_orders DESC;