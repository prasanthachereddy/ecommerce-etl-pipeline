-- ============================================================
-- E-Commerce ETL Pipeline — Athena Analytics Queries
-- Author  : Chereddy Prasantha Reddy
-- Tables  : processed_orders, processed_order_items,
--           processed_customers, processed_products,
--           processed_categories
-- ============================================================


-- ── Q1: Pipeline validation — raw vs processed row counts ───
SELECT 'raw_orders'             AS table_name, COUNT(*) AS row_count FROM raw_orders
UNION ALL
SELECT 'processed_orders',                     COUNT(*) FROM processed_orders
UNION ALL
SELECT 'raw_order_items',                      COUNT(*) FROM raw_order_items
UNION ALL
SELECT 'processed_order_items',                COUNT(*) FROM processed_order_items
UNION ALL
SELECT 'raw_customers',                        COUNT(*) FROM raw_customers
UNION ALL
SELECT 'processed_customers',                  COUNT(*) FROM processed_customers;


-- ── Q2: Total revenue by category ───────────────────────────
SELECT
    c.category_name,
    COUNT(DISTINCT o.order_id)               AS total_orders,
    SUM(oi.quantity)                         AS total_units_sold,
    ROUND(SUM(oi.total_amount), 2)           AS total_revenue,
    ROUND(AVG(oi.unit_price), 2)             AS avg_unit_price
FROM processed_order_items oi
JOIN processed_products  p  ON oi.product_id  = p.product_id
JOIN processed_categories c  ON p.category_id = c.category_id
JOIN processed_orders     o  ON oi.order_id   = o.order_id
WHERE o.order_status = 'completed'
GROUP BY c.category_name
ORDER BY total_revenue DESC;


-- ── Q3: Top 5 products by revenue ───────────────────────────
SELECT
    oi.product_name,
    COUNT(DISTINCT oi.order_id)              AS times_ordered,
    SUM(oi.quantity)                         AS total_units_sold,
    ROUND(SUM(oi.total_amount), 2)           AS total_revenue
FROM processed_order_items oi
JOIN processed_orders o ON oi.order_id = o.order_id
WHERE o.order_status = 'completed'
GROUP BY oi.product_name
ORDER BY total_revenue DESC
LIMIT 5;


-- ── Q4: Revenue by payment method ───────────────────────────
SELECT
    payment_method,
    COUNT(*)                                 AS total_orders,
    ROUND(SUM(oi.total_amount), 2)           AS total_revenue,
    ROUND(AVG(oi.total_amount), 2)           AS avg_order_value
FROM processed_orders o
JOIN processed_order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'completed'
GROUP BY payment_method
ORDER BY total_revenue DESC;


-- ── Q5: Order status breakdown ───────────────────────────────
SELECT
    order_status,
    COUNT(*)                                 AS total_orders,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM processed_orders
GROUP BY order_status
ORDER BY total_orders DESC;


-- ── Q6: Monthly revenue trend ────────────────────────────────
SELECT
    DATE_TRUNC('month', CAST(order_date AS DATE)) AS order_month,
    COUNT(DISTINCT o.order_id)               AS total_orders,
    ROUND(SUM(oi.total_amount), 2)           AS monthly_revenue
FROM processed_orders o
JOIN processed_order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'completed'
GROUP BY DATE_TRUNC('month', CAST(order_date AS DATE))
ORDER BY order_month;


-- ── Q7: Top 10 customers by lifetime value ───────────────────
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name       AS customer_name,
    c.city,
    COUNT(DISTINCT o.order_id)               AS total_orders,
    ROUND(SUM(oi.total_amount), 2)           AS lifetime_value
FROM processed_customers c
JOIN processed_orders     o  ON c.customer_id = o.customer_id
JOIN processed_order_items oi ON o.order_id   = oi.order_id
WHERE o.order_status = 'completed'
GROUP BY c.customer_id, c.first_name, c.last_name, c.city
ORDER BY lifetime_value DESC
LIMIT 10;


-- ── Q8: Customer segmentation by order count ─────────────────
SELECT
    CASE
        WHEN order_count = 1 THEN '1 order'
        WHEN order_count BETWEEN 2 AND 3 THEN '2-3 orders'
        WHEN order_count BETWEEN 4 AND 5 THEN '4-5 orders'
        ELSE '6+ orders'
    END                                      AS segment,
    COUNT(*)                                 AS customer_count,
    ROUND(COUNT(*) * 100.0 /
          SUM(COUNT(*)) OVER (), 2)          AS percentage
FROM (
    SELECT customer_id, COUNT(DISTINCT order_id) AS order_count
    FROM processed_orders
    WHERE order_status = 'completed'
    GROUP BY customer_id
) customer_orders
GROUP BY
    CASE
        WHEN order_count = 1 THEN '1 order'
        WHEN order_count BETWEEN 2 AND 3 THEN '2-3 orders'
        WHEN order_count BETWEEN 4 AND 5 THEN '4-5 orders'
        ELSE '6+ orders'
    END
ORDER BY customer_count DESC;


-- ── Q9: Running total revenue by date ────────────────────────
SELECT
    CAST(o.order_date AS DATE)               AS order_date,
    ROUND(SUM(oi.total_amount), 2)           AS daily_revenue,
    ROUND(SUM(SUM(oi.total_amount)) OVER (
        ORDER BY CAST(o.order_date AS DATE)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                    AS running_total
FROM processed_orders o
JOIN processed_order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'completed'
GROUP BY CAST(o.order_date AS DATE)
ORDER BY order_date;


-- ── Q10: Top 3 products per category by revenue ──────────────
SELECT * FROM (
    SELECT
        c.category_name,
        oi.product_name,
        ROUND(SUM(oi.total_amount), 2)       AS revenue,
        RANK() OVER (
            PARTITION BY c.category_name
            ORDER BY SUM(oi.total_amount) DESC
        )                                    AS rnk
    FROM processed_order_items oi
    JOIN processed_products   p ON oi.product_id  = p.product_id
    JOIN processed_categories c ON p.category_id  = c.category_id
    JOIN processed_orders     o ON oi.order_id    = o.order_id
    WHERE o.order_status = 'completed'
    GROUP BY c.category_name, oi.product_name
) ranked
WHERE rnk <= 3
ORDER BY category_name, rnk;


-- ── Q11: Average discount by category ────────────────────────
SELECT
    c.category_name,
    ROUND(AVG(oi.discount), 2)               AS avg_discount_pct,
    ROUND(SUM(oi.unit_price * oi.quantity)
        - SUM(oi.total_amount), 2)           AS total_discount_given
FROM processed_order_items oi
JOIN processed_products   p ON oi.product_id  = p.product_id
JOIN processed_categories c ON p.category_id  = c.category_id
JOIN processed_orders     o ON oi.order_id    = o.order_id
WHERE o.order_status = 'completed'
GROUP BY c.category_name
ORDER BY avg_discount_pct DESC;


-- ── Q12: New customers acquired per month ────────────────────
SELECT
    DATE_TRUNC('month', CAST(signup_date AS DATE)) AS signup_month,
    COUNT(*)                                 AS new_customers
FROM processed_customers
GROUP BY DATE_TRUNC('month', CAST(signup_date AS DATE))
ORDER BY signup_month;
