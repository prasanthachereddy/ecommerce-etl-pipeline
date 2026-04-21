-- Q1_Total revenue by product category (completed orders only)
SELECT
    c.category_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity) AS total_units_sold,
    ROUND(
        SUM(CAST(oi.final_price AS DECIMAL(18,2)) * oi.quantity),
        2
    ) AS total_revenue_inr
FROM processed_order_items oi
JOIN processed_orders o ON oi.order_id = o.order_id
JOIN processed_products p ON oi.product_id = p.product_id
JOIN processed_categories c ON p.category_id = c.category_id
WHERE o.order_status = 'completed'
GROUP BY c.category_name
ORDER BY total_revenue_inr DESC;
 
 -- Q2_Top 10 customers by total spend
SELECT
    cu.customer_id,
    cu.first_name || ' ' || cu.last_name AS customer_name,
    cu.city,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(oi.final_price * oi.quantity), 2) AS total_spend_inr
FROM processed_customers cu
JOIN processed_orders o ON cu.customer_id = o.customer_id
JOIN processed_order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'completed'
GROUP BY cu.customer_id, cu.first_name, cu.last_name, cu.city
ORDER BY total_spend_inr DESC
LIMIT 10;


-- Q3_Monthly order volume and revenue trend
SELECT
    SUBSTR(CAST(o.order_date AS VARCHAR), 1, 7) AS order_month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    ROUND(SUM(oi.final_price * oi.quantity), 2) AS monthly_revenue_inr
FROM processed_orders o
JOIN processed_order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'completed'
GROUP BY SUBSTR(CAST(o.order_date AS VARCHAR), 1, 7)
ORDER BY order_month ASC;



-- Q4_Revenue and order count by payment method
SELECT
    o.payment_method,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(COUNT(DISTINCT o.order_id) * 100.0 /
          SUM(COUNT(DISTINCT o.order_id)) OVER(), 2) AS pct_of_orders,
    ROUND(SUM(oi.final_price * oi.quantity), 2) AS total_revenue_inr
FROM processed_orders o
JOIN processed_order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'completed'
GROUP BY o.payment_method
ORDER BY total_orders DESC;




-- Q5_Top product per category by units sold (using RANK window function)
WITH product_sales AS (
    SELECT
        p.product_name,
        c.category_name,
        SUM(oi.quantity) AS total_units_sold,
        ROUND(SUM(oi.final_price * oi.quantity), 2) AS total_revenue_inr,
        RANK() OVER (
            PARTITION BY c.category_id
            ORDER BY SUM(oi.quantity) DESC
        ) AS rank_in_category
    FROM processed_order_items oi
    JOIN processed_products p ON oi.product_id = p.product_id
    JOIN processed_categories c ON p.category_id = c.category_id
    GROUP BY p.product_name, c.category_name, c.category_id
)
SELECT *
FROM product_sales
WHERE rank_in_category = 1
ORDER BY total_revenue_inr DESC;