--New customers acquired per month
SELECT
    DATE_TRUNC('month', CAST(signup_date AS DATE)) AS signup_month,
    COUNT(*) AS new_customers
FROM processed_customers
GROUP BY DATE_TRUNC('month', CAST(signup_date AS DATE))
ORDER BY signup_month;