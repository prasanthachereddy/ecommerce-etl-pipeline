--Pipeline validation
SELECT 'raw_orders' AS table_name, COUNT(*) AS row_count FROM raw_orders
UNION ALL
SELECT 'processed_orders',COUNT(*) FROM processed_orders
UNION ALL
SELECT 'raw_order_items', COUNT(*) FROM raw_order_items
UNION ALL
SELECT 'processed_order_items', COUNT(*) FROM processed_order_items
UNION ALL
SELECT 'raw_customers', COUNT(*) FROM raw_customers
UNION ALL
SELECT 'processed_customers', COUNT(*) FROM processed_customers;