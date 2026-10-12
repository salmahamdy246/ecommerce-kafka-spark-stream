CREATE DATABASE IF NOT EXISTS ecommerce_dw;
USE ecommerce_dw;

CREATE EXTERNAL TABLE IF NOT EXISTS streaming_orders (
    order_id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    price DOUBLE,
    total_amount DOUBLE,
    order_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/ecommerce_dw.db/streaming_orders';

SELECT COUNT(DISTINCT order_id) AS total_orders 
FROM streaming_orders;

SELECT ROUND(SUM(total_amount), 2) AS gross_revenue 
FROM streaming_orders;

SELECT ROUND(AVG(total_amount), 2) AS average_order_value 
FROM streaming_orders;

SELECT 
    product_id, 
    SUM(quantity) AS total_units_sold, 
    ROUND(SUM(total_amount), 2) AS product_revenue 
FROM streaming_orders 
GROUP BY product_id 
ORDER BY product_revenue DESC;

SELECT 
    customer_id, 
    COUNT(order_id) AS total_orders, 
    ROUND(SUM(total_amount), 2) AS customer_expenditure 
FROM streaming_orders 
GROUP BY customer_id 
ORDER BY customer_expenditure DESC;

SELECT 
    product_id, 
    ROUND(SUM(total_amount), 2) AS gross_revenue 
FROM streaming_orders 
GROUP BY product_id 
ORDER BY gross_revenue DESC 
LIMIT 5;
