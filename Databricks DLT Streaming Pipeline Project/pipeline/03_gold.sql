-- Daily sales

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_sales
AS
SELECT
    DATE(sale_timestamp) AS sale_date,
    SUM(sale_amount) AS total_sales,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT sale_id) AS total_orders
FROM silver_sales
GROUP BY DATE(sale_timestamp);

-- Sales by product

CREATE OR REFRESH MATERIALIZED VIEW gold_product_sales
AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(s.quantity) AS units_sold,
    SUM(s.sale_amount) AS revenue
FROM silver_sales s
JOIN silver_products_scd2 p
    ON s.product_id = p.product_id
WHERE p.__END_AT IS NULL
GROUP BY
    p.product_id,
    p.product_name,
    p.category;

-- Customer sales

CREATE OR REFRESH MATERIALIZED VIEW gold_customer_sales
AS
SELECT
    c.customer_id,
    c.customer_name,
    c.city,
    COUNT(s.sale_id) AS order_count,
    SUM(s.sale_amount) AS total_spend
FROM silver_sales s
JOIN silver_customers_scd1 c
    ON s.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.customer_name,
    c.city;

-- Executive summary
-- (This table use as one of the dashboard datasets)

CREATE OR REFRESH MATERIALIZED VIEW gold_sales_summary
AS
SELECT
    COUNT(DISTINCT sale_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS active_customers,
    SUM(quantity) AS units_sold,
    SUM(sale_amount) AS total_revenue,
    AVG(sale_amount) AS average_order_value
FROM silver_sales;


