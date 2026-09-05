-- ==============================================
-- GOLD DAILY SALES
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_sales
COMMENT 'Daily sales summary'
AS
SELECT
    CAST(sale_timestamp AS DATE) AS sale_date,
    COUNT(DISTINCT sale_id) AS total_transactions,
    SUM(quantity) AS total_quantity,
    SUM(sale_amount) AS total_sales,
    AVG(sale_amount) AS average_transaction_value
FROM silver_sales
GROUP BY CAST(sale_timestamp AS DATE);


-- ============================================================
-- GOLD SALES BY PRODUCT
-- =======================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_sales_by_product
COMMENT 'Sales performance by product'
AS
SELECT
    product_id,
    product_name,
    category,
    COUNT(DISTINCT sale_id) AS total_transactions,
    SUM(quantity) AS total_quantity,
    SUM(sale_amount) AS total_sales,
    AVG(sale_amount) AS average_sale
FROM silver_sales
GROUP BY
    product_id,
    product_name,
    category;


-- ================================================
-- GOLD SALES BY CITY
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_sales_by_city
COMMENT 'Sales performance by customer city'
AS
SELECT
    city,
    COUNT(DISTINCT sale_id) AS total_transactions,
    SUM(quantity) AS total_quantity,
    SUM(sale_amount) AS total_sales,
    AVG(sale_amount) AS average_sale
FROM silver_sales
WHERE city IS NOT NULL
GROUP BY city;


-- ========================================================
-- GOLD CUSTOMER SALES
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_customer_sales
COMMENT 'Customer sales summary'
AS
SELECT
    customer_id,
    customer_name,
    city,
    COUNT(DISTINCT sale_id) AS total_transactions,
    SUM(quantity) AS total_quantity,
    SUM(sale_amount) AS total_sales,
    AVG(sale_amount) AS average_transaction_value
FROM silver_sales
WHERE customer_id IS NOT NULL
GROUP BY
    customer_id,
    customer_name,
    city;
