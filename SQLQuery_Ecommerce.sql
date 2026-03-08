SELECT * FROM Ecommerce


SELECT * INTO RAW_Ecommerce
FROM Ecommerce

ALTER TABLE Ecommerce
DROP COLUMN 
    sales_commission_code,
    [Working Date],
    [M-Y],
    FY;

IF OBJECT_ID('dbo.vw_ecommerce_sales', 'V') IS NOT NULL
    DROP VIEW dbo.vw_ecommerce_sales;
GO

IF OBJECT_ID('dbo.ecommerce_clean', 'U') IS NOT NULL
    DROP TABLE dbo.ecommerce_clean;
GO

WITH src AS
(
    SELECT
        TRY_CONVERT(BIGINT, [item_id]) AS item_id,
        LOWER(LTRIM(RTRIM(CAST([status] AS NVARCHAR(100))))) AS status_raw,
        TRY_CONVERT(DATETIME, [created_at]) AS created_at,
        NULLIF(LTRIM(RTRIM(CAST([sku] AS NVARCHAR(255)))), '') AS sku,
        TRY_CONVERT(DECIMAL(18,2), [price]) AS price,
        TRY_CONVERT(INT, [qty_ordered]) AS qty_ordered,
        TRY_CONVERT(DECIMAL(18,2), [grand_total]) AS grand_total,
        TRY_CONVERT(BIGINT, [increment_id]) AS increment_id,
        NULLIF(LTRIM(RTRIM(CAST([category_name_1] AS NVARCHAR(255)))), '') AS category_name_1,
        ISNULL(TRY_CONVERT(DECIMAL(18,2), [discount_amount]), 0) AS discount_amount,
        LOWER(NULLIF(LTRIM(RTRIM(CAST([payment_method] AS NVARCHAR(100)))), '')) AS payment_method,
        NULLIF(LTRIM(RTRIM(CAST([BI Status] AS NVARCHAR(100)))), '') AS bi_status,
        TRY_CONVERT(INT, [Year]) AS order_year,
        TRY_CONVERT(INT, [Month]) AS order_month,
        NULLIF(LTRIM(RTRIM(CAST([Customer Since] AS NVARCHAR(50)))), '') AS customer_since,
        TRY_CONVERT(BIGINT, [Customer ID]) AS customer_id
    FROM dbo.Ecommerce
),

standardized AS
(
    SELECT
        item_id,
        CASE
            WHEN status_raw IN ('complete', 'completed') THEN 'complete'
            WHEN status_raw IN ('cancelled', 'canceled') THEN 'canceled'
            WHEN status_raw IN ('order_refunded', 'refund', 'refunded') THEN 'refunded'
            WHEN status_raw = 'received' THEN 'received'
            WHEN status_raw IS NULL OR status_raw = '' THEN 'unknown'
            ELSE status_raw
        END AS status,
        created_at,
        sku,
        price,
        qty_ordered,
        grand_total,
        increment_id,
        ISNULL(category_name_1, 'Unknown') AS category_name_1,
        discount_amount,
        ISNULL(payment_method, 'unknown') AS payment_method,
        ISNULL(bi_status, 'Unknown') AS bi_status,
        order_year,
        order_month,
        customer_since,
        customer_id
    FROM src
),

calc AS
(
    SELECT
        item_id,
        status,
        created_at,
        CAST(created_at AS DATE) AS order_date,
        sku,
        price,
        qty_ordered,
        increment_id,
        category_name_1,
        discount_amount,
        payment_method,
        bi_status,
        order_year,
        order_month,
        customer_since,
        customer_id,
        price * qty_ordered AS gross_sales,
        COALESCE(grand_total, (price * qty_ordered) - discount_amount) AS grand_total_clean,
        (price * qty_ordered) - discount_amount AS net_sales
    FROM standardized
),

dedup AS
(
    SELECT *,
           ROW_NUMBER() OVER
           (
               PARTITION BY item_id
               ORDER BY created_at DESC, increment_id DESC
           ) AS rn
    FROM calc
)

SELECT
    item_id,
    status,
    created_at,
    order_date,
    sku,
    price,
    qty_ordered,
    increment_id,
    category_name_1,
    discount_amount,
    payment_method,
    bi_status,
    order_year,
    order_month,
    customer_since,
    customer_id,
    gross_sales,
    grand_total_clean AS grand_total,
    net_sales,
    CASE WHEN status = 'complete' THEN 1 ELSE 0 END AS is_complete,
    CASE WHEN status = 'canceled' THEN 1 ELSE 0 END AS is_canceled,
    CASE WHEN status = 'refunded' THEN 1 ELSE 0 END AS is_refunded
INTO dbo.ecommerce_clean
FROM dedup
WHERE rn = 1
  AND item_id IS NOT NULL
  AND created_at IS NOT NULL
  AND sku IS NOT NULL
  AND price IS NOT NULL
  AND qty_ordered IS NOT NULL
  AND qty_ordered > 0
  AND price >= 0;
GO

CREATE OR ALTER VIEW dbo.vw_ecommerce_sales AS
SELECT *
FROM dbo.ecommerce_clean
WHERE status = 'complete';
GO

SELECT COUNT(*) AS clean_row_count
FROM dbo.ecommerce_clean;
GO

SELECT status, COUNT(*) AS row_count
FROM dbo.ecommerce_clean
GROUP BY status
ORDER BY row_count DESC;
GO

SELECT TOP 20 *
FROM dbo.ecommerce_clean
ORDER BY created_at;
GO

WITH q AS
(
    SELECT
        price,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price) OVER () AS Q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) OVER () AS Q3
    FROM dbo.ecommerce_clean
    WHERE price IS NOT NULL
),
iqr_calc AS
(
    SELECT DISTINCT
        (Q1 - 1.5 * (Q3 - Q1)) AS lower_bound,
        (Q3 + 1.5 * (Q3 - Q1)) AS upper_bound
    FROM q
)
DELETE e
FROM dbo.ecommerce_clean e
CROSS JOIN iqr_calc i
WHERE e.price < i.lower_bound
   OR e.price > i.upper_bound;



SELECT 
    item_id,
    COUNT(*) AS duplicate_count
FROM dbo.ecommerce_clean
GROUP BY item_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;


WITH cte AS
(
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY item_id
               ORDER BY created_at DESC, increment_id DESC
           ) AS rn
    FROM dbo.ecommerce_clean
)
DELETE FROM cte
WHERE rn > 1;


CREATE OR ALTER VIEW dbo.vw_ecommerce_dashboard AS
SELECT
    increment_id,
    category_name_1,
    payment_method,
    bi_status,
    order_year,
    order_month,
    customer_since,
    customer_id,
    is_complete,
    is_canceled,
    is_refunded,
    price_azn,
    discount_clean_azn AS discount_amount_azn,
    grand_total_azn,
    gross_sales_clean AS gross_sales,
    net_sales_clean AS net_sales
FROM dbo.ecommerce_clean;

SELECT * FROM vw_ecommerce_dashboard