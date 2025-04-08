{{ config(
    materialized='table',
    alias='gold_top_products_by_month',
    schema='GOLD_DBT',
    database='MANAGE_DB'
) }} 

SELECT
  DATE_TRUNC('month', day) AS sales_month,
  product_productid,
  product_name,
  product_class,
  category_name,
  SUM(total_sold) AS units_sold,
  SUM(price_in_dollars * total_sold) AS gross_revenue,
  SUM((price_in_dollars - (discount * price_in_dollars)) * total_sold) AS net_revenue
FROM {{ ref('silver_fact_sales_daily') }}
GROUP BY 1, 2, 3,4,5
ORDER BY net_revenue DESC
