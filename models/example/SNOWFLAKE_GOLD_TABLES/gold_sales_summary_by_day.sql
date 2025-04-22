{{ config(
    materialized='table',
    alias='gold_sales_summary_by_day',
    schema='GOLD_DBT',
    database='MANAGE_DB'
) }} 

SELECT
  DATE_TRUNC('day', day) AS sales_day,
  SUM(PRICE_IN_DOLLARS) AS total_revenue,
  SUM(total_sold) AS total_units_sold,
  SUM(price_in_dollars * discount) AS total_discounts_given,
SUM((price_in_dollars - (discount * price_in_dollars)) * total_sold) AS total_sold_discounted
FROM {{ ref('silver_fact_sales_daily') }}
WHERE day IS NOT NULL
GROUP BY 1
ORDER BY 1
