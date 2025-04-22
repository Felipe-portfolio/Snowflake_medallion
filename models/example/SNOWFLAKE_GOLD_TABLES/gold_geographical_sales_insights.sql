{{ config(
    materialized='table',
    alias='gold_geographical_sales_insights',
    schema='GOLD_DBT',
    database='MANAGE_DB'
) }} 

SELECT
  DATE_TRUNC('month', fs.day) AS sales_month,
  c.ZIP_CODE,
  c.city_name,
  co.country_name,
  
  -- Aggregates
  SUM(fs.total_sold) AS total_units_sold,
  SUM(fs.price_in_dollars * fs.total_sold) AS gross_revenue,
  SUM((fs.price_in_dollars - (fs.discount * fs.price_in_dollars)) * fs.total_sold) AS net_revenue,

  -- Performance Ratios
  ROUND(SUM((fs.price_in_dollars - (fs.discount * fs.price_in_dollars)) * fs.total_sold) / 
        NULLIF(SUM(fs.price_in_dollars * fs.total_sold), 0), 2) AS discount_effectiveness_ratio,

  COUNT(DISTINCT fs.sales_order_key) AS total_orders,
  COUNT(DISTINCT fs.customer_key) AS unique_customers

FROM {{ ref('silver_fact_sales_daily') }} AS fs
LEFT JOIN {{ ref('silver_dim_customers') }} AS cust 
  ON fs.customer_key = cust.customerID
LEFT JOIN {{ ref('silver_dim_cities') }} AS c 
  ON cust.cityID = c.cityID
LEFT JOIN {{ ref('silver_dim_countries') }} AS co 
  ON c.country_code = co.country_code

GROUP BY 
  sales_month, c.ZIP_CODE, c.city_name, co.country_name

ORDER BY 
  sales_month, net_revenue DESC
