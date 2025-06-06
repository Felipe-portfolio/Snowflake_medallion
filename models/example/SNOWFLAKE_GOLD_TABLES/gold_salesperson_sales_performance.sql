{{ config(
    materialized='table',
    alias='gold_salesperson_sales_performance',
    schema='GOLD_DBT',
    database='MANAGE_DB'
) }} 

SELECT
  DATE_TRUNC('month', fs.day) AS sales_month,
  p.category_name,
  sp.employeeid,
  sp.employee_first_name || ' ' || sp.employee_last_name AS salesperson_name,
  
  -- 📦 Total Units
  SUM(fs.total_sold) AS total_units,

  -- 💰 Gross Sales
  SUM(fs.price_in_dollars * fs.total_sold) AS gross_sales,

  -- 🤑 Discounted Sales
  SUM((fs.price_in_dollars - (fs.discount * fs.price_in_dollars)) * fs.total_sold) AS discounted_sales,

  -- 🎯 Rank by Discounted Sales
  DENSE_RANK() OVER (
    PARTITION BY DATE_TRUNC('month', fs.day), p.category_name
    ORDER BY SUM((fs.price_in_dollars - (fs.discount * fs.price_in_dollars)) * fs.total_sold) DESC
  ) AS performance_rank

FROM {{ ref('silver_fact_sales_daily') }} AS fs
LEFT JOIN {{ ref('silver_dim_employees') }} AS sp 
  ON fs.salesperson_key = sp.employeeid
LEFT JOIN {{ ref('silver_dim_products') }} AS p 
  ON fs.PRODUCT_PRODUCTID = p.PRODUCT_PRODUCTID

GROUP BY 
  DATE_TRUNC('month', fs.day),
  p.category_name,
  sp.employeeid, sp.employee_first_name, sp.employee_last_name

ORDER BY 
  sales_month, 
  p.category_name, 
  performance_rank