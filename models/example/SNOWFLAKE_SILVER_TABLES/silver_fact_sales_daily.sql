{{ config(
    materialized='table',
    alias='silver_fact_daily_sales',
    schema='SILVER_DBT',
    database='MANAGE_DB'
) }} 

SELECT
  DATE_TRUNC('day', sa.sales_date) AS day,
  p.product_ProductID,
  sa.order_key as sales_order_key,
  p.product_name,
  p.product_class,
  p.category_name,
  concat(e.employee_first_name,' ',e.employee_last_name) as salesperson,
  sa.Total_sold,
  sa.discount_in_percent as discount,
  p.price_in_dollars as price_in_dollars,
  p.price_in_dollars - (sa.discount_in_percent * p.price_in_dollars) as price_with_discount,
  p.price_in_dollars * sa.total_sold as gross_total_revenue,
  price_with_discount * sa.total_sold as total_revenue_discount,
  gross_total_revenue - total_revenue_discount as discount_in_dollars 
FROM {{ref ('silver_fact_sales')}} AS sa
left join {{ref ('silver_dim_products') }} as p on p.product_productid=sa.sales_productid
left join {{ref ('silver_dim_employees') }} as e on e.employeeid=sa.salespersonid
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
order by 1 asc
