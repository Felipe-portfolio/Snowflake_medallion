{{ config(
    materialized='table',
    alias='silver_fact_sales',
    schema='SILVER_DBT',
    database='MANAGE_DB'
) }} 

SELECT
  s.Order_Key,
  s.transactionID,
  s.Sales_Date::DATE as sales_date,
  s.ProductID as sales_product_id,
  s.SALESPERSONID,
  cu.CustomerID   AS customer_key,
  e.EmployeeID,
  s.Total_Sold,
  s.Discount_in_percent,
  s.Total_Price,
  p.ProductID as product_product_id,
  p.Product_Name,
  p.Price_in_dollars,
  p.Product_Class,
  p.CategoryID,
  p.Modify_Date,
  p.RESITANCE_CATEGORY,
  p.Is_Allergic,
  p.Vitality_Days,
  ca.Category_Name
  FROM {{ ref ('SALES_RAW') }} as s
LEFT JOIN {{ ref ('CUSTOMERS_RAW') }} AS cu ON s.Customer_Key = cu.CustomerID
LEFT JOIN {{ ref ('EMPLOYEES_RAW') }} AS e ON s.salespersonID = e.EMPLOYEEID
left join  {{ ref ('PRODUCTS_RAW') }} as p on s.ProductID=p.productid
 LEFT JOIN {{ ref('CATEGORIES_RAW') }} as ca on ca.categoryID=p.categoryID
WHERE s.ORDER_KEY IS NOT NULL and Sales_Date is not null