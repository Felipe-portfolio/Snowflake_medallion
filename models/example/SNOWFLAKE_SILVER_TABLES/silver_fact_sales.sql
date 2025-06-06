{{ config(
    materialized='table',
    alias='silver_fact_sales',
    schema='SILVER_DBT',
    database='MANAGE_DB'
) }} 

WITH deduplicated_sales AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY s.Order_Key ORDER BY s.Sales_Date) AS rn
  FROM {{ ref('SALES_RAW') }} as s
  LEFT JOIN {{ ref('CUSTOMERS_RAW') }} AS cu ON s.Customer_Key = cu.CustomerID
  LEFT JOIN {{ ref('EMPLOYEES_RAW') }} AS e ON s.salespersonID = e.EMPLOYEEID
  LEFT JOIN {{ ref('PRODUCTS_RAW') }} AS p ON s.Sales_ProductID = p.Product_ProductID
  LEFT JOIN {{ ref('CATEGORIES_RAW') }} AS ca ON ca.Category_CategoryID = p.Product_CategoryID
  WHERE s.Order_Key IS NOT NULL AND s.Sales_Date IS NOT NULL
)

SELECT
  Order_Key,
  transactionID,
  Sales_Date::DATE AS sales_date,
  Sales_ProductID,
  SalespersonID,
  Customer_Key,
  EmployeeID,
  Total_Sold,
  Discount_in_percent,
  Total_Price,
  Product_Name,
  Price_in_dollars,
  Product_Class,
  Category_CategoryID,
  Modify_Date,
  RESITANCE_CATEGORY,
  Is_Allergic,
  Vitality_Days,
  Category_Name
FROM deduplicated_sales
WHERE rn = 1
