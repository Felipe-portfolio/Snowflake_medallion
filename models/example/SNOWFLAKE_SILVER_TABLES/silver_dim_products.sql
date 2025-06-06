{{ config(
    materialized='table',
    alias='silver_dim_products',
    schema='SILVER_DBT',
    database='MANAGE_DB'
) }} 


SELECT
  p.Product_ProductID,
  p.Product_Name,
  p.Price_in_dollars,
  p.Product_Class,
  p.Product_CategoryID,
  p.Modify_Date,
  p.RESITANCE_CATEGORY,
  p.Is_Allergic,
  p.Vitality_Days,
  ca.Category_Name
FROM {{ ref ('PRODUCTS_RAW') }} as p
 LEFT JOIN {{ ref('CATEGORIES_RAW') }} as ca on ca.Category_categoryID=p.Product_categoryID


