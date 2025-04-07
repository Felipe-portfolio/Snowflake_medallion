{{ config(
    materialized='table',
    alias='silver_dim_categories',
    schema='SILVER_DBT',
    database='MANAGE_DB'
) }} 

-- dim_categories
SELECT DISTINCT
  CategoryID,
  Category_Name
FROM {{ ref('CATEGORIES_RAW') }}
WHERE CategoryID IS NOT NULL