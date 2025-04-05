{{ config(
    materialized='table',
    alias='CATEGORIES_RAW',
    schema='RAW_DBT',
    database='MANAGE_DB'
) }} 

SELECT  
CATEGORYID as CategoryID,
CATEGORYNAME as Category_Name,
CURRENT_TIMESTAMP AS ingested_at  
FROM {{ ref('categories') }}  