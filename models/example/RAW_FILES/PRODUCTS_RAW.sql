{{ config(
    materialized='table',
    alias='PRODUCTS_RAW',
    schema='RAW_DBT',
    database='MANAGE_DB'
) }} 

SELECT  
PRODUCTID as Product_ProductID,
PRODUCTNAME  as Product_Name,
PRICE as Price_in_dollars,
CATEGORYID as Product_CategoryID,
CLASS as Product_Class,
MODIFYDATE as Modify_Date,
RESISTANT as Resitance_category, 
ISALLERGIC as Is_Allergic,
VITALITYDAYS as Vitality_Days,
CURRENT_TIMESTAMP AS ingested_at  
FROM {{ ref('products') }}  