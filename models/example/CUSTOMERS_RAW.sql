{{ config(
    materialized='table',
    alias='CUSTOMERS_RAW',
    schema='RAW_DBT',
    database='MANAGE_DB'
) }} 

SELECT  
CUSTOMERID as CustomerID, 
firstname as first_name, 
middleinitial as middle_initial,
lastname as last_name,
cityid as cityID,
address as Customer_Address,
CURRENT_TIMESTAMP AS ingested_at  
FROM {{ ref('customers') }}  