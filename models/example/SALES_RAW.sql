{{ config(
    materialized='table',
    alias='final_orders_table',
    schema='RAW_DBT',
    database='MANAGE_DB'
) }} 

SELECT  
  SALESID AS order_key,
  SALESPERSONID AS salespersonID,  
  CUSTOMERID AS customer_key,  
  PRODUCTID AS order_date,
  QUANTITY AS total_sold,
  DISCOUNT as discount_in_percent,
  TOTALPRICE as total_price,
  SALESDATE as sales_date,
  TRANSACTIONNUMBER  as transactionID, 
  CURRENT_TIMESTAMP AS ingested_at  
FROM {{ ref('sales') }}  
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_key ORDER BY ingested_at DESC) = 1

