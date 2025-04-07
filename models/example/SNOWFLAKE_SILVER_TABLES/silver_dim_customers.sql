{{ config(
    materialized='table',
    alias='silver_dim_customers',
    schema='SILVER_DBT',
    database='MANAGE_DB'
) }} 

SELECT
  cu.CustomerID,
  cu.First_Name,
  cu.Middle_Initial,
  cu.Last_Name,
  cu.CUSTOMER_ADDRESS,
  city.City_Name,
  co.Country_Name
FROM {{ ref('CUSTOMERS_RAW') }} AS cu
LEFT JOIN {{ ref('CITIES_RAW') }}  city on cu.cityID= city.cityID
LEFT JOIN {{ ref('silver_dim_countries') }} AS co
  ON city.COUNTRYID = co.COUNTRYID
WHERE CustomerID IS NOT NULL