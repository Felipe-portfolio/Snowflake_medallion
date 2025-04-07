{{ config(
    materialized='table',
    alias='silver_dim_cities',
    schema='SILVER_DBT',
    database='MANAGE_DB'
) }} 

SELECT 
  c.CityID, 
  c.City_Name, 
  c.Zip_Code, 
  co.Country_Name, 
  co.Country_Code
FROM {{ ref('CITIES_RAW') }} AS c
LEFT JOIN {{ ref('silver_dim_countries') }} AS co
  ON c.COUNTRYID = co.COUNTRYID
