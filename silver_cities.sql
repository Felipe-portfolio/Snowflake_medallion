
{{ config(
    materialized='table',
    alias='silver_dim_cities',
    schema='SILVER_DBT',
    database='MANAGE_DB'
) }} 

SELECT c.CityID, c.City_Name, c.Zip_code, co.Country_Name, co.Country_Code
FROM {{ ref CITIES_RAW }} c
JOIN {{ref silver_dim_countries }} co USING (CountryID);
FROM {{ ref('COUNTRIES_RAW') }}