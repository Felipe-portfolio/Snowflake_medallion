{{ config(
    materialized='table',
    alias='silver_dim_countries',
    schema='SILVER_DBT',
    database='MANAGE_DB'
) }} 

-- dim_categories
SELECT DISTINCT CountryID, Country_Name, Country_Code
FROM {{ ref('COUNTRIES_RAW') }}
