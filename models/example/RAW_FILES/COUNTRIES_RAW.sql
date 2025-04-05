{{ config(
    materialized='table',
    alias='COUNTRIES_RAW',
    schema='RAW_DBT',
    database='MANAGE_DB'
) }} 

SELECT  
CountryID as CountryID,
CountryName	as Country_Name,
CountryCode	as Country_Code,
CURRENT_TIMESTAMP AS ingested_at  
FROM {{ ref('countries') }}  