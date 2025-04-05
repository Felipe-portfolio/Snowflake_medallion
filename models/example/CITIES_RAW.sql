{{ config(
    materialized='table',
    alias='CITIES_RAW',
    schema='RAW_DBT',
    database='MANAGE_DB'
) }} 

SELECT  
CITYID AS CityID,
CITYNAME as City_Name,
ZIPCODE as ZIP_Code,
COUNTRYID as CountryID, 
CURRENT_TIMESTAMP AS ingested_at  
FROM {{ ref('cities') }}  