{{ config(
    materialized='table',
    alias='silver_dim_employees',
    schema='SILVER_DBT',
    database='MANAGE_DB'
) }} 

SELECT
  e.EmployeeID,
  e.Employee_First_Name,
  e.Employee_Middle_Initial,
  e.Employee_Last_Name,
  e.Employee_BirthDate,
  e.Employee_Gender,
  city.City_Name,
  co.Country_Name,
  e.Employee_Hire_Date
FROM {{ ref('EMPLOYEES_RAW') }} as e
LEFT JOIN {{ ref('CITIES_RAW') }} city on e.Employee_cityID=city.CityID
LEFT JOIN {{ ref('silver_dim_countries') }} AS co
  ON city.COUNTRYID = co.COUNTRYID