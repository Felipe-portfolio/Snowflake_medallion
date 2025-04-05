{{ config(
    materialized='table',
    alias='EMPLOYEES_RAW',
    schema='RAW_DBT',
    database='MANAGE_DB'
) }} 

SELECT  
EMPLOYEID as EmployeeID,
FIRSTNAME as Employee_First_Name,
MIDDLEINITIAL as Employee_Middle_Initial,
LASTNAME as Employee_Last_Name,
BIRTHDATE as Employee_BirthDate,
GENDER as Employee_Gender,
CITYID as Employee_CityID,
HIREDATE AS Employee_Hire_date,
CURRENT_TIMESTAMP AS ingested_at  
FROM {{ ref('employees') }}  

