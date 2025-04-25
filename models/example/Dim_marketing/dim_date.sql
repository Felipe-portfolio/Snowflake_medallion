{{ config(schema="marketing_data", alias='dim_date', database="MANAGE_DB") }}

-- models/oltp/dim_date.sql
WITH date_data AS (
    SELECT DISTINCT
        c_date
    FROM raw_marketing_data
)
SELECT
    c_date AS date_id,
    EXTRACT(YEAR FROM c_date) AS year,
    EXTRACT(MONTH FROM c_date) AS month,
    EXTRACT(DAY FROM c_date) AS day,
    EXTRACT(DAYOFWEEK FROM c_date) AS day_of_week,
    EXTRACT(QUARTER FROM c_date) AS quarter
FROM date_data
