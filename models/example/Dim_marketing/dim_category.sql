{{ config(schema="marketing_data", alias='dim_category', database="MANAGE_DB") }}

-- models/oltp/dim_category.sql
WITH category_data AS (
    SELECT DISTINCT
        category
    FROM raw_marketing_data
)
SELECT
    ROW_NUMBER() OVER (ORDER BY category) AS category_id,  -- Generate unique ID for each category
    category
FROM category_data
