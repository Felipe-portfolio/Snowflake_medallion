-- models/my_first_model.sql

SELECT
    id,
    created_at,
    total_amount
FROM {{ ref('orders') }}

