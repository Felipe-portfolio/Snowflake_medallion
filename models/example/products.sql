SELECT *
FROM {{ source('manage_db', 'products') }}
