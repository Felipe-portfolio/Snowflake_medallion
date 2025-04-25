{{ config(schema="marketing_data", alias='fact_campaign_performance', database="MANAGE_DB") }}

-- models/oltp/fact_campaign_performance.sql
-- models/oltp/fact_campaign_performance.sql
WITH fact_data AS (
    SELECT
        c.c_date,
        c.campaign_name,
        c.category,
        c.campaign_id,
        c.impressions,
        c.mark_spent,
        c.clicks,
        c.leads,
        c.orders,
        c.revenue,
        d.date_id,
        ca.category_id
    FROM raw_marketing_data c
    LEFT JOIN {{ ref('dim_date') }} d
        ON c.c_date = d.date_id
    LEFT JOIN {{ ref('dim_campaign') }} cam
        ON c.campaign_id = cam.campaign_id
    LEFT JOIN {{ ref('dim_category') }} ca
        ON c.category = ca.category
)
SELECT
    date_id,
    campaign_id,
    category_id,
    impressions,
    mark_spent,
    clicks,
    leads,
    orders,
    revenue
FROM fact_data
