{{ config(schema="marketing_data", alias='gold_campaign_metrics', database="MANAGE_DB") }}
--gold_campaign_metrics.sql
SELECT
    f.date_id AS date,
    cam.campaign_name,
    cat.category,
    f.impressions,
    f.mark_spent,
    f.clicks,
    f.leads,
    f.orders,
    f.revenue,

    -- Metrics
    ROUND((f.revenue - f.mark_spent) / NULLIF(f.mark_spent, 0), 2) AS romi,
    ROUND(f.clicks / NULLIF(f.impressions, 0), 4) AS ctr,
    ROUND(f.leads / NULLIF(f.clicks, 0), 4) AS conversion_rate_1,
    ROUND(f.orders / NULLIF(f.leads, 0), 4) AS conversion_rate_2,
    ROUND(f.revenue / NULLIF(f.orders, 0), 2) AS aov,
    ROUND(f.mark_spent / NULLIF(f.clicks, 0), 2) AS cpc,
    ROUND(f.mark_spent / NULLIF(f.leads, 0), 2) AS cpl,
    ROUND(f.mark_spent / NULLIF(f.orders, 0), 2) AS cac,
    ROUND(f.revenue - f.mark_spent, 2) AS gross_profit

FROM {{ ref('fact_campaign_performance') }} f
LEFT JOIN {{ ref('dim_campaign') }} cam ON f.campaign_id = cam.campaign_id
LEFT JOIN {{ ref('dim_category') }} cat ON f.category_id = cat.category_id
