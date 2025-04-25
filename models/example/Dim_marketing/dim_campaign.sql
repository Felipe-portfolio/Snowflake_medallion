{{ config(schema="marketing_data", alias='dim_campaign', database="MANAGE_DB") }}

-- models/oltp/dim_campaign.sql
WITH campaign_data AS (
    SELECT DISTINCT
        campaign_id,
        campaign_name
    FROM raw_marketing_data
)
SELECT
    campaign_id,
    campaign_name
FROM campaign_data
