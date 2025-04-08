{{ config(
    materialized='table',
    alias='gold_customer_sales_segments',
    schema='GOLD_DBT',
    database='MANAGE_DB'
) }} 


WITH customer_summary AS (
  SELECT
    customer_key,
    COUNT(DISTINCT sales_order_key) AS total_orders,
    SUM(total_sold) AS total_units,
    SUM(price_in_dollars * total_sold) AS gross_revenue,
    SUM((price_in_dollars - (discount * price_in_dollars)) * total_sold) AS net_revenue,
    MIN(day) AS first_purchase,
    MAX(day) AS last_purchase,
    ROUND(total_orders / (DATEDIFF('day', first_purchase, last_purchase) / 7), 2) AS orders_per_week,
    ROUND(SUM(total_sold) / NULLIF(total_orders, 0), 2) AS avg_items_per_order,
    DATEDIFF('day', MIN(day), MAX(day)) AS lifecycle_days,
    DATEDIFF('day', MAX(day), '2018-03-05') AS days_since_last_purchase
  FROM {{ ref('silver_fact_sales_daily') }} as cs


  GROUP BY customer_key
),
percentiles AS (
  SELECT 
    APPROX_PERCENTILE(gross_revenue, 0.25) AS p25,
    APPROX_PERCENTILE(gross_revenue, 0.5) AS p50,
    APPROX_PERCENTILE(gross_revenue, 0.75) AS p75
  FROM customer_summary
)

SELECT 
  cs.*,

-- 🚀 New Metrics
  ROUND(net_revenue / NULLIF(total_orders, 0), 2) AS avg_order_value,
  ROUND(gross_revenue / NULLIF(total_orders, 0), 2) AS gross_order_value,
  ROUND(lifecycle_days / NULLIF(total_orders - 1, 0), 2) AS avg_days_between_orders,
  ROUND(total_orders / NULLIF(lifecycle_days, 0), 2) AS order_frequency,
  ROUND(net_revenue / NULLIF(lifecycle_days, 0), 2) AS revenue_per_day_active,
  ROUND(avg_order_value * orders_per_week * 6, 2) AS estimated_monthly_ltv,
  DATEDIFF('day', cs.first_purchase, '2018-03-05') AS customer_age_days,

  -- 🎯 Spending Tier
  CASE 
    WHEN cs.gross_revenue >= p.p75 THEN 'High Spender'
    WHEN cs.gross_revenue >= p.p50 THEN 'Mid Spender'
    WHEN cs.gross_revenue >= p.p25 THEN 'Low Spender'
    ELSE 'Minimal Spender'
  END AS spending_segment,

  -- 🧠 Behavior Segment
  CASE 
    WHEN total_orders >= 40 AND days_since_last_purchase > 0 and days_since_last_purchase  <= 3 THEN 'Loyal'
    WHEN days_since_last_purchase > 3 AND days_since_last_purchase < 7  THEN 'Frequent Buyer'
    WHEN days_since_last_purchase > 7 THEN 'Inactive'
    ELSE 'Regular'
  END AS behavior_segment,
CASE 
  WHEN days_since_last_purchase <= 2 THEN 'Low Risk'
  WHEN days_since_last_purchase <= 5 THEN 'Medium Risk'
  ELSE 'High Risk'
END AS churn_risk,

  CASE 
    WHEN days_since_last_purchase <= 1 THEN 'Recent'
    WHEN days_since_last_purchase <= 3 THEN 'Within last Week'
    WHEN days_since_last_purchase >= 7 THEN 'Last 1 Week'
    ELSE 'Old'
  END AS recency_bucket,
  -- RFM bucket score (3 = best)
CASE 
  WHEN days_since_last_purchase <= 1 THEN 3
  WHEN days_since_last_purchase <= 3 THEN 1
  ELSE 0.5
END AS recency_score,

CASE 
  WHEN total_orders >= 40 THEN 3
  WHEN total_orders >= 38 THEN 1
  ELSE 0.5
END AS frequency_score,

CASE 
  WHEN gross_revenue >= p.p75 THEN 3
  WHEN gross_revenue >= p.p50 THEN 1
  ELSE 0.5
END AS monetary_score,

  CASE 
   WHEN first_purchase <= '2018-01-03' THEN 'Early Adopter'
   WHEN first_purchase BETWEEN '2018-01-04' AND '2018-01-21' THEN 'Mid Cycle'
   ELSE 'Late Joiner'
  END AS cohort_entry_segment

FROM customer_summary cs, percentiles p




