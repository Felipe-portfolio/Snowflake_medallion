{{ config(
    materialized='table',
    alias='gold_customer_sales_segments',
    schema='GOLD_DBT',
    database='MANAGE_DB'
) }} 


WITH customer_summary AS (
  SELECT
    cs.customer_key,
    CONCAT(sc.First_Name, ' ', sc.Middle_Initial, ' ', sc.Last_Name) AS customer_name,
    sc.CUSTOMER_ADDRESS,
    city.City_Name,
    co.Country_Name,
    city.ZIP_CODE,
    COUNT(DISTINCT cs.sales_order_key) AS total_orders,
    SUM(cs.total_sold) AS total_units,
    SUM(cs.price_in_dollars * cs.total_sold) AS gross_revenue,
    SUM((cs.price_in_dollars - (cs.discount * cs.price_in_dollars)) * cs.total_sold) AS net_revenue,
    MIN(cs.day) AS first_purchase,
    MAX(cs.day) AS last_purchase,
    ROUND(COUNT(DISTINCT cs.sales_order_key) / NULLIF(DATEDIFF('day', MIN(cs.day), MAX(cs.day)) / 7, 0), 2) AS orders_per_week,
    ROUND(SUM(cs.total_sold) / NULLIF(COUNT(DISTINCT cs.sales_order_key), 0), 2) AS avg_items_per_order,
    DATEDIFF('day', MIN(cs.day), MAX(cs.day)) AS lifecycle_days,
    DATEDIFF('day', MAX(cs.day), '2018-03-05') AS days_since_last_purchase

  FROM {{ ref('silver_fact_sales_daily') }} AS cs
  LEFT JOIN {{ ref('silver_dim_customers') }} AS sc ON sc.customerID = cs.customer_key
  LEFT JOIN {{ ref('silver_dim_cities') }} AS city ON sc.cityID = city.cityID
  LEFT JOIN {{ ref('silver_dim_countries') }} AS co ON city.country_code = co.country_code

  GROUP BY 
    cs.customer_key, 
    sc.First_Name, sc.Middle_Initial, sc.Last_Name, 
    sc.CUSTOMER_ADDRESS, 
    city.City_Name, 
    co.Country_Name,
    city.ZIP_CODE
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




