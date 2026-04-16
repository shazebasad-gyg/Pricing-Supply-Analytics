CREATE OR REPLACE TABLE production.supply_analytics.dst_supply_weekly_dataset AS

WITH params AS (
  SELECT 
    DATE '2025-01-01' AS yoy_pre_start_date,
    DATE '2025-01-15' AS yoy_pre_end_date,
    DATE '2025-01-16' AS yoy_event_week_start,
    DATE '2025-01-30' AS yoy_post_end_date,
    DATE '2026-01-01' AS event_pre_start_date,
    DATE '2026-01-15' AS event_pre_end_date,
    DATE '2026-01-16' AS event_week_start_str,
    DATE '2026-01-30' AS event_post_end_date,
    ARRAY('Germany') AS countries,
    ARRAY() AS tour_ids
),

event_data AS (
  SELECT
    dl.country_name,
    h.tour_id,
    h.supplier_id,
    h.date_id,
    date_trunc('week', h.date_id) AS week_start,
    CAST(h.is_online AS INT) AS is_online,
    CASE 
      WHEN h.date_id BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date THEN 'LY'
      WHEN h.date_id BETWEEN params.event_pre_start_date AND params.event_post_end_date THEN 'CY'
    END AS period,
    CASE 
      WHEN h.date_id BETWEEN params.yoy_pre_start_date AND params.yoy_pre_end_date THEN 'Pre'
      WHEN h.date_id BETWEEN params.yoy_event_week_start AND params.yoy_post_end_date THEN 'Post'
      WHEN h.date_id BETWEEN params.event_pre_start_date AND params.event_pre_end_date THEN 'Pre'
      WHEN h.date_id BETWEEN params.event_week_start_str AND params.event_post_end_date THEN 'Post'
    END AS phase,
    CASE
      WHEN h.date_id BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date
        THEN CAST(datediff(date_trunc('week', h.date_id), date_trunc('week', params.yoy_event_week_start)) / 7 AS INT)
      WHEN h.date_id BETWEEN params.event_pre_start_date AND params.event_post_end_date
        THEN CAST(datediff(date_trunc('week', h.date_id), date_trunc('week', params.event_week_start_str)) / 7 AS INT)
    END AS week_index
  FROM production.supply.fact_tour_review_history h
  INNER JOIN dwh.dim_location dl ON h.location_id = dl.location_id
  CROSS JOIN params
  WHERE (
      h.date_id BETWEEN params.event_pre_start_date AND params.event_post_end_date
      OR h.date_id BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date
    )
    AND (SIZE(params.tour_ids) = 0 OR ARRAY_CONTAINS(params.tour_ids, h.tour_id))
    AND (SIZE(params.countries) = 0 OR ARRAY_CONTAINS(params.countries, dl.country_name))
),

tour_week_online AS (
  SELECT
    period, phase, week_index,
    country_name, week_start,
    tour_id,
    SUM(is_online) AS days_online
  FROM event_data
  WHERE period IS NOT NULL AND phase IS NOT NULL
  GROUP BY period, phase, week_index, country_name, week_start, tour_id
),

avg_tour_online AS (
  SELECT
    period, phase, week_index,
    country_name, week_start,
    AVG(days_online) AS avg_days_online_per_tour_per_week
  FROM tour_week_online
  GROUP BY period, phase, week_index, country_name, week_start
),

supplier_day_online AS (
  SELECT
    period, phase, week_index,
    country_name, week_start,
    supplier_id, date_id,
    MAX(is_online) AS is_online_day
  FROM event_data
  WHERE period IS NOT NULL AND phase IS NOT NULL
  GROUP BY period, phase, week_index, country_name, week_start, supplier_id, date_id
),

supplier_week_online AS (
  SELECT
    period, phase, week_index,
    country_name, week_start,
    supplier_id,
    SUM(is_online_day) AS days_online
  FROM supplier_day_online
  GROUP BY period, phase, week_index, country_name, week_start, supplier_id
),

avg_supplier_online AS (
  SELECT
    period, phase, week_index,
    country_name, week_start,
    AVG(days_online) AS avg_days_online_per_supplier_per_week
  FROM supplier_week_online
  GROUP BY period, phase, week_index, country_name, week_start
),

main_agg AS (
  SELECT
    period, phase, week_index,
    country_name, week_start,
    COUNT(DISTINCT tour_id) AS total_tours,
    COUNT(DISTINCT CASE WHEN is_online = 1 THEN tour_id END) AS active_tours,
    CAST(COUNT(DISTINCT CASE WHEN is_online = 1 THEN tour_id END) AS DOUBLE)
      / NULLIF(COUNT(DISTINCT tour_id), 0) AS share_active_tours,
    COUNT(DISTINCT supplier_id) AS total_suppliers,
    COUNT(DISTINCT CASE WHEN is_online = 1 THEN supplier_id END) AS active_suppliers,
    CAST(COUNT(DISTINCT CASE WHEN is_online = 1 THEN supplier_id END) AS DOUBLE)
      / NULLIF(COUNT(DISTINCT supplier_id), 0) AS share_active_suppliers
  FROM event_data
  WHERE period IS NOT NULL AND phase IS NOT NULL
  GROUP BY period, phase, week_index, country_name, week_start
)

SELECT
  m.country_name,
  m.period,
  m.phase,
  m.week_start,
  m.week_index,
  m.total_tours,
  m.active_tours,
  m.share_active_tours,
  m.total_suppliers,
  m.active_suppliers,
  m.share_active_suppliers,
  t.avg_days_online_per_tour_per_week,
  s.avg_days_online_per_supplier_per_week
FROM main_agg m
LEFT JOIN avg_tour_online t 
  ON m.period = t.period AND m.phase = t.phase AND m.week_index = t.week_index
  AND m.country_name = t.country_name AND m.week_start = t.week_start
LEFT JOIN avg_supplier_online s 
  ON m.period = s.period AND m.phase = s.phase AND m.week_index = s.week_index
  AND m.country_name = s.country_name AND m.week_start = s.week_start
ORDER BY m.country_name, m.period, m.week_index;
