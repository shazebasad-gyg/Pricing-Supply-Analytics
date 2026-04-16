CREATE OR REPLACE TABLE production.supply_analytics.dst_price_change_weekly AS

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

tour_level_prices AS (
  SELECT 
    p.tour_id,
    dt.user_id AS supplier_id,
    dl.country_name,
    date_trunc('week', p.snapshot_date) AS week_start,
    CASE 
      WHEN p.snapshot_date BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date THEN 'LY'
      WHEN p.snapshot_date BETWEEN params.event_pre_start_date AND params.event_post_end_date THEN 'CY'
    END AS period,
    CASE 
      WHEN p.snapshot_date BETWEEN params.yoy_pre_start_date AND params.yoy_pre_end_date THEN 'Pre'
      WHEN p.snapshot_date BETWEEN params.yoy_event_week_start AND params.yoy_post_end_date THEN 'Post'
      WHEN p.snapshot_date BETWEEN params.event_pre_start_date AND params.event_pre_end_date THEN 'Pre'
      WHEN p.snapshot_date BETWEEN params.event_week_start_str AND params.event_post_end_date THEN 'Post'
    END AS phase,
    CASE
      WHEN p.snapshot_date BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date
        THEN CAST(datediff(date_trunc('week', p.snapshot_date), date_trunc('week', params.yoy_event_week_start)) / 7 AS INT)
      WHEN p.snapshot_date BETWEEN params.event_pre_start_date AND params.event_post_end_date
        THEN CAST(datediff(date_trunc('week', p.snapshot_date), date_trunc('week', params.event_week_start_str)) / 7 AS INT)
    END AS week_index,
    CASE 
      WHEN p.snapshot_date BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date
        THEN params.yoy_event_week_start
      ELSE params.event_week_start_str
    END AS event_start,
    p.travel_date,
    AVG(p.tour_from_price_red_per_adult) AS tour_from_price_red_per_adult,
    AVG(p.tour_from_price_black_per_adult) AS tour_from_price_black_per_adult,
    AVG(p.final_price_red_per_adult) AS final_price_red_per_adult,
    AVG(p.final_price_black_per_adult) AS final_price_black_per_adult,
    AVG(COALESCE(SIZE(p.availability_timeslots), 0)) AS timeslot_count
  FROM production.dwh.daily_tour_price_snapshot p
  INNER JOIN production.dwh.dim_tour dt ON p.tour_id = dt.tour_id
  INNER JOIN production.dwh.dim_location dl ON dt.location_id = dl.location_id
  CROSS JOIN params
  WHERE (
    (p.snapshot_date BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date
     AND p.travel_date BETWEEN params.yoy_event_week_start AND ADD_MONTHS(params.yoy_event_week_start, 12))
    OR
    (p.snapshot_date BETWEEN params.event_pre_start_date AND params.event_post_end_date
     AND p.travel_date BETWEEN params.event_week_start_str AND ADD_MONTHS(params.event_week_start_str, 12))
  )
    AND (SIZE(params.countries) = 0 OR ARRAY_CONTAINS(params.countries, dl.country_name))
    AND (SIZE(params.tour_ids) = 0 OR ARRAY_CONTAINS(params.tour_ids, dt.tour_id))
  GROUP BY 
    p.tour_id, dt.user_id, dl.country_name,
    week_start, period, phase, week_index, event_start,
    p.travel_date
)

SELECT
  country_name,
  period,
  phase,
  week_start,
  week_index,

  COUNT(DISTINCT supplier_id) AS total_suppliers,
  COUNT(DISTINCT tour_id) AS total_tours,

  PERCENTILE_APPROX(tour_from_price_red_per_adult, 0.5) AS median_from_red_price,
  STDDEV(tour_from_price_red_per_adult) AS stddev_from_red_price,
  PERCENTILE_APPROX(tour_from_price_black_per_adult, 0.5) AS median_from_black_price,
  STDDEV(tour_from_price_black_per_adult) AS stddev_from_black_price,

  PERCENTILE_APPROX(CASE WHEN travel_date BETWEEN event_start AND ADD_MONTHS(event_start, 3) 
           THEN final_price_red_per_adult END, 0.5) AS median_final_red_3m,
  STDDEV(CASE WHEN travel_date BETWEEN event_start AND ADD_MONTHS(event_start, 3) 
              THEN final_price_red_per_adult END) AS stddev_final_red_3m,
  PERCENTILE_APPROX(CASE WHEN travel_date BETWEEN event_start AND ADD_MONTHS(event_start, 6) 
           THEN final_price_red_per_adult END, 0.5) AS median_final_red_6m,
  STDDEV(CASE WHEN travel_date BETWEEN event_start AND ADD_MONTHS(event_start, 6) 
              THEN final_price_red_per_adult END) AS stddev_final_red_6m,
  PERCENTILE_APPROX(CASE WHEN travel_date BETWEEN event_start AND ADD_MONTHS(event_start, 12) 
           THEN final_price_red_per_adult END, 0.5) AS median_final_red_12m,
  STDDEV(CASE WHEN travel_date BETWEEN event_start AND ADD_MONTHS(event_start, 12) 
              THEN final_price_red_per_adult END) AS stddev_final_red_12m,

  PERCENTILE_APPROX(CASE WHEN travel_date BETWEEN event_start AND ADD_MONTHS(event_start, 3) 
           THEN final_price_black_per_adult END, 0.5) AS median_final_black_3m,
  PERCENTILE_APPROX(CASE WHEN travel_date BETWEEN event_start AND ADD_MONTHS(event_start, 6) 
           THEN final_price_black_per_adult END, 0.5) AS median_final_black_6m,
  PERCENTILE_APPROX(CASE WHEN travel_date BETWEEN event_start AND ADD_MONTHS(event_start, 12) 
           THEN final_price_black_per_adult END, 0.5) AS median_final_black_12m,

  PERCENTILE_APPROX(CASE WHEN travel_date BETWEEN event_start AND ADD_MONTHS(event_start, 3) 
           THEN timeslot_count END, 0.5) AS median_timeslots_3m,
  PERCENTILE_APPROX(CASE WHEN travel_date BETWEEN event_start AND ADD_MONTHS(event_start, 6) 
           THEN timeslot_count END, 0.5) AS median_timeslots_6m,
  PERCENTILE_APPROX(CASE WHEN travel_date BETWEEN event_start AND ADD_MONTHS(event_start, 12) 
           THEN timeslot_count END, 0.5) AS median_timeslots_12m

FROM tour_level_prices
WHERE period IS NOT NULL AND phase IS NOT NULL
GROUP BY country_name, period, phase, week_start, week_index
ORDER BY country_name, period, week_index;
