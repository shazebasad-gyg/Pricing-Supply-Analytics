WITH v_inputs AS (
  SELECT
    CAST(:comm_date          AS DATE)   AS comm_date,
    CAST(:rollout_date       AS DATE)   AS rollout_date,
    CAST(:country            AS STRING) AS country,
    CAST(:tour_ids           AS STRING) AS tour_ids,
    CAST(:supplier_ids       AS STRING) AS supplier_ids,
    CAST(:comm_weeks_before   AS INT)    AS comm_weeks_before,
    CAST(:comm_weeks_after    AS INT)    AS comm_weeks_after,
    CAST(:rollout_weeks_before  AS INT)    AS rollout_weeks_before,
    CAST(:rollout_weeks_after   AS INT)    AS rollout_weeks_after
),

tour_filter AS (
  SELECT CAST(trim(value) AS BIGINT) AS tour_id
  FROM v_inputs
  LATERAL VIEW explode(split(tour_ids, ',')) AS value
  WHERE tour_ids != '' AND trim(value) != ''
),

supplier_filter AS (
  SELECT CAST(trim(value) AS BIGINT) AS supplier_id
  FROM v_inputs
  LATERAL VIEW explode(split(supplier_ids, ',')) AS value
  WHERE supplier_ids != '' AND trim(value) != ''
),

filter_flags AS (
  SELECT 
    CASE WHEN COALESCE(tour_ids, '') = '' THEN 0 ELSE 1 END AS should_filter_tours,
    CASE WHEN COALESCE(supplier_ids, '') = '' THEN 0 ELSE 1 END AS should_filter_suppliers
  FROM v_inputs
),

date_ranges AS (
  SELECT
    date_trunc('week', pr.comm_date) AS cy_comm_week_start,
    date_trunc('week', pr.rollout_date) AS cy_rollout_week_start,
    date_trunc('week', add_months(pr.comm_date, -12)) AS ly_comm_week_start,
    date_trunc('week', add_months(pr.rollout_date, -12)) AS ly_rollout_week_start,

    pr.country,
    pr.comm_weeks_before,
    pr.comm_weeks_after,
    pr.rollout_weeks_before,
    pr.rollout_weeks_after,
    
    -- CY snapshot windows
    date_add(date_trunc('week', pr.comm_date), -7 * pr.comm_weeks_before) AS cy_pre_comm_start,
    date_add(date_trunc('week', pr.comm_date), -1) AS cy_pre_comm_end,
    
    date_trunc('week', pr.comm_date) AS cy_post_comm_start,
    date_add(date_trunc('week', pr.comm_date), 7 * pr.comm_weeks_after - 1) AS cy_post_comm_end,
    
    date_add(date_trunc('week', pr.rollout_date), -7 * pr.rollout_weeks_before) AS cy_pre_rollout_start,
    date_add(date_trunc('week', pr.rollout_date), -1) AS cy_pre_rollout_end,
    
    date_trunc('week', pr.rollout_date) AS cy_post_rollout_start,
    date_add(date_trunc('week', pr.rollout_date), 7 * pr.rollout_weeks_after - 1) AS cy_post_rollout_end,
    
    -- LY snapshot windows
    date_add(date_trunc('week', add_months(pr.comm_date, -12)), -7 * pr.comm_weeks_before) AS ly_pre_comm_start,
    date_add(date_trunc('week', add_months(pr.comm_date, -12)), -1) AS ly_pre_comm_end,
    
    date_trunc('week', add_months(pr.comm_date, -12)) AS ly_post_comm_start,
    date_add(date_trunc('week', add_months(pr.comm_date, -12)), 7 * pr.comm_weeks_after - 1) AS ly_post_comm_end,
    
    date_add(date_trunc('week', add_months(pr.rollout_date, -12)), -7 * pr.rollout_weeks_before) AS ly_pre_rollout_start,
    date_add(date_trunc('week', add_months(pr.rollout_date, -12)), -1) AS ly_pre_rollout_end,
    
    date_trunc('week', add_months(pr.rollout_date, -12)) AS ly_post_rollout_start,
    date_add(date_trunc('week', add_months(pr.rollout_date, -12)), 7 * pr.rollout_weeks_after - 1) AS ly_post_rollout_end,
    
    -- CY travel date ranges from comm week start
    date_trunc('week', pr.comm_date) AS cy_comm_travel_anchor,
    add_months(date_trunc('week', pr.comm_date), 3) AS cy_comm_travel_3m_end,
    add_months(date_trunc('week', pr.comm_date), 6) AS cy_comm_travel_6m_end,
    add_months(date_trunc('week', pr.comm_date), 12) AS cy_comm_travel_12m_end,
    
    -- CY travel date ranges from rollout week start
    date_trunc('week', pr.rollout_date) AS cy_rollout_travel_anchor,
    add_months(date_trunc('week', pr.rollout_date), 3) AS cy_rollout_travel_3m_end,
    add_months(date_trunc('week', pr.rollout_date), 6) AS cy_rollout_travel_6m_end,
    add_months(date_trunc('week', pr.rollout_date), 12) AS cy_rollout_travel_12m_end,
    
    -- LY travel date ranges from comm week start
    date_trunc('week', add_months(pr.comm_date, -12)) AS ly_comm_travel_anchor,
    add_months(date_trunc('week', add_months(pr.comm_date, -12)), 3) AS ly_comm_travel_3m_end,
    add_months(date_trunc('week', add_months(pr.comm_date, -12)), 6) AS ly_comm_travel_6m_end,
    add_months(date_trunc('week', add_months(pr.comm_date, -12)), 12) AS ly_comm_travel_12m_end,
    
    -- LY travel date ranges from rollout week start
    date_trunc('week', add_months(pr.rollout_date, -12)) AS ly_rollout_travel_anchor,
    add_months(date_trunc('week', add_months(pr.rollout_date, -12)), 3) AS ly_rollout_travel_3m_end,
    add_months(date_trunc('week', add_months(pr.rollout_date, -12)), 6) AS ly_rollout_travel_6m_end,
    add_months(date_trunc('week', add_months(pr.rollout_date, -12)), 12) AS ly_rollout_travel_12m_end
    
  FROM v_inputs pr
),

price_base AS (
  SELECT
    CAST(p.snapshot_date AS date) AS snapshot_dt,
    CAST(p.travel_date   AS date) AS travel_dt,

    p.currency_iso_code,
    p.tour_id,
    p.tour_option_id,

    p.final_price_red_per_adult,
    p.final_price_black_per_adult,
    p.availability_timeslots,

    dl.country_name,
    
    dr.cy_pre_comm_start,
    dr.cy_pre_comm_end,
    dr.cy_post_comm_start,
    dr.cy_post_comm_end,
    dr.cy_pre_rollout_start,
    dr.cy_pre_rollout_end,
    dr.cy_post_rollout_start,
    dr.cy_post_rollout_end,
    dr.ly_pre_comm_start,
    dr.ly_pre_comm_end,
    dr.ly_post_comm_start,
    dr.ly_post_comm_end,
    dr.ly_pre_rollout_start,
    dr.ly_pre_rollout_end,
    dr.ly_post_rollout_start,
    dr.ly_post_rollout_end,
    
    dr.cy_comm_travel_anchor,
    dr.cy_comm_travel_3m_end,
    dr.cy_comm_travel_6m_end,
    dr.cy_comm_travel_12m_end,
    dr.cy_rollout_travel_anchor,
    dr.cy_rollout_travel_3m_end,
    dr.cy_rollout_travel_6m_end,
    dr.cy_rollout_travel_12m_end,
    
    dr.ly_comm_travel_anchor,
    dr.ly_comm_travel_3m_end,
    dr.ly_comm_travel_6m_end,
    dr.ly_comm_travel_12m_end,
    dr.ly_rollout_travel_anchor,
    dr.ly_rollout_travel_3m_end,
    dr.ly_rollout_travel_6m_end,
    dr.ly_rollout_travel_12m_end,
    
    CASE
      WHEN CAST(p.snapshot_date AS date) >= dr.cy_pre_comm_start 
           AND CAST(p.snapshot_date AS date) <= dr.cy_post_rollout_end 
        THEN 'CY'
      WHEN CAST(p.snapshot_date AS date) >= dr.ly_pre_comm_start 
           AND CAST(p.snapshot_date AS date) <= dr.ly_post_rollout_end 
        THEN 'LY'
    END AS period
    
  FROM production.dwh.daily_tour_price_snapshot p
  INNER JOIN production.dwh.dim_tour dt
    ON p.tour_id = dt.tour_id
  INNER JOIN production.dwh.dim_location dl
    ON dt.location_id = dl.location_id
  CROSS JOIN date_ranges dr
  CROSS JOIN filter_flags ff
  LEFT JOIN tour_filter tf
    ON p.tour_id = tf.tour_id
  LEFT JOIN supplier_filter sf
    ON dt.supplier_id = sf.supplier_id
  WHERE
    LOWER(dl.country_name) = LOWER(dr.country)
    AND (ff.should_filter_tours = 0 OR tf.tour_id IS NOT NULL)
    AND (ff.should_filter_suppliers = 0 OR sf.supplier_id IS NOT NULL)
    AND (
      (CAST(p.snapshot_date AS date) >= dr.cy_pre_comm_start AND CAST(p.snapshot_date AS date) <= dr.cy_post_rollout_end)
      OR
      (CAST(p.snapshot_date AS date) >= dr.ly_pre_comm_start AND CAST(p.snapshot_date AS date) <= dr.ly_post_rollout_end)
    )
),

windowed_daily AS (
  -- PRE_COMM with travel dates anchored to comm week
  SELECT
    'PRE_COMM' AS window,
    period,
    country_name,
    snapshot_dt,
    travel_dt,
    CAST(date_trunc('week', snapshot_dt) AS date) AS snapshot_week_start,
    currency_iso_code,
    tour_id,
    tour_option_id,
    
    CASE
      WHEN travel_dt >= cy_comm_travel_anchor AND travel_dt < cy_comm_travel_3m_end THEN '3M'
      WHEN travel_dt >= cy_comm_travel_3m_end AND travel_dt < cy_comm_travel_6m_end THEN '6M'
      WHEN travel_dt >= cy_comm_travel_6m_end AND travel_dt <= cy_comm_travel_12m_end THEN '12M'
      WHEN travel_dt >= ly_comm_travel_anchor AND travel_dt < ly_comm_travel_3m_end THEN '3M'
      WHEN travel_dt >= ly_comm_travel_3m_end AND travel_dt < ly_comm_travel_6m_end THEN '6M'
      WHEN travel_dt >= ly_comm_travel_6m_end AND travel_dt <= ly_comm_travel_12m_end THEN '12M'
    END AS travel_period,
    
    final_price_red_per_adult,
    final_price_black_per_adult,
    
    CASE
      WHEN availability_timeslots IS NULL THEN 0
      ELSE size(availability_timeslots)
    END AS timeslot_count
    
  FROM price_base
  WHERE (period = 'CY' AND snapshot_dt >= cy_pre_comm_start AND snapshot_dt <= cy_pre_comm_end)
     OR (period = 'LY' AND snapshot_dt >= ly_pre_comm_start AND snapshot_dt <= ly_pre_comm_end)
  
  UNION ALL
  
  -- POST_COMM with travel dates anchored to comm week
  SELECT
    'POST_COMM' AS window,
    period,
    country_name,
    snapshot_dt,
    travel_dt,
    CAST(date_trunc('week', snapshot_dt) AS date) AS snapshot_week_start,
    currency_iso_code,
    tour_id,
    tour_option_id,
    
    CASE
      WHEN travel_dt >= cy_comm_travel_anchor AND travel_dt < cy_comm_travel_3m_end THEN '3M'
      WHEN travel_dt >= cy_comm_travel_3m_end AND travel_dt < cy_comm_travel_6m_end THEN '6M'
      WHEN travel_dt >= cy_comm_travel_6m_end AND travel_dt <= cy_comm_travel_12m_end THEN '12M'
      WHEN travel_dt >= ly_comm_travel_anchor AND travel_dt < ly_comm_travel_3m_end THEN '3M'
      WHEN travel_dt >= ly_comm_travel_3m_end AND travel_dt < ly_comm_travel_6m_end THEN '6M'
      WHEN travel_dt >= ly_comm_travel_6m_end AND travel_dt <= ly_comm_travel_12m_end THEN '12M'
    END AS travel_period,
    
    final_price_red_per_adult,
    final_price_black_per_adult,
    
    CASE
      WHEN availability_timeslots IS NULL THEN 0
      ELSE size(availability_timeslots)
    END AS timeslot_count
    
  FROM price_base
  WHERE (period = 'CY' AND snapshot_dt >= cy_post_comm_start AND snapshot_dt <= cy_post_comm_end)
     OR (period = 'LY' AND snapshot_dt >= ly_post_comm_start AND snapshot_dt <= ly_post_comm_end)
  
  UNION ALL
  
  -- PRE_ROLLOUT with travel dates anchored to rollout week
  SELECT
    'PRE_ROLLOUT' AS window,
    period,
    country_name,
    snapshot_dt,
    travel_dt,
    CAST(date_trunc('week', snapshot_dt) AS date) AS snapshot_week_start,
    currency_iso_code,
    tour_id,
    tour_option_id,
    
    CASE
      WHEN travel_dt >= cy_rollout_travel_anchor AND travel_dt < cy_rollout_travel_3m_end THEN '3M'
      WHEN travel_dt >= cy_rollout_travel_3m_end AND travel_dt < cy_rollout_travel_6m_end THEN '6M'
      WHEN travel_dt >= cy_rollout_travel_6m_end AND travel_dt <= cy_rollout_travel_12m_end THEN '12M'
      WHEN travel_dt >= ly_rollout_travel_anchor AND travel_dt < ly_rollout_travel_3m_end THEN '3M'
      WHEN travel_dt >= ly_rollout_travel_3m_end AND travel_dt < ly_rollout_travel_6m_end THEN '6M'
      WHEN travel_dt >= ly_rollout_travel_6m_end AND travel_dt <= ly_rollout_travel_12m_end THEN '12M'
    END AS travel_period,
    
    final_price_red_per_adult,
    final_price_black_per_adult,
    
    CASE
      WHEN availability_timeslots IS NULL THEN 0
      ELSE size(availability_timeslots)
    END AS timeslot_count
    
  FROM price_base
  WHERE (period = 'CY' AND snapshot_dt >= cy_pre_rollout_start AND snapshot_dt <= cy_pre_rollout_end)
     OR (period = 'LY' AND snapshot_dt >= ly_pre_rollout_start AND snapshot_dt <= ly_pre_rollout_end)
  
  UNION ALL
  
  -- POST_ROLLOUT with travel dates anchored to rollout week
  SELECT
    'POST_ROLLOUT' AS window,
    period,
    country_name,
    snapshot_dt,
    travel_dt,
    CAST(date_trunc('week', snapshot_dt) AS date) AS snapshot_week_start,
    currency_iso_code,
    tour_id,
    tour_option_id,
    
    CASE
      WHEN travel_dt >= cy_rollout_travel_anchor AND travel_dt < cy_rollout_travel_3m_end THEN '3M'
      WHEN travel_dt >= cy_rollout_travel_3m_end AND travel_dt < cy_rollout_travel_6m_end THEN '6M'
      WHEN travel_dt >= cy_rollout_travel_6m_end AND travel_dt <= cy_rollout_travel_12m_end THEN '12M'
      WHEN travel_dt >= ly_rollout_travel_anchor AND travel_dt < ly_rollout_travel_3m_end THEN '3M'
      WHEN travel_dt >= ly_rollout_travel_3m_end AND travel_dt < ly_rollout_travel_6m_end THEN '6M'
      WHEN travel_dt >= ly_rollout_travel_6m_end AND travel_dt <= ly_rollout_travel_12m_end THEN '12M'
    END AS travel_period,
    
    final_price_red_per_adult,
    final_price_black_per_adult,
    
    CASE
      WHEN availability_timeslots IS NULL THEN 0
      ELSE size(availability_timeslots)
    END AS timeslot_count
    
  FROM price_base
  WHERE (period = 'CY' AND snapshot_dt >= cy_post_rollout_start AND snapshot_dt <= cy_post_rollout_end)
     OR (period = 'LY' AND snapshot_dt >= ly_post_rollout_start AND snapshot_dt <= ly_post_rollout_end)
),

weekly_metrics AS (
  SELECT
    period,
    country_name,
    window,
    snapshot_week_start,
    currency_iso_code,
    travel_period,

    COUNT(*) AS wk_rows,
    COUNT(DISTINCT tour_id) AS wk_tours,
    COUNT(DISTINCT tour_option_id) AS wk_tour_options,

    percentile_approx(final_price_red_per_adult, 0.5)   AS wk_median_red_price_per_adult,
    percentile_approx(final_price_black_per_adult, 0.5) AS wk_median_black_price_per_adult,
    percentile_approx(timeslot_count, 0.5)              AS wk_median_timeslots
  FROM windowed_daily
  WHERE period IS NOT NULL
    AND window IS NOT NULL
    AND travel_period IS NOT NULL
    AND final_price_red_per_adult IS NOT NULL
    AND final_price_red_per_adult > 0
  GROUP BY 1,2,3,4,5,6
),

window_level AS (
  SELECT
    period,
    country_name,
    window,
    currency_iso_code,
    travel_period,

    COUNT(DISTINCT snapshot_week_start) AS weeks_in_window,

    AVG(wk_rows)         AS avg_wk_rows,
    AVG(wk_tours)        AS avg_wk_tours,
    AVG(wk_tour_options) AS avg_wk_tour_options,

    percentile_approx(wk_median_red_price_per_adult, 0.5)   AS window_median_red_price_per_adult,
    percentile_approx(wk_median_black_price_per_adult, 0.5) AS window_median_black_price_per_adult,
    percentile_approx(wk_median_timeslots, 0.5)             AS window_median_timeslots
  FROM weekly_metrics
  GROUP BY 1,2,3,4,5
),

pivoted AS (
  SELECT
    period,
    country_name,
    window,
    currency_iso_code,

    MAX(CASE WHEN travel_period = '3M'  THEN weeks_in_window END) AS weeks_3m,
    MAX(CASE WHEN travel_period = '6M'  THEN weeks_in_window END) AS weeks_6m,
    MAX(CASE WHEN travel_period = '12M' THEN weeks_in_window END) AS weeks_12m,

    MAX(CASE WHEN travel_period = '3M'  THEN window_median_red_price_per_adult END)   AS red_med_3m,
    MAX(CASE WHEN travel_period = '6M'  THEN window_median_red_price_per_adult END)   AS red_med_6m,
    MAX(CASE WHEN travel_period = '12M' THEN window_median_red_price_per_adult END)   AS red_med_12m,

    MAX(CASE WHEN travel_period = '3M'  THEN window_median_black_price_per_adult END)  AS black_med_3m,
    MAX(CASE WHEN travel_period = '6M'  THEN window_median_black_price_per_adult END)  AS black_med_6m,
    MAX(CASE WHEN travel_period = '12M' THEN window_median_black_price_per_adult END)  AS black_med_12m,

    MAX(CASE WHEN travel_period = '3M'  THEN window_median_timeslots END) AS timeslots_med_3m,
    MAX(CASE WHEN travel_period = '6M'  THEN window_median_timeslots END) AS timeslots_med_6m,
    MAX(CASE WHEN travel_period = '12M' THEN window_median_timeslots END) AS timeslots_med_12m

  FROM window_level
  GROUP BY 1,2,3,4
)

SELECT *
FROM pivoted
ORDER BY
  country_name,
  period,
  CASE window 
    WHEN 'PRE_COMM' THEN 1 
    WHEN 'POST_COMM' THEN 2 
    WHEN 'PRE_ROLLOUT' THEN 3 
    WHEN 'POST_ROLLOUT' THEN 4 
  END,
  currency_iso_code;
