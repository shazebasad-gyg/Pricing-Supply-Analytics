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
  FROM v_inputs vi
  LATERAL VIEW explode(split(vi.tour_ids, ',')) AS value
  WHERE vi.tour_ids != '' AND trim(value) != ''
),

supplier_filter AS (
  SELECT CAST(trim(value) AS BIGINT) AS supplier_id
  FROM v_inputs vi
  LATERAL VIEW explode(split(vi.supplier_ids, ',')) AS value
  WHERE vi.supplier_ids != '' AND trim(value) != ''
),

filter_flags AS (
  SELECT 
    CASE WHEN COALESCE(tour_ids, '') = '' THEN 0 ELSE 1 END AS should_filter_tours,
    CASE WHEN COALESCE(supplier_ids, '') = '' THEN 0 ELSE 1 END AS should_filter_suppliers
  FROM v_inputs
),

window_defs AS (
  SELECT 'CY' AS period, 'PRE_COMM' AS window,
    date_add(date_trunc('week', vi.comm_date), -7 * vi.comm_weeks_before) AS snap_start,
    date_add(date_trunc('week', vi.comm_date), -1) AS snap_end,
    date_trunc('week', vi.comm_date) AS travel_anchor,
    vi.country
  FROM v_inputs vi
  UNION ALL
  SELECT 'CY', 'POST_COMM',
    date_trunc('week', vi.comm_date),
    date_add(date_trunc('week', vi.comm_date), 7 * vi.comm_weeks_after - 1),
    date_trunc('week', vi.comm_date),
    vi.country
  FROM v_inputs vi
  UNION ALL
  SELECT 'CY', 'PRE_ROLLOUT',
    date_add(date_trunc('week', vi.rollout_date), -7 * vi.rollout_weeks_before),
    date_add(date_trunc('week', vi.rollout_date), -1),
    date_trunc('week', vi.rollout_date),
    vi.country
  FROM v_inputs vi
  UNION ALL
  SELECT 'CY', 'POST_ROLLOUT',
    date_trunc('week', vi.rollout_date),
    date_add(date_trunc('week', vi.rollout_date), 7 * vi.rollout_weeks_after - 1),
    date_trunc('week', vi.rollout_date),
    vi.country
  FROM v_inputs vi
  UNION ALL
  SELECT 'LY', 'PRE_COMM',
    date_add(date_trunc('week', add_months(vi.comm_date, -12)), -7 * vi.comm_weeks_before),
    date_add(date_trunc('week', add_months(vi.comm_date, -12)), -1),
    date_trunc('week', add_months(vi.comm_date, -12)),
    vi.country
  FROM v_inputs vi
  UNION ALL
  SELECT 'LY', 'POST_COMM',
    date_trunc('week', add_months(vi.comm_date, -12)),
    date_add(date_trunc('week', add_months(vi.comm_date, -12)), 7 * vi.comm_weeks_after - 1),
    date_trunc('week', add_months(vi.comm_date, -12)),
    vi.country
  FROM v_inputs vi
  UNION ALL
  SELECT 'LY', 'PRE_ROLLOUT',
    date_add(date_trunc('week', add_months(vi.rollout_date, -12)), -7 * vi.rollout_weeks_before),
    date_add(date_trunc('week', add_months(vi.rollout_date, -12)), -1),
    date_trunc('week', add_months(vi.rollout_date, -12)),
    vi.country
  FROM v_inputs vi
  UNION ALL
  SELECT 'LY', 'POST_ROLLOUT',
    date_trunc('week', add_months(vi.rollout_date, -12)),
    date_add(date_trunc('week', add_months(vi.rollout_date, -12)), 7 * vi.rollout_weeks_after - 1),
    date_trunc('week', add_months(vi.rollout_date, -12)),
    vi.country
  FROM v_inputs vi
),

price_filtered AS (
  SELECT /*+ BROADCAST(ff, tf, sf) */
    CAST(p.snapshot_date AS date) AS snapshot_dt,
    CAST(p.travel_date AS date) AS travel_dt,
    p.currency_iso_code,
    p.tour_id,
    p.tour_option_id,
    p.final_price_red_per_adult,
    p.final_price_black_per_adult,
    CASE WHEN p.availability_timeslots IS NULL THEN 0 ELSE size(p.availability_timeslots) END AS timeslot_count,
    dl.country_name
  FROM production.dwh.daily_tour_price_snapshot p
  INNER JOIN production.dwh.dim_tour dt ON p.tour_id = dt.tour_id
  INNER JOIN production.dwh.dim_location dl ON dt.location_id = dl.location_id
  CROSS JOIN filter_flags ff
  LEFT JOIN tour_filter tf ON p.tour_id = tf.tour_id
  LEFT JOIN supplier_filter sf ON dt.user_id = sf.supplier_id
  WHERE
    LOWER(dl.country_name) = LOWER(CAST(:country AS STRING))
    AND p.snapshot_date >= date_add(date_trunc('week', add_months(CAST(:comm_date AS DATE), -12)), -7 * CAST(:comm_weeks_before AS INT))
    AND p.snapshot_date <= date_add(date_trunc('week', CAST(:rollout_date AS DATE)), 7 * CAST(:rollout_weeks_after AS INT))
    AND p.travel_date >= date_trunc('week', add_months(CAST(:comm_date AS DATE), -12))
    AND p.travel_date <= add_months(date_trunc('week', CAST(:rollout_date AS DATE)), 12)
    AND (ff.should_filter_tours = 0 OR tf.tour_id IS NOT NULL)
    AND (ff.should_filter_suppliers = 0 OR sf.supplier_id IS NOT NULL)
    AND p.final_price_red_per_adult IS NOT NULL
    AND p.final_price_red_per_adult > 0
),

windowed AS (
  SELECT /*+ BROADCAST(wd) */
    wd.period,
    wd.window,
    pf.country_name,
    date_trunc('week', pf.snapshot_dt) AS snapshot_week_start,
    pf.currency_iso_code,
    pf.tour_id,
    pf.tour_option_id,
    pf.final_price_red_per_adult,
    pf.final_price_black_per_adult,
    pf.timeslot_count,
    CASE
      WHEN pf.travel_dt >= wd.travel_anchor AND pf.travel_dt < add_months(wd.travel_anchor, 3) THEN '3M'
      WHEN pf.travel_dt >= add_months(wd.travel_anchor, 3) AND pf.travel_dt < add_months(wd.travel_anchor, 6) THEN '6M'
      WHEN pf.travel_dt >= add_months(wd.travel_anchor, 6) AND pf.travel_dt <= add_months(wd.travel_anchor, 12) THEN '12M'
    END AS travel_period
  FROM price_filtered pf
  INNER JOIN window_defs wd
    ON pf.snapshot_dt >= wd.snap_start
    AND pf.snapshot_dt <= wd.snap_end
    AND LOWER(pf.country_name) = LOWER(wd.country)
    AND pf.travel_dt >= wd.travel_anchor
    AND pf.travel_dt <= add_months(wd.travel_anchor, 12)
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
    percentile_approx(final_price_red_per_adult, 0.5) AS wk_median_red,
    percentile_approx(final_price_black_per_adult, 0.5) AS wk_median_black,
    percentile_approx(timeslot_count, 0.5) AS wk_median_timeslots
  FROM windowed
  WHERE travel_period IS NOT NULL
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
    percentile_approx(wk_median_red, 0.5) AS window_median_red,
    percentile_approx(wk_median_black, 0.5) AS window_median_black,
    percentile_approx(wk_median_timeslots, 0.5) AS window_median_timeslots
  FROM weekly_metrics
  GROUP BY 1,2,3,4,5
)

SELECT
  period,
  country_name,
  window,
  currency_iso_code,
  MAX(CASE WHEN travel_period = '3M' THEN weeks_in_window END) AS weeks_3m,
  MAX(CASE WHEN travel_period = '6M' THEN weeks_in_window END) AS weeks_6m,
  MAX(CASE WHEN travel_period = '12M' THEN weeks_in_window END) AS weeks_12m,
  MAX(CASE WHEN travel_period = '3M' THEN window_median_red END) AS red_med_3m,
  MAX(CASE WHEN travel_period = '6M' THEN window_median_red END) AS red_med_6m,
  MAX(CASE WHEN travel_period = '12M' THEN window_median_red END) AS red_med_12m,
  MAX(CASE WHEN travel_period = '3M' THEN window_median_black END) AS black_med_3m,
  MAX(CASE WHEN travel_period = '6M' THEN window_median_black END) AS black_med_6m,
  MAX(CASE WHEN travel_period = '12M' THEN window_median_black END) AS black_med_12m,
  MAX(CASE WHEN travel_period = '3M' THEN window_median_timeslots END) AS timeslots_med_3m,
  MAX(CASE WHEN travel_period = '6M' THEN window_median_timeslots END) AS timeslots_med_6m,
  MAX(CASE WHEN travel_period = '12M' THEN window_median_timeslots END) AS timeslots_med_12m
FROM window_level
GROUP BY 1,2,3,4
ORDER BY country_name, period,
  CASE window 
    WHEN 'PRE_COMM' THEN 1 
    WHEN 'POST_COMM' THEN 2 
    WHEN 'PRE_ROLLOUT' THEN 3 
    WHEN 'POST_ROLLOUT' THEN 4 
  END,
  currency_iso_code;
