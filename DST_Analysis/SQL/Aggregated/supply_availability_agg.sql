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
    
    -- CY windows
    date_add(date_trunc('week', pr.comm_date), -7 * pr.comm_weeks_before) AS cy_pre_comm_start,
    date_add(date_trunc('week', pr.comm_date), -1) AS cy_pre_comm_end,
    
    date_trunc('week', pr.comm_date) AS cy_post_comm_start,
    date_add(date_trunc('week', pr.comm_date), 7 * pr.comm_weeks_after - 1) AS cy_post_comm_end,
    
    date_add(date_trunc('week', pr.rollout_date), -7 * pr.rollout_weeks_before) AS cy_pre_rollout_start,
    date_add(date_trunc('week', pr.rollout_date), -1) AS cy_pre_rollout_end,
    
    date_trunc('week', pr.rollout_date) AS cy_post_rollout_start,
    date_add(date_trunc('week', pr.rollout_date), 7 * pr.rollout_weeks_after - 1) AS cy_post_rollout_end,
    
    -- LY windows
    date_add(date_trunc('week', add_months(pr.comm_date, -12)), -7 * pr.comm_weeks_before) AS ly_pre_comm_start,
    date_add(date_trunc('week', add_months(pr.comm_date, -12)), -1) AS ly_pre_comm_end,
    
    date_trunc('week', add_months(pr.comm_date, -12)) AS ly_post_comm_start,
    date_add(date_trunc('week', add_months(pr.comm_date, -12)), 7 * pr.comm_weeks_after - 1) AS ly_post_comm_end,
    
    date_add(date_trunc('week', add_months(pr.rollout_date, -12)), -7 * pr.rollout_weeks_before) AS ly_pre_rollout_start,
    date_add(date_trunc('week', add_months(pr.rollout_date, -12)), -1) AS ly_pre_rollout_end,
    
    date_trunc('week', add_months(pr.rollout_date, -12)) AS ly_post_rollout_start,
    date_add(date_trunc('week', add_months(pr.rollout_date, -12)), 7 * pr.rollout_weeks_after - 1) AS ly_post_rollout_end
    
  FROM v_inputs pr
),

history_filtered AS (
  SELECT
    dr.country AS country_name,
    h.tour_id,
    h.supplier_id,
    CAST(h.date_id AS date) AS dt,

    CASE
      WHEN CAST(h.date_id AS date) >= dr.cy_pre_comm_start 
           AND CAST(h.date_id AS date) <= dr.cy_post_rollout_end 
        THEN 'CY'
      WHEN CAST(h.date_id AS date) >= dr.ly_pre_comm_start 
           AND CAST(h.date_id AS date) <= dr.ly_post_rollout_end 
        THEN 'LY'
    END AS period,

    CAST(h.is_online AS int) AS is_online_day,
    
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
    dr.ly_post_rollout_end
    
  FROM production.supply.fact_tour_review_history h
  INNER JOIN dwh.dim_location dl
    ON dl.location_id = h.location_id
  CROSS JOIN date_ranges dr
  CROSS JOIN filter_flags ff
  LEFT JOIN tour_filter tf
    ON h.tour_id = tf.tour_id
  LEFT JOIN supplier_filter sf
    ON h.supplier_id = sf.supplier_id
  WHERE
    LOWER(dl.country_name) = LOWER(dr.country)
    AND (ff.should_filter_tours = 0 OR tf.tour_id IS NOT NULL)
    AND (ff.should_filter_suppliers = 0 OR sf.supplier_id IS NOT NULL)
    AND (
      (CAST(h.date_id AS date) >= dr.cy_pre_comm_start AND CAST(h.date_id AS date) <= dr.cy_post_rollout_end)
      OR
      (CAST(h.date_id AS date) >= dr.ly_pre_comm_start AND CAST(h.date_id AS date) <= dr.ly_post_rollout_end)
    )
),

tours_in_scope AS (
  SELECT DISTINCT
    period,
    country_name,
    tour_id,
    supplier_id,
    dt,
    is_online_day,
    cy_pre_comm_start,
    cy_pre_comm_end,
    cy_post_comm_start,
    cy_post_comm_end,
    cy_pre_rollout_start,
    cy_pre_rollout_end,
    cy_post_rollout_start,
    cy_post_rollout_end,
    ly_pre_comm_start,
    ly_pre_comm_end,
    ly_post_comm_start,
    ly_post_comm_end,
    ly_pre_rollout_start,
    ly_pre_rollout_end,
    ly_post_rollout_start,
    ly_post_rollout_end
  FROM history_filtered
),

windowed_days AS (
  -- PRE_COMM
  SELECT
    period, country_name, tour_id, supplier_id, dt,
    date_trunc('week', dt) AS week_start,
    is_online_day,
    'PRE_COMM' AS window
  FROM tours_in_scope
  WHERE (period = 'CY' AND dt >= cy_pre_comm_start AND dt <= cy_pre_comm_end)
     OR (period = 'LY' AND dt >= ly_pre_comm_start AND dt <= ly_pre_comm_end)
  
  UNION ALL
  
  -- POST_COMM
  SELECT
    period, country_name, tour_id, supplier_id, dt,
    date_trunc('week', dt) AS week_start,
    is_online_day,
    'POST_COMM' AS window
  FROM tours_in_scope
  WHERE (period = 'CY' AND dt >= cy_post_comm_start AND dt <= cy_post_comm_end)
     OR (period = 'LY' AND dt >= ly_post_comm_start AND dt <= ly_post_comm_end)
  
  UNION ALL
  
  -- PRE_ROLLOUT
  SELECT
    period, country_name, tour_id, supplier_id, dt,
    date_trunc('week', dt) AS week_start,
    is_online_day,
    'PRE_ROLLOUT' AS window
  FROM tours_in_scope
  WHERE (period = 'CY' AND dt >= cy_pre_rollout_start AND dt <= cy_pre_rollout_end)
     OR (period = 'LY' AND dt >= ly_pre_rollout_start AND dt <= ly_pre_rollout_end)
  
  UNION ALL
  
  -- POST_ROLLOUT
  SELECT
    period, country_name, tour_id, supplier_id, dt,
    date_trunc('week', dt) AS week_start,
    is_online_day,
    'POST_ROLLOUT' AS window
  FROM tours_in_scope
  WHERE (period = 'CY' AND dt >= cy_post_rollout_start AND dt <= cy_post_rollout_end)
     OR (period = 'LY' AND dt >= ly_post_rollout_start AND dt <= ly_post_rollout_end)
),

window_counts AS (
  SELECT
    period,
    country_name,
    window,
    COUNT(DISTINCT week_start) AS n_weeks,
    COUNT(DISTINCT tour_id) AS total_tours,
    COUNT(DISTINCT CASE WHEN is_online_day = 1 THEN tour_id END) AS active_tours,
    COUNT(DISTINCT supplier_id) AS total_suppliers,
    COUNT(DISTINCT CASE WHEN is_online_day = 1 THEN supplier_id END) AS active_suppliers,
    COUNT(DISTINCT CASE WHEN is_online_day = 1 THEN tour_id END) / NULLIF(COUNT(DISTINCT tour_id), 0) AS share_active_tours,
    COUNT(DISTINCT CASE WHEN is_online_day = 1 THEN supplier_id END) / NULLIF(COUNT(DISTINCT supplier_id), 0) AS share_active_suppliers
  FROM windowed_days
  WHERE window IS NOT NULL
  GROUP BY period, country_name, window
),

tour_week_days AS (
  SELECT
    period,
    country_name,
    window,
    week_start,
    tour_id,
    SUM(is_online_day) AS online_days
  FROM windowed_days
  WHERE window IS NOT NULL
  GROUP BY period, country_name, window, week_start, tour_id
),

tour_week_metrics AS (
  SELECT
    period,
    country_name,
    window,
    week_start,
    AVG(online_days) AS avg_days_online_per_tour_week,
    AVG(CASE WHEN online_days > 0 THEN online_days END) AS avg_days_online_per_active_tour_week
  FROM tour_week_days
  GROUP BY period, country_name, window, week_start
),

tour_window_weekly_avgs AS (
  SELECT
    period,
    country_name,
    window,
    AVG(avg_days_online_per_tour_week) AS avg_days_online_per_tour,
    AVG(avg_days_online_per_active_tour_week) AS avg_days_online_per_active_tour
  FROM tour_week_metrics
  GROUP BY period, country_name, window
),

supplier_day_dedup AS (
  SELECT
    period,
    country_name,
    window,
    week_start,
    supplier_id,
    dt,
    MAX(is_online_day) AS supplier_online_day
  FROM windowed_days
  WHERE window IS NOT NULL
  GROUP BY period, country_name, window, week_start, supplier_id, dt
),

supplier_week_days AS (
  SELECT
    period,
    country_name,
    window,
    week_start,
    supplier_id,
    SUM(supplier_online_day) AS online_days
  FROM supplier_day_dedup
  GROUP BY period, country_name, window, week_start, supplier_id
),

supplier_week_metrics AS (
  SELECT
    period,
    country_name,
    window,
    week_start,
    AVG(online_days) AS avg_days_online_per_supplier_week,
    AVG(CASE WHEN online_days > 0 THEN online_days END) AS avg_days_online_per_active_supplier_week
  FROM supplier_week_days
  GROUP BY period, country_name, window, week_start
),

supplier_window_weekly_avgs AS (
  SELECT
    period,
    country_name,
    window,
    AVG(avg_days_online_per_supplier_week) AS avg_days_online_per_supplier,
    AVG(avg_days_online_per_active_supplier_week) AS avg_days_online_per_active_supplier
  FROM supplier_week_metrics
  GROUP BY period, country_name, window
)

SELECT
  wc.period,
  wc.country_name,
  wc.window,
  wc.n_weeks,
  wc.total_tours,
  wc.active_tours,
  wc.share_active_tours,
  wc.total_suppliers,
  wc.active_suppliers,
  wc.share_active_suppliers,
  twa.avg_days_online_per_tour,
  twa.avg_days_online_per_active_tour,
  swa.avg_days_online_per_supplier,
  swa.avg_days_online_per_active_supplier
FROM window_counts wc
LEFT JOIN tour_window_weekly_avgs twa
  ON twa.period = wc.period
  AND twa.country_name = wc.country_name
  AND twa.window = wc.window
LEFT JOIN supplier_window_weekly_avgs swa
  ON swa.period = wc.period
  AND swa.country_name = wc.country_name
  AND swa.window = wc.window
ORDER BY country_name, period, 
  CASE window 
    WHEN 'PRE_COMM' THEN 1 
    WHEN 'POST_COMM' THEN 2 
    WHEN 'PRE_ROLLOUT' THEN 3 
    WHEN 'POST_ROLLOUT' THEN 4 
  END;
