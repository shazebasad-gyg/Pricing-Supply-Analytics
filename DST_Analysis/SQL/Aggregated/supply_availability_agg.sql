WITH v_inputs AS (
  SELECT
    CAST(:comm_date AS DATE) AS comm_date,
    CAST(:rollout_date AS DATE) AS rollout_date,
    CAST(:comm_weeks_before AS INT) AS comm_weeks_before,
    CAST(:comm_weeks_after AS INT) AS comm_weeks_after,
    CAST(:rollout_weeks_before AS INT) AS rollout_weeks_before,
    CAST(:rollout_weeks_after AS INT) AS rollout_weeks_after,
    CAST(:filter_tours AS INT) AS filter_tours,
    CAST(:filter_suppliers AS INT) AS filter_suppliers,
    CAST(:filter_countries AS INT) AS filter_countries
),

date_bounds AS (
  SELECT
    date_add(date_trunc('week', comm_date), -7 * comm_weeks_before) AS cy_min_date,
    date_add(date_trunc('week', rollout_date), 7 * rollout_weeks_after - 1) AS cy_max_date,
    date_add(date_trunc('week', add_months(comm_date, -12)), -7 * comm_weeks_before) AS ly_min_date,
    date_add(date_trunc('week', add_months(rollout_date, -12)), 7 * rollout_weeks_after - 1) AS ly_max_date,
    date_add(date_trunc('week', comm_date), -7 * comm_weeks_before) AS cy_pre_comm_start,
    date_add(date_trunc('week', comm_date), -1) AS cy_pre_comm_end,
    date_trunc('week', comm_date) AS cy_post_comm_start,
    date_add(date_trunc('week', comm_date), 7 * comm_weeks_after - 1) AS cy_post_comm_end,
    date_add(date_trunc('week', rollout_date), -7 * rollout_weeks_before) AS cy_pre_rollout_start,
    date_add(date_trunc('week', rollout_date), -1) AS cy_pre_rollout_end,
    date_trunc('week', rollout_date) AS cy_post_rollout_start,
    date_add(date_trunc('week', rollout_date), 7 * rollout_weeks_after - 1) AS cy_post_rollout_end,
    date_add(date_trunc('week', add_months(comm_date, -12)), -7 * comm_weeks_before) AS ly_pre_comm_start,
    date_add(date_trunc('week', add_months(comm_date, -12)), -1) AS ly_pre_comm_end,
    date_trunc('week', add_months(comm_date, -12)) AS ly_post_comm_start,
    date_add(date_trunc('week', add_months(comm_date, -12)), 7 * comm_weeks_after - 1) AS ly_post_comm_end,
    date_add(date_trunc('week', add_months(rollout_date, -12)), -7 * rollout_weeks_before) AS ly_pre_rollout_start,
    date_add(date_trunc('week', add_months(rollout_date, -12)), -1) AS ly_pre_rollout_end,
    date_trunc('week', add_months(rollout_date, -12)) AS ly_post_rollout_start,
    date_add(date_trunc('week', add_months(rollout_date, -12)), 7 * rollout_weeks_after - 1) AS ly_post_rollout_end,
    filter_tours,
    filter_suppliers,
    filter_countries
  FROM v_inputs
),

base_data AS (
  SELECT /*+ BROADCAST(db), BROADCAST(tf), BROADCAST(cf) */
    CASE 
      WHEN CAST(h.date_id AS date) BETWEEN db.cy_min_date AND db.cy_max_date THEN 'CY'
      WHEN CAST(h.date_id AS date) BETWEEN db.ly_min_date AND db.ly_max_date THEN 'LY'
    END AS period,
    dl.country_name,
    h.tour_id,
    h.supplier_id,
    date_trunc('week', CAST(h.date_id AS date)) AS week_start,
    CAST(h.date_id AS date) AS dt,
    CAST(h.is_online AS int) AS is_online_day,
    db.cy_pre_comm_start, db.cy_pre_comm_end,
    db.cy_post_comm_start, db.cy_post_comm_end,
    db.cy_pre_rollout_start, db.cy_pre_rollout_end,
    db.cy_post_rollout_start, db.cy_post_rollout_end,
    db.ly_pre_comm_start, db.ly_pre_comm_end,
    db.ly_post_comm_start, db.ly_post_comm_end,
    db.ly_pre_rollout_start, db.ly_pre_rollout_end,
    db.ly_post_rollout_start, db.ly_post_rollout_end
  FROM production.supply.fact_tour_review_history h
  INNER JOIN dwh.dim_location dl ON dl.location_id = h.location_id
  INNER JOIN date_bounds db ON 1=1
  INNER JOIN _param_tour_ids tf ON h.tour_id = tf.tour_id
  INNER JOIN _param_countries cf ON LOWER(dl.country_name) = LOWER(cf.country_name)
  WHERE
    (CAST(h.date_id AS date) BETWEEN db.cy_min_date AND db.cy_max_date
     OR CAST(h.date_id AS date) BETWEEN db.ly_min_date AND db.ly_max_date)
),

windowed_data AS (
  SELECT
    period, country_name, week_start, tour_id, supplier_id, dt, is_online_day,
    CASE
      WHEN (period = 'CY' AND dt BETWEEN cy_pre_comm_start AND cy_pre_comm_end) THEN 'PRE_COMM'
      WHEN (period = 'CY' AND dt BETWEEN cy_post_comm_start AND cy_post_comm_end) THEN 'POST_COMM'
      WHEN (period = 'CY' AND dt BETWEEN cy_pre_rollout_start AND cy_pre_rollout_end) THEN 'PRE_ROLLOUT'
      WHEN (period = 'CY' AND dt BETWEEN cy_post_rollout_start AND cy_post_rollout_end) THEN 'POST_ROLLOUT'
      WHEN (period = 'LY' AND dt BETWEEN ly_pre_comm_start AND ly_pre_comm_end) THEN 'PRE_COMM'
      WHEN (period = 'LY' AND dt BETWEEN ly_post_comm_start AND ly_post_comm_end) THEN 'POST_COMM'
      WHEN (period = 'LY' AND dt BETWEEN ly_pre_rollout_start AND ly_pre_rollout_end) THEN 'PRE_ROLLOUT'
      WHEN (period = 'LY' AND dt BETWEEN ly_post_rollout_start AND ly_post_rollout_end) THEN 'POST_ROLLOUT'
    END AS window
  FROM base_data
),

tour_week_agg AS (
  SELECT
    period, country_name, window, week_start, tour_id,
    SUM(is_online_day) AS online_days
  FROM windowed_data
  WHERE window IS NOT NULL
  GROUP BY period, country_name, window, week_start, tour_id
),

supplier_week_agg AS (
  SELECT
    period, country_name, window, week_start, supplier_id,
    SUM(is_online_day) AS online_days
  FROM windowed_data
  WHERE window IS NOT NULL
  GROUP BY period, country_name, window, week_start, supplier_id
),

final_metrics AS (
  SELECT
    wd.period,
    wd.country_name,
    wd.window,
    COUNT(DISTINCT wd.week_start) AS n_weeks,
    COUNT(DISTINCT wd.tour_id) AS total_tours,
    COUNT(DISTINCT CASE WHEN wd.is_online_day = 1 THEN wd.tour_id END) AS active_tours,
    COUNT(DISTINCT wd.supplier_id) AS total_suppliers,
    COUNT(DISTINCT CASE WHEN wd.is_online_day = 1 THEN wd.supplier_id END) AS active_suppliers,
    AVG(tw.online_days) AS avg_days_online_per_tour,
    AVG(CASE WHEN tw.online_days > 0 THEN tw.online_days END) AS avg_days_online_per_active_tour,
    AVG(sw.online_days) AS avg_days_online_per_supplier,
    AVG(CASE WHEN sw.online_days > 0 THEN sw.online_days END) AS avg_days_online_per_active_supplier
  FROM windowed_data wd
  LEFT JOIN tour_week_agg tw 
    ON wd.period = tw.period 
    AND wd.country_name = tw.country_name 
    AND wd.window = tw.window 
    AND wd.week_start = tw.week_start 
    AND wd.tour_id = tw.tour_id
  LEFT JOIN supplier_week_agg sw
    ON wd.period = sw.period 
    AND wd.country_name = sw.country_name 
    AND wd.window = sw.window 
    AND wd.week_start = sw.week_start 
    AND wd.supplier_id = sw.supplier_id
  WHERE wd.window IS NOT NULL
  GROUP BY wd.period, wd.country_name, wd.window
)

SELECT
  period,
  country_name,
  window,
  n_weeks,
  total_tours,
  active_tours,
  CAST(active_tours AS DOUBLE) / NULLIF(total_tours, 0) AS share_active_tours,
  total_suppliers,
  active_suppliers,
  CAST(active_suppliers AS DOUBLE) / NULLIF(total_suppliers, 0) AS share_active_suppliers,
  avg_days_online_per_tour,
  avg_days_online_per_active_tour,
  avg_days_online_per_supplier,
  avg_days_online_per_active_supplier
FROM final_metrics
ORDER BY country_name, period,
  CASE window
    WHEN 'PRE_COMM' THEN 1
    WHEN 'POST_COMM' THEN 2
    WHEN 'PRE_ROLLOUT' THEN 3
    WHEN 'POST_ROLLOUT' THEN 4
  END;
