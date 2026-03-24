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

session_base AS (
  SELECT
    sp.*,
    CAST(sp.date AS date) AS dt,
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
    
    CASE
      WHEN CAST(sp.date AS date) >= dr.cy_pre_comm_start 
           AND CAST(sp.date AS date) <= dr.cy_post_rollout_end 
        THEN 'CY'
      WHEN CAST(sp.date AS date) >= dr.ly_pre_comm_start 
           AND CAST(sp.date AS date) <= dr.ly_post_rollout_end 
        THEN 'LY'
    END AS period
    
  FROM production.MARKETPLACE_REPORTS.AGG_SESSION_PERFORMANCE sp
  INNER JOIN production.dwh.dim_location dl
    ON sp.ip_geo_country_id = dl.country_id
  CROSS JOIN date_ranges dr
  WHERE
    LOWER(dl.country_name) = LOWER(dr.country)
    AND (
      (CAST(sp.date AS date) >= dr.cy_pre_comm_start AND CAST(sp.date AS date) <= dr.cy_post_rollout_end)
      OR
      (CAST(sp.date AS date) >= dr.ly_pre_comm_start AND CAST(sp.date AS date) <= dr.ly_post_rollout_end)
    )
),

windowed AS (
  -- PRE_COMM
  SELECT
    period,
    country_name,
    visitor_id,
    adp_views,
    checked_availability_activity_ids,
    added_to_cart_activity_ids,
    dt,
    'PRE_COMM' AS window
  FROM session_base
  WHERE (period = 'CY' AND dt >= cy_pre_comm_start AND dt <= cy_pre_comm_end)
     OR (period = 'LY' AND dt >= ly_pre_comm_start AND dt <= ly_pre_comm_end)
  
  UNION ALL
  
  -- POST_COMM
  SELECT
    period,
    country_name,
    visitor_id,
    adp_views,
    checked_availability_activity_ids,
    added_to_cart_activity_ids,
    dt,
    'POST_COMM' AS window
  FROM session_base
  WHERE (period = 'CY' AND dt >= cy_post_comm_start AND dt <= cy_post_comm_end)
     OR (period = 'LY' AND dt >= ly_post_comm_start AND dt <= ly_post_comm_end)
  
  UNION ALL
  
  -- PRE_ROLLOUT
  SELECT
    period,
    country_name,
    visitor_id,
    adp_views,
    checked_availability_activity_ids,
    added_to_cart_activity_ids,
    dt,
    'PRE_ROLLOUT' AS window
  FROM session_base
  WHERE (period = 'CY' AND dt >= cy_pre_rollout_start AND dt <= cy_pre_rollout_end)
     OR (period = 'LY' AND dt >= ly_pre_rollout_start AND dt <= ly_pre_rollout_end)
  
  UNION ALL
  
  -- POST_ROLLOUT
  SELECT
    period,
    country_name,
    visitor_id,
    adp_views,
    checked_availability_activity_ids,
    added_to_cart_activity_ids,
    dt,
    'POST_ROLLOUT' AS window
  FROM session_base
  WHERE (period = 'CY' AND dt >= cy_post_rollout_start AND dt <= cy_post_rollout_end)
     OR (period = 'LY' AND dt >= ly_post_rollout_start AND dt <= ly_post_rollout_end)
)

SELECT
  period,
  country_name,
  window,

  COUNT(DISTINCT visitor_id) AS total_visitors,
  COUNT(DISTINCT CASE WHEN adp_views > 0 THEN visitor_id END) AS adp_view_visitors,

  COUNT(DISTINCT CASE WHEN array_size(checked_availability_activity_ids) > 0 THEN visitor_id END)
    / NULLIF(COUNT(DISTINCT CASE WHEN adp_views > 0 THEN visitor_id END), 0)
    AS check_availability_rate,

  COUNT(DISTINCT CASE WHEN array_size(added_to_cart_activity_ids) > 0 THEN visitor_id END)
    / NULLIF(COUNT(DISTINCT CASE WHEN adp_views > 0 THEN visitor_id END), 0)
    AS add_to_cart_rate,

  COUNT(DISTINCT visitor_id) AS customers,

  COUNT(DISTINCT visitor_id)
    / NULLIF(COUNT(DISTINCT visitor_id), 0)
    AS conversion_rate

FROM windowed
WHERE period IS NOT NULL
  AND window IS NOT NULL
GROUP BY period, country_name, window
ORDER BY
  country_name,
  period,
  CASE window 
    WHEN 'PRE_COMM' THEN 1 
    WHEN 'POST_COMM' THEN 2 
    WHEN 'PRE_ROLLOUT' THEN 3 
    WHEN 'POST_ROLLOUT' THEN 4 
  END;
