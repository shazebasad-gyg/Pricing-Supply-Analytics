-- Weekly counterpart to Aggregated/customer_metrics_agg.sql: same DST logic, CY/LY,
-- weekly grain with week_offset from rollout and cy_* / ly_* pivoted.
-- Params: :comm_date, :rollout_date, :x_weeks, :country

WITH v_inputs AS (
  SELECT
    CAST(:comm_date    AS DATE)   AS comm_date,
    CAST(:rollout_date AS DATE)   AS rollout_date,
    CAST(:x_weeks      AS INT)    AS x_weeks,
    CAST(:country      AS STRING) AS country
),

date_ranges AS (
  SELECT
    CAST(date_trunc('week', CAST(pr.comm_date    AS timestamp)) AS date) AS cy_comm_week_start,
    CAST(date_trunc('week', CAST(pr.rollout_date AS timestamp)) AS date) AS cy_rollout_week_start,

    CAST(date_trunc('week', CAST(add_months(pr.comm_date,    -12) AS timestamp)) AS date) AS ly_comm_week_start,
    CAST(date_trunc('week', CAST(add_months(pr.rollout_date, -12) AS timestamp)) AS date) AS ly_rollout_week_start,

    pr.x_weeks,
    pr.country
  FROM v_inputs pr
),

session_base AS (
  SELECT
    sp.*,
    CAST(sp.date AS date) AS dt,
    dl.country_name
  FROM production.MARKETPLACE_REPORTS.AGG_SESSION_PERFORMANCE sp
  INNER JOIN production.dwh.dim_location dl
    ON sp.ip_geo_country_id = dl.country_id
  CROSS JOIN date_ranges dr
  WHERE
    lower(dl.country_name) = lower(dr.country)
    AND (
      CAST(sp.date AS date) BETWEEN date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
                               AND date_add(dr.cy_rollout_week_start, 7 * dr.x_weeks + 6)
      OR
      CAST(sp.date AS date) BETWEEN date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
                               AND date_add(dr.ly_rollout_week_start, 7 * dr.x_weeks + 6)
    )
),

windowed AS (
  SELECT
    sb.country_name,
    sb.visitor_id,
    sb.adp_views,
    sb.checked_availability_activity_ids,
    sb.added_to_cart_activity_ids,
    sb.dt,

    CASE
      WHEN sb.dt BETWEEN date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
                     AND date_add(dr.cy_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'CY'
      WHEN sb.dt BETWEEN date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
                     AND date_add(dr.ly_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'LY'
    END AS period,

    CASE
      WHEN sb.dt >= date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
       AND sb.dt <  dr.cy_comm_week_start
        THEN 'PRE'
      WHEN sb.dt >= dr.cy_comm_week_start
       AND sb.dt <  dr.cy_rollout_week_start
        THEN 'COMM'
      WHEN sb.dt >= dr.cy_rollout_week_start
       AND sb.dt <= date_add(dr.cy_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'POST'

      WHEN sb.dt >= date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
       AND sb.dt <  dr.ly_comm_week_start
        THEN 'PRE'
      WHEN sb.dt >= dr.ly_comm_week_start
       AND sb.dt <  dr.ly_rollout_week_start
        THEN 'COMM'
      WHEN sb.dt >= dr.ly_rollout_week_start
       AND sb.dt <= date_add(dr.ly_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'POST'
    END AS window

  FROM session_base sb
  CROSS JOIN date_ranges dr
),

weekly AS (
  SELECT
    country_name,
    period,
    window,
    CAST(date_trunc('week', CAST(dt AS timestamp)) AS date) AS week_start,

    COUNT(DISTINCT visitor_id) AS total_visitors,
    COUNT(DISTINCT IFF(adp_views > 0, visitor_id, NULL)) AS adp_view_visitors,

    COUNT(DISTINCT IFF(array_size(checked_availability_activity_ids) > 0, visitor_id, NULL))
      / NULLIF(COUNT(DISTINCT IFF(adp_views > 0, visitor_id, NULL)), 0)
      AS check_availability_rate,

    COUNT(DISTINCT IFF(array_size(added_to_cart_activity_ids) > 0, visitor_id, NULL))
      / NULLIF(COUNT(DISTINCT IFF(adp_views > 0, visitor_id, NULL)), 0)
      AS add_to_cart_rate,

    COUNT(DISTINCT IFF(visitor_id IS NOT NULL, visitor_id, NULL)) AS customers,

    COUNT(DISTINCT visitor_id)
      / NULLIF(COUNT(DISTINCT visitor_id), 0)
      AS conversion_rate

  FROM windowed
  WHERE period IS NOT NULL AND window IS NOT NULL
  GROUP BY 1, 2, 3, 4
),

indexed AS (
  SELECT
    w.*,
    CAST(
      FLOOR(
        datediff(
          w.week_start,
          CASE
            WHEN w.period = 'CY' THEN dr.cy_rollout_week_start
            ELSE dr.ly_rollout_week_start
          END
        ) / 7.0
      ) AS INT
    ) AS week_offset
  FROM weekly w
  CROSS JOIN date_ranges dr
)

SELECT
  country_name,
  week_offset,
  MAX(window) AS window,

  MAX(CASE WHEN period = 'CY' THEN week_start END) AS cy_week_start,
  MAX(CASE WHEN period = 'LY' THEN week_start END) AS ly_week_start,

  MAX(CASE WHEN period = 'CY' THEN total_visitors END) AS cy_total_visitors,
  MAX(CASE WHEN period = 'LY' THEN total_visitors END) AS ly_total_visitors,
  MAX(CASE WHEN period = 'CY' THEN adp_view_visitors END) AS cy_adp_view_visitors,
  MAX(CASE WHEN period = 'LY' THEN adp_view_visitors END) AS ly_adp_view_visitors,
  MAX(CASE WHEN period = 'CY' THEN check_availability_rate END) AS cy_check_availability_rate,
  MAX(CASE WHEN period = 'LY' THEN check_availability_rate END) AS ly_check_availability_rate,
  MAX(CASE WHEN period = 'CY' THEN add_to_cart_rate END) AS cy_add_to_cart_rate,
  MAX(CASE WHEN period = 'LY' THEN add_to_cart_rate END) AS ly_add_to_cart_rate,
  MAX(CASE WHEN period = 'CY' THEN customers END) AS cy_customers,
  MAX(CASE WHEN period = 'LY' THEN customers END) AS ly_customers,
  MAX(CASE WHEN period = 'CY' THEN conversion_rate END) AS cy_conversion_rate,
  MAX(CASE WHEN period = 'LY' THEN conversion_rate END) AS ly_conversion_rate

FROM indexed
GROUP BY 1, 2
ORDER BY 1, 2;
