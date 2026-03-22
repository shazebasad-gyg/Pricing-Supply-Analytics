WITH v_inputs AS (
  SELECT
    CAST(:comm_date    AS DATE)   AS comm_date,
    CAST(:rollout_date AS DATE)   AS rollout_date,
    CAST(:x_weeks      AS INT)    AS x_weeks,
    CAST(:country      AS STRING) AS country,
    CAST(:tour_ids     AS STRING) AS tour_ids
),

tour_filter AS (
  SELECT CAST(trim(x) AS BIGINT) AS tour_id
  FROM v_inputs p
  LATERAL VIEW explode(split(coalesce(p.tour_ids,''), ',')) e AS x
  WHERE trim(x) <> ''
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

booking_scoped AS (
  SELECT
    dr.country AS country_name,
    CAST(fb.date_of_checkout AS date) AS dt_checkout,
    fb.booking_id,
    fb.tickets,
    fb.nr,
    fb.gmv,
    fb.supplier_share,
    fb.gmv_supplier,
    fb.price_net_rate_eur

  FROM production.dwh.fact_booking_v2 fb
  INNER JOIN production.dwh.dim_status s
    ON coalesce(fb.status_id, -1) = s.status_id
  LEFT JOIN production.dwh.dim_tour tour
    ON fb.tour_id = tour.tour_id
  LEFT JOIN production.dwh.dim_location primary_location
    ON tour.location_id = primary_location.location_id
  CROSS JOIN date_ranges dr

  WHERE
    s.status_display = 'Active'

    AND lower(coalesce(primary_location.country_name,'Other')) = lower(dr.country)

    AND (
      (SELECT COUNT(*) FROM tour_filter) = 0
      OR fb.tour_id IN (SELECT tour_id FROM tour_filter)
    )

    AND (
      CAST(fb.date_of_checkout AS date) BETWEEN date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
                     AND date_add(dr.cy_rollout_week_start, 7 * dr.x_weeks + 6)
      OR
      CAST(fb.date_of_checkout AS date) BETWEEN date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
                     AND date_add(dr.ly_rollout_week_start, 7 * dr.x_weeks + 6)
    )
),

windowed AS (
  SELECT
    bs.country_name,
    bs.booking_id,
    bs.tickets,
    bs.nr,
    bs.gmv,
    bs.supplier_share,
    bs.price_net_rate_eur,

    CASE
      WHEN bs.dt_checkout BETWEEN
           date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
           AND date_add(dr.cy_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'CY'
      WHEN bs.dt_checkout BETWEEN
           date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
           AND date_add(dr.ly_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'LY'
    END AS period,

    CASE
      -- CY windows
      WHEN bs.dt_checkout >= date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
       AND bs.dt_checkout <  dr.cy_comm_week_start
        THEN 'PRE'
      WHEN bs.dt_checkout >= dr.cy_comm_week_start
       AND bs.dt_checkout <  dr.cy_rollout_week_start
        THEN 'COMM'
      WHEN bs.dt_checkout >= dr.cy_rollout_week_start
       AND bs.dt_checkout <= date_add(dr.cy_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'POST'

      -- LY windows
      WHEN bs.dt_checkout >= date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
       AND bs.dt_checkout <  dr.ly_comm_week_start
        THEN 'PRE'
      WHEN bs.dt_checkout >= dr.ly_comm_week_start
       AND bs.dt_checkout <  dr.ly_rollout_week_start
        THEN 'COMM'
      WHEN bs.dt_checkout >= dr.ly_rollout_week_start
       AND bs.dt_checkout <= date_add(dr.ly_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'POST'
    END AS window

  FROM booking_scoped bs
  CROSS JOIN date_ranges dr
)

SELECT
  period,
  country_name,
  window,
  COUNT(DISTINCT booking_id) AS bookings,
  SUM(tickets) AS tickets,
  SUM(nr)      AS nr,
  SUM(gmv)     AS gmv,
  SUM(supplier_share) AS supplier_share_revenue,
  AVG(price_net_rate_eur) AS avg_effective_price_eur,
  percentile_approx(price_net_rate_eur, 0.5) AS median_effective_price_eur 
FROM windowed
WHERE period IS NOT NULL AND window IS NOT NULL
GROUP BY 1,2,3
ORDER BY
  period,
  country_name,
  CASE window WHEN 'PRE' THEN 1 WHEN 'COMM' THEN 2 WHEN 'POST' THEN 3 ELSE 9 END;
