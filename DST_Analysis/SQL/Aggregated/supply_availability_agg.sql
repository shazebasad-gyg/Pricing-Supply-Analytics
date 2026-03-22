WITH v_inputs AS (
  SELECT
    CAST(:comm_date    AS DATE)   AS comm_date,
    CAST(:rollout_date AS DATE)   AS rollout_date,
    CAST(:start_date   AS DATE)   AS start_date,
    CAST(:end_date     AS DATE)   AS end_date,
    CAST(:country      AS STRING) AS country,
    CAST(:tour_ids     AS STRING) AS tour_ids,
    CAST(:x_weeks      AS INT)    AS x_weeks
),

tour_filter AS (
  SELECT CAST(trim(x) AS BIGINT) AS tour_id
  FROM v_inputs p
  LATERAL VIEW explode(split(p.tour_ids, ',')) e AS x
  WHERE trim(x) <> ''
),


date_ranges AS (
  SELECT
    CAST(date_trunc('week', CAST(pr.comm_date    AS timestamp)) AS date) AS cy_comm_week_start,
    CAST(date_trunc('week', CAST(pr.rollout_date AS timestamp)) AS date) AS cy_rollout_week_start,

    CAST(date_trunc('week', CAST(add_months(pr.comm_date,    -12) AS timestamp)) AS date) AS ly_comm_week_start,
    CAST(date_trunc('week', CAST(add_months(pr.rollout_date, -12) AS timestamp)) AS date) AS ly_rollout_week_start,

    pr.country,
    pr.x_weeks
  FROM v_inputs pr
),


history_filtered AS (
  SELECT
    dr.country AS country_name,
    h.tour_id,
    h.supplier_id,
    CAST(h.date_id AS date) AS dt,

    CASE
      WHEN CAST(h.date_id AS date) BETWEEN
           date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
           AND date_add(dr.cy_rollout_week_start,  7 * dr.x_weeks + 6)
        THEN 'CY'
      WHEN CAST(h.date_id AS date) BETWEEN
           date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
           AND date_add(dr.ly_rollout_week_start,  7 * dr.x_weeks + 6)
        THEN 'LY'
    END AS period,

    CAST(h.is_online AS int) AS is_online_day
  FROM production.supply.fact_tour_review_history h
  CROSS JOIN date_ranges dr
  INNER JOIN tour_filter tf
    ON h.tour_id = tf.tour_id
  INNER JOIN dwh.dim_location dl
    ON dl.location_id = h.location_id
  WHERE
    (
      CAST(h.date_id AS date) BETWEEN
        date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
        AND date_add(dr.cy_rollout_week_start,  7 * dr.x_weeks + 6)
    )
    OR
    (
      CAST(h.date_id AS date) BETWEEN
        date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
        AND date_add(dr.ly_rollout_week_start,  7 * dr.x_weeks + 6)
    )
),

tours_in_scope AS (
  SELECT DISTINCT
    period,
    country_name,
    tour_id,
    supplier_id,
    dt,
    is_online_day
  FROM history_filtered
),


windowed_days AS (
  SELECT
    tis.period,
    tis.country_name,
    tis.tour_id,
    tis.supplier_id,
    tis.dt,
    CAST(date_trunc('week', CAST(tis.dt AS timestamp)) AS date) AS week_start,
    tis.is_online_day,

    CASE
      -- CY windows
      WHEN tis.period = 'CY'
           AND tis.dt >= date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
           AND tis.dt <  dr.cy_comm_week_start
        THEN 'PRE'

      WHEN tis.period = 'CY'
           AND tis.dt >= dr.cy_comm_week_start
           AND tis.dt <  dr.cy_rollout_week_start
        THEN 'COMM'

      WHEN tis.period = 'CY'
           AND tis.dt >= dr.cy_rollout_week_start
           AND tis.dt <= date_add(dr.cy_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'POST'

      -- LY windows
      WHEN tis.period = 'LY'
           AND tis.dt >= date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
           AND tis.dt <  dr.ly_comm_week_start
        THEN 'PRE'

      WHEN tis.period = 'LY'
           AND tis.dt >= dr.ly_comm_week_start
           AND tis.dt <  dr.ly_rollout_week_start
        THEN 'COMM'

      WHEN tis.period = 'LY'
           AND tis.dt >= dr.ly_rollout_week_start
           AND tis.dt <= date_add(dr.ly_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'POST'
    END AS window

  FROM tours_in_scope tis
  CROSS JOIN date_ranges dr
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

    COUNT(DISTINCT CASE WHEN is_online_day = 1 THEN tour_id END)
      / NULLIF(COUNT(DISTINCT tour_id), 0) AS share_active_tours,

    COUNT(DISTINCT CASE WHEN is_online_day = 1 THEN supplier_id END)
      / NULLIF(COUNT(DISTINCT supplier_id), 0) AS share_active_suppliers

  FROM windowed_days
  WHERE window IS NOT NULL
  GROUP BY 1,2,3
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
  GROUP BY 1,2,3,4,5
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
  GROUP BY 1,2,3,4
),

tour_window_weekly_avgs AS (
  SELECT
    period,
    country_name,
    window,
    AVG(avg_days_online_per_tour_week) AS avg_days_online_per_tour,
    AVG(avg_days_online_per_active_tour_week) AS avg_days_online_per_active_tour
  FROM tour_week_metrics
  GROUP BY 1,2,3
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
  GROUP BY 1,2,3,4,5,6
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
  GROUP BY 1,2,3,4,5
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
  GROUP BY 1,2,3,4
),

supplier_window_weekly_avgs AS (
  SELECT
    period,
    country_name,
    window,
    AVG(avg_days_online_per_supplier_week) AS avg_days_online_per_supplier,
    AVG(avg_days_online_per_active_supplier_week) AS avg_days_online_per_active_supplier
  FROM supplier_week_metrics
  GROUP BY 1,2,3
),


final_window_output AS (
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
)

SELECT *
FROM final_window_output
ORDER BY country_name, period, window;
