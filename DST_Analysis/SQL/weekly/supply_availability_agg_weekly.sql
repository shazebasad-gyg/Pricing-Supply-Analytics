-- Weekly counterpart to Aggregated/supply_availability_agg.sql: same DST windows and definitions,
-- one row per week_offset from rollout with cy_* / ly_* pivoted (supply intensity / extensive margin).
-- Params: :comm_date, :rollout_date, :x_weeks, :country, :tour_ids

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

weekly_counts AS (
  SELECT
    period,
    country_name,
    window,
    week_start,

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
  GROUP BY 1, 2, 3, 4
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
  GROUP BY 1, 2, 3, 4, 5
),

tour_week_metrics AS (
  SELECT
    period,
    country_name,
    window,
    week_start,
    AVG(online_days) AS avg_days_online_per_tour,
    AVG(CASE WHEN online_days > 0 THEN online_days END) AS avg_days_online_per_active_tour
  FROM tour_week_days
  GROUP BY 1, 2, 3, 4
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
  GROUP BY 1, 2, 3, 4, 5, 6
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
  GROUP BY 1, 2, 3, 4, 5
),

supplier_week_metrics AS (
  SELECT
    period,
    country_name,
    window,
    week_start,
    AVG(online_days) AS avg_days_online_per_supplier,
    AVG(CASE WHEN online_days > 0 THEN online_days END) AS avg_days_online_per_active_supplier
  FROM supplier_week_days
  GROUP BY 1, 2, 3, 4
),

supply_weekly AS (
  SELECT
    wc.period,
    wc.country_name,
    wc.window,
    wc.week_start,

    wc.total_tours,
    wc.active_tours,
    wc.share_active_tours,
    wc.total_suppliers,
    wc.active_suppliers,
    wc.share_active_suppliers,

    twm.avg_days_online_per_tour,
    twm.avg_days_online_per_active_tour,
    swm.avg_days_online_per_supplier,
    swm.avg_days_online_per_active_supplier

  FROM weekly_counts wc
  LEFT JOIN tour_week_metrics twm
    ON twm.period = wc.period
    AND twm.country_name = wc.country_name
    AND twm.window = wc.window
    AND twm.week_start = wc.week_start
  LEFT JOIN supplier_week_metrics swm
    ON swm.period = wc.period
    AND swm.country_name = wc.country_name
    AND swm.window = wc.window
    AND swm.week_start = wc.week_start
),

indexed AS (
  SELECT
    s.*,
    CAST(
      FLOOR(
        datediff(
          s.week_start,
          CASE
            WHEN s.period = 'CY' THEN dr.cy_rollout_week_start
            ELSE dr.ly_rollout_week_start
          END
        ) / 7.0
      ) AS INT
    ) AS week_offset
  FROM supply_weekly s
  CROSS JOIN date_ranges dr
)

SELECT
  country_name,
  week_offset,
  MAX(window) AS window,

  MAX(CASE WHEN period = 'CY' THEN week_start END) AS cy_week_start,
  MAX(CASE WHEN period = 'LY' THEN week_start END) AS ly_week_start,

  MAX(CASE WHEN period = 'CY' THEN total_tours END) AS cy_total_tours,
  MAX(CASE WHEN period = 'LY' THEN total_tours END) AS ly_total_tours,
  MAX(CASE WHEN period = 'CY' THEN active_tours END) AS cy_active_tours,
  MAX(CASE WHEN period = 'LY' THEN active_tours END) AS ly_active_tours,
  MAX(CASE WHEN period = 'CY' THEN share_active_tours END) AS cy_share_active_tours,
  MAX(CASE WHEN period = 'LY' THEN share_active_tours END) AS ly_share_active_tours,

  MAX(CASE WHEN period = 'CY' THEN total_suppliers END) AS cy_total_suppliers,
  MAX(CASE WHEN period = 'LY' THEN total_suppliers END) AS ly_total_suppliers,
  MAX(CASE WHEN period = 'CY' THEN active_suppliers END) AS cy_active_suppliers,
  MAX(CASE WHEN period = 'LY' THEN active_suppliers END) AS ly_active_suppliers,
  MAX(CASE WHEN period = 'CY' THEN share_active_suppliers END) AS cy_share_active_suppliers,
  MAX(CASE WHEN period = 'LY' THEN share_active_suppliers END) AS ly_share_active_suppliers,

  MAX(CASE WHEN period = 'CY' THEN avg_days_online_per_tour END) AS cy_avg_days_online_per_tour,
  MAX(CASE WHEN period = 'LY' THEN avg_days_online_per_tour END) AS ly_avg_days_online_per_tour,
  MAX(CASE WHEN period = 'CY' THEN avg_days_online_per_active_tour END) AS cy_avg_days_online_per_active_tour,
  MAX(CASE WHEN period = 'LY' THEN avg_days_online_per_active_tour END) AS ly_avg_days_online_per_active_tour,

  MAX(CASE WHEN period = 'CY' THEN avg_days_online_per_supplier END) AS cy_avg_days_online_per_supplier,
  MAX(CASE WHEN period = 'LY' THEN avg_days_online_per_supplier END) AS ly_avg_days_online_per_supplier,
  MAX(CASE WHEN period = 'CY' THEN avg_days_online_per_active_supplier END) AS cy_avg_days_online_per_active_supplier,
  MAX(CASE WHEN period = 'LY' THEN avg_days_online_per_active_supplier END) AS ly_avg_days_online_per_active_supplier

FROM indexed
GROUP BY 1, 2
ORDER BY 1, 2;
