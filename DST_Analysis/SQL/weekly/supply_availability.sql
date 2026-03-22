WITH v_inputs AS (
  SELECT
    CAST(:comm_date AS DATE)      AS comm_date,
    CAST(:rollout_date AS DATE)   AS rollout_date,
    CAST(:start_date AS DATE)     AS start_date,
    CAST(:end_date AS DATE)       AS end_date,
    CAST(:country AS STRING)      AS country,
    CAST(:tour_ids AS STRING) AS tour_ids
),
tour_filter AS (
  SELECT CAST(trim(x) AS BIGINT) AS tour_id
  FROM v_inputs p
  LATERAL VIEW explode(split(p.tour_ids, ',')) e AS x
  WHERE trim(x) <> ''
),

-- Compute the CY and LY date ranges
date_ranges AS (
  SELECT
    
    -- CY (this year)
    CAST(date_trunc('week', CAST(pr.comm_date    AS timestamp)) AS date) AS cy_comm_week_start,
    CAST(date_trunc('week', CAST(pr.rollout_date AS timestamp)) AS date) AS cy_rollout_week_start,
    CAST(date_trunc('week', CAST(pr.start_date   AS timestamp)) AS date) AS cy_start_week,
    date_add(CAST(date_trunc('week', CAST(pr.end_date AS timestamp)) AS date), 6) AS cy_end_week,

    -- LY (last year, shifted -12 months)
    CAST(date_trunc('week', CAST(add_months(pr.comm_date,    -12) AS timestamp)) AS date) AS ly_comm_week_start,
    CAST(date_trunc('week', CAST(add_months(pr.rollout_date, -12) AS timestamp)) AS date) AS ly_rollout_week_start,
    CAST(date_trunc('week', CAST(add_months(pr.start_date,   -12) AS timestamp)) AS date) AS ly_start_week,
    date_add(CAST(date_trunc('week', CAST(add_months(pr.end_date, -12) AS timestamp)) AS date), 6) AS ly_end_week,
    
    pr.country
  FROM v_inputs pr
),

history_filtered AS (
  SELECT
    dr.country AS country_name,
    h.tour_id,
    h.supplier_id, 
    CAST(h.date_id AS date) AS dt,
    CASE 
      WHEN CAST(h.date_id AS date) BETWEEN dr.cy_start_week AND dr.cy_end_week THEN 'CY'
      WHEN CAST(h.date_id AS date) BETWEEN dr.ly_start_week AND dr.ly_end_week THEN 'LY'
    END AS period,
    CAST(h.is_online AS int) AS is_online_day
  FROM production.supply.fact_tour_review_history h
  CROSS JOIN date_ranges dr
  INNER JOIN tour_filter tf
    ON h.tour_id = tf.tour_id  
  INNER JOIN dwh.dim_location dl
    ON dl.location_id = h.location_id
  WHERE (
    (CAST(h.date_id AS date) BETWEEN dr.cy_start_week AND dr.cy_end_week)
    OR 
    (CAST(h.date_id AS date) BETWEEN dr.ly_start_week AND dr.ly_end_week)
  )
),

-- All tours in scope (for base denominators)
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

tour_week AS (
  SELECT
    period,
    CAST(date_trunc('week', CAST(dt AS timestamp)) AS date) AS week_start,
    country_name,
    tour_id,
    SUM(is_online_day) AS online_days
  FROM tours_in_scope
  GROUP BY 1, 2, 3, 4
),

supplier_day AS (
  SELECT
    period,
    country_name,
    supplier_id,
    dt,
    MAX(is_online_day) AS supplier_online_day
  FROM tours_in_scope
  GROUP BY 1, 2, 3, 4
),

supplier_week AS (
  SELECT
    period,
    CAST(date_trunc('week', CAST(dt AS timestamp)) AS date) AS week_start,
    country_name,
    supplier_id,
    COUNT(DISTINCT CASE WHEN supplier_online_day = 1 THEN dt END) AS online_days
  FROM supplier_day
  GROUP BY 1, 2, 3, 4
),

tour_week_agg AS (
  SELECT
    period,
    week_start,
    country_name,
    COUNT(DISTINCT CASE WHEN online_days > 0 THEN tour_id END) AS active_tours,
    SUM(online_days) AS sum_online_days_tours,
    SUM(CASE WHEN online_days > 0 THEN online_days END) AS sum_online_days_active_tours
  FROM tour_week
  GROUP BY 1, 2, 3
),

supplier_week_agg AS (
  SELECT
    period,
    week_start,
    country_name,
    COUNT(DISTINCT CASE WHEN online_days > 0 THEN supplier_id END) AS active_suppliers,
    SUM(online_days) AS sum_online_days_suppliers,
    SUM(CASE WHEN online_days > 0 THEN online_days END) AS sum_online_days_active_suppliers
  FROM supplier_week
  GROUP BY 1, 2, 3
),

base_values AS (
  SELECT
    period,
    country_name,
    COUNT(DISTINCT tour_id) AS total_tours,
    COUNT(DISTINCT supplier_id) AS total_suppliers
  FROM tours_in_scope
  GROUP BY 1, 2
),

base AS (
  SELECT
    twa.period,
    twa.week_start,
    twa.country_name,

    CASE
      WHEN twa.period = 'CY' THEN
        CASE
          WHEN twa.week_start < dr.cy_comm_week_start THEN '1.Pre'
          WHEN twa.week_start < dr.cy_rollout_week_start THEN '2.Transition'
          ELSE '3.Post'
        END
      ELSE
        CASE
          WHEN twa.week_start < dr.ly_comm_week_start THEN '1.Pre'
          WHEN twa.week_start < dr.ly_rollout_week_start THEN '2.Transition'
          ELSE '3.Post'
        END
    END AS phase,

    CASE
      WHEN twa.period = 'CY' THEN CAST(datediff(twa.week_start, dr.cy_comm_week_start) / 7 AS INT)
      ELSE CAST(datediff(twa.week_start, dr.ly_comm_week_start) / 7 AS INT)
    END AS week_index,

    twa.active_tours,
    swa.active_suppliers,
    bv.total_tours,
    bv.total_suppliers,

    twa.sum_online_days_tours / NULLIF(bv.total_tours, 0) AS avg_days_online_per_tour,
    swa.sum_online_days_suppliers / NULLIF(bv.total_suppliers, 0) AS avg_days_online_per_supplier,

    twa.sum_online_days_active_tours / NULLIF(twa.active_tours, 0) AS avg_days_online_per_active_tour,
    swa.sum_online_days_active_suppliers / NULLIF(swa.active_suppliers, 0) AS avg_days_online_per_active_supplier,

    twa.active_tours / NULLIF(bv.total_tours, 0) AS share_active_tours,
    swa.active_suppliers / NULLIF(bv.total_suppliers, 0) AS share_active_suppliers

  FROM tour_week_agg twa
  LEFT JOIN supplier_week_agg swa
    ON swa.period = twa.period
    AND swa.week_start = twa.week_start
    AND swa.country_name = twa.country_name
  JOIN base_values bv
    ON bv.period = twa.period
    AND bv.country_name = twa.country_name
  CROSS JOIN date_ranges dr
)

SELECT
  period,
  week_start,
  week_index,
  phase,
  country_name,

  active_tours,
  active_suppliers,
  total_tours,
  total_suppliers,

  share_active_tours,
  share_active_suppliers,

  avg_days_online_per_tour,
  avg_days_online_per_supplier,
  avg_days_online_per_active_tour,
  avg_days_online_per_active_supplier
FROM base
ORDER BY country_name, period, week_start;

