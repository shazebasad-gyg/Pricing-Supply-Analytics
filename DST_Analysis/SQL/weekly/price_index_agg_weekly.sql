-- Weekly counterpart to Aggregated/price_index_agg.sql: same DST windows (PRE/COMM/POST), CY/LY,
-- but one row per week_offset from rollout with cy_* / ly_* pivoted for event-study plots.
-- Filtered to 0-14 day lead-time bucket for short-term price signal.
-- Params: :comm_date, :rollout_date, :start_date, :end_date, :x_weeks, :country, :tour_ids

WITH v_inputs AS (
  SELECT
    CAST(:comm_date    AS DATE)   AS comm_date,
    CAST(:rollout_date AS DATE)   AS rollout_date,
    CAST(:start_date   AS DATE)   AS start_date,
    CAST(:end_date     AS DATE)   AS end_date,
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
    CAST(date_trunc('week', CAST(i.comm_date    AS timestamp)) AS date) AS cy_comm_week_start,
    CAST(date_trunc('week', CAST(i.rollout_date AS timestamp)) AS date) AS cy_rollout_week_start,

    CAST(date_trunc('week', CAST(add_months(i.comm_date,    -12) AS timestamp)) AS date) AS ly_comm_week_start,
    CAST(date_trunc('week', CAST(add_months(i.rollout_date, -12) AS timestamp)) AS date) AS ly_rollout_week_start,

    i.start_date,
    i.end_date,
    i.x_weeks,
    i.country
  FROM v_inputs i
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

    dl.country_name
  FROM production.dwh.daily_tour_price_snapshot p
  INNER JOIN production.dwh.dim_tour dt
    ON p.tour_id = dt.tour_id
  INNER JOIN production.dwh.dim_location dl
    ON dt.location_id = dl.location_id
  INNER JOIN tour_filter tf
    ON p.tour_id = tf.tour_id
  CROSS JOIN date_ranges dr
  WHERE
    upper(dl.country_name) = upper(dr.country)
    AND (
      (
        CAST(p.snapshot_date AS date) BETWEEN date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
                                        AND date_add(dr.cy_rollout_week_start, 7 * dr.x_weeks + 6)
        AND CAST(p.snapshot_date AS date) BETWEEN dr.start_date AND dr.end_date
      )
      OR
      (
        CAST(p.snapshot_date AS date) BETWEEN date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
                                        AND date_add(dr.ly_rollout_week_start, 7 * dr.x_weeks + 6)
        AND CAST(p.snapshot_date AS date) BETWEEN add_months(dr.start_date, -12) AND add_months(dr.end_date, -12)
      )
    )
),

windowed_daily AS (
  SELECT
    pb.country_name,
    CAST(date_trunc('week', CAST(pb.snapshot_dt AS timestamp)) AS date) AS snapshot_week_start,

    pb.currency_iso_code,
    pb.tour_id,
    pb.tour_option_id,

    pb.snapshot_dt,
    pb.travel_dt,

    datediff(pb.travel_dt, pb.snapshot_dt) AS days_ahead,

    CASE
      WHEN pb.snapshot_dt BETWEEN date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
                            AND date_add(dr.cy_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'CY'
      WHEN pb.snapshot_dt BETWEEN date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
                            AND date_add(dr.ly_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'LY'
    END AS period,

    CASE
      WHEN pb.snapshot_dt >= date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
       AND pb.snapshot_dt <  dr.cy_comm_week_start
        THEN 'PRE'
      WHEN pb.snapshot_dt >= dr.cy_comm_week_start
       AND pb.snapshot_dt <  dr.cy_rollout_week_start
        THEN 'COMM'
      WHEN pb.snapshot_dt >= dr.cy_rollout_week_start
       AND pb.snapshot_dt <= date_add(dr.cy_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'POST'

      WHEN pb.snapshot_dt >= date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
       AND pb.snapshot_dt <  dr.ly_comm_week_start
        THEN 'PRE'
      WHEN pb.snapshot_dt >= dr.ly_comm_week_start
       AND pb.snapshot_dt <  dr.ly_rollout_week_start
        THEN 'COMM'
      WHEN pb.snapshot_dt >= dr.ly_rollout_week_start
       AND pb.snapshot_dt <= date_add(dr.ly_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'POST'
    END AS window,

    pb.final_price_red_per_adult,
    pb.final_price_black_per_adult,

    CASE
      WHEN pb.availability_timeslots IS NULL THEN 0
      ELSE size(pb.availability_timeslots)
    END AS timeslot_count

  FROM price_base pb
  CROSS JOIN date_ranges dr
),

weekly AS (
  SELECT
    country_name,
    period,
    window,
    snapshot_week_start AS week_start,

    COUNT(DISTINCT tour_option_id) AS tour_options,

    percentile_approx(final_price_red_per_adult, 0.5)   AS median_red_price_per_adult,
    percentile_approx(final_price_black_per_adult, 0.5) AS median_black_price_per_adult,
    percentile_approx(timeslot_count, 0.5)              AS median_timeslots

  FROM windowed_daily
  WHERE period IS NOT NULL
    AND window IS NOT NULL
    AND datediff(travel_dt, snapshot_dt) BETWEEN 0 AND 14
    AND final_price_red_per_adult IS NOT NULL
    AND final_price_red_per_adult > 0
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

  MAX(CASE WHEN period = 'CY' THEN tour_options END) AS cy_tour_options,
  MAX(CASE WHEN period = 'LY' THEN tour_options END) AS ly_tour_options,

  MAX(CASE WHEN period = 'CY' THEN median_red_price_per_adult END) AS cy_median_red_price,
  MAX(CASE WHEN period = 'LY' THEN median_red_price_per_adult END) AS ly_median_red_price,

  MAX(CASE WHEN period = 'CY' THEN median_black_price_per_adult END) AS cy_median_black_price,
  MAX(CASE WHEN period = 'LY' THEN median_black_price_per_adult END) AS ly_median_black_price,

  MAX(CASE WHEN period = 'CY' THEN median_timeslots END) AS cy_median_timeslots,
  MAX(CASE WHEN period = 'LY' THEN median_timeslots END) AS ly_median_timeslots

FROM indexed
GROUP BY 1, 2
ORDER BY 1, 2;
