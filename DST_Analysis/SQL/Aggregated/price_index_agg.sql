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
      -- CY
      WHEN pb.snapshot_dt >= date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
       AND pb.snapshot_dt <  dr.cy_comm_week_start
        THEN 'PRE'
      WHEN pb.snapshot_dt >= dr.cy_comm_week_start
       AND pb.snapshot_dt <  dr.cy_rollout_week_start
        THEN 'COMM'
      WHEN pb.snapshot_dt >= dr.cy_rollout_week_start
       AND pb.snapshot_dt <= date_add(dr.cy_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'POST'

      -- LY
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

    CASE
      WHEN datediff(pb.travel_dt, pb.snapshot_dt) BETWEEN 0 AND 14 THEN '0-14'
      WHEN datediff(pb.travel_dt, pb.snapshot_dt) BETWEEN 15 AND 60 THEN '15-60'
      WHEN datediff(pb.travel_dt, pb.snapshot_dt) BETWEEN 61 AND 180 THEN '61-180'
      ELSE NULL
    END AS lead_time_bucket,

    pb.final_price_red_per_adult,
    pb.final_price_black_per_adult,

    CASE
      WHEN pb.availability_timeslots IS NULL THEN 0
      ELSE size(pb.availability_timeslots)
    END AS timeslot_count

  FROM price_base pb
  CROSS JOIN date_ranges dr
),

weekly_metrics AS (
  SELECT
    period,
    country_name,
    window,
    snapshot_week_start,
    currency_iso_code,
    lead_time_bucket,

    COUNT(*) AS wk_rows,
    COUNT(DISTINCT tour_id) AS wk_tours,
    COUNT(DISTINCT tour_option_id) AS wk_tour_options,

    percentile_approx(final_price_red_per_adult, 0.5)   AS wk_median_red_price_per_adult,
    percentile_approx(final_price_black_per_adult, 0.5) AS wk_median_black_price_per_adult,
    percentile_approx(timeslot_count, 0.5)              AS wk_median_timeslots
  FROM windowed_daily
  WHERE period IS NOT NULL
    AND window IS NOT NULL
    AND lead_time_bucket IS NOT NULL
    AND final_price_red_per_adult IS NOT NULL
    AND final_price_red_per_adult > 0
  GROUP BY 1,2,3,4,5,6
),

window_level AS (
  SELECT
    period,
    country_name,
    window,
    currency_iso_code,
    lead_time_bucket,

    COUNT(DISTINCT snapshot_week_start) AS weeks_in_window,

    AVG(wk_rows)         AS avg_wk_rows,
    AVG(wk_tours)        AS avg_wk_tours,
    AVG(wk_tour_options) AS avg_wk_tour_options,

    percentile_approx(wk_median_red_price_per_adult, 0.5)   AS window_median_red_price_per_adult,
    percentile_approx(wk_median_black_price_per_adult, 0.5) AS window_median_black_price_per_adult,
    percentile_approx(wk_median_timeslots, 0.5)             AS window_median_timeslots
  FROM weekly_metrics
  GROUP BY 1,2,3,4,5
),

pivoted AS (
  SELECT
    period,
    country_name,
    window,
    currency_iso_code,

    MAX(CASE WHEN lead_time_bucket = '0-14'  THEN weeks_in_window END) AS weeks_0_14,
    MAX(CASE WHEN lead_time_bucket = '15-60' THEN weeks_in_window END) AS weeks_15_60,
    MAX(CASE WHEN lead_time_bucket = '61-180' THEN weeks_in_window END) AS weeks_61_180,

    MAX(CASE WHEN lead_time_bucket = '0-14'  THEN window_median_red_price_per_adult END)   AS red_med_0_14,
    MAX(CASE WHEN lead_time_bucket = '15-60' THEN window_median_red_price_per_adult END)   AS red_med_15_60,
    MAX(CASE WHEN lead_time_bucket = '61-180' THEN window_median_red_price_per_adult END)  AS red_med_61_180,

    MAX(CASE WHEN lead_time_bucket = '0-14'  THEN window_median_black_price_per_adult END)  AS black_med_0_14,
    MAX(CASE WHEN lead_time_bucket = '15-60' THEN window_median_black_price_per_adult END)  AS black_med_15_60,
    MAX(CASE WHEN lead_time_bucket = '61-180' THEN window_median_black_price_per_adult END) AS black_med_61_180,

    MAX(CASE WHEN lead_time_bucket = '0-14'  THEN window_median_timeslots END) AS timeslots_med_0_14,
    MAX(CASE WHEN lead_time_bucket = '15-60' THEN window_median_timeslots END) AS timeslots_med_15_60,
    MAX(CASE WHEN lead_time_bucket = '61-180' THEN window_median_timeslots END) AS timeslots_med_61_180

  FROM window_level
  GROUP BY 1,2,3,4
)

SELECT *
FROM pivoted
ORDER BY
  period,
  country_name,
  CASE window WHEN 'PRE' THEN 1 WHEN 'COMM' THEN 2 WHEN 'POST' THEN 3 ELSE 9 END,
  currency_iso_code;
