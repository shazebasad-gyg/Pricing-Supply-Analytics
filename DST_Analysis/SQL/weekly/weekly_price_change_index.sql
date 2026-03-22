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

/* Parse comma-separated tour_ids like "62214,153354,195566,562279" */
tour_filter AS (
  SELECT CAST(trim(tour_id_str) AS BIGINT) AS tour_id
  FROM v_inputs
  LATERAL VIEW explode(split(tour_ids, ',')) t AS tour_id_str
),

date_ranges AS (
  SELECT
    -- CY anchors (week starts)
    CAST(date_trunc('week', CAST(i.comm_date    AS timestamp)) AS date) AS cy_comm_week_start,
    CAST(date_trunc('week', CAST(i.rollout_date AS timestamp)) AS date) AS cy_rollout_week_start,

    -- LY anchors (shift -12 months then week-trunc)
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
    p.snapshot_date,
    p.currency_iso_code,
    p.tour_id,
    p.tour_option_id,
    p.travel_date,
    p.pricing_id,
    p.price_category_id,
    p.price_ticket_category,
    p.price_class,
    p.final_price_black_per_adult,
    p.final_price_red_per_adult,
    p.final_price_black,
    p.final_price_red,
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
      /* CY: x_weeks window around comm/rollout + bounded by start/end */
      (
        CAST(p.snapshot_date AS date) BETWEEN date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
                                        AND date_add(dr.cy_rollout_week_start, 7 * dr.x_weeks + 6)
        AND CAST(p.snapshot_date AS date) BETWEEN dr.start_date AND dr.end_date
      )
      OR
      /* LY: same pattern with LY anchors + bounded by shifted start/end */
      (
        CAST(p.snapshot_date AS date) BETWEEN date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
                                        AND date_add(dr.ly_rollout_week_start, 7 * dr.x_weeks + 6)
        AND CAST(p.snapshot_date AS date) BETWEEN add_months(dr.start_date, -12) AND add_months(dr.end_date, -12)
      )
    )
),

windowed AS (
  SELECT
    pb.country_name,
    CAST(date_trunc('week', CAST(pb.snapshot_date AS timestamp)) AS date) AS snapshot_week_start,

    pb.currency_iso_code,
    pb.tour_id,
    pb.tour_option_id,
    pb.travel_date,

    datediff(CAST(pb.travel_date AS date), CAST(pb.snapshot_date AS date)) AS days_ahead,

    CASE
      WHEN CAST(pb.snapshot_date AS date) BETWEEN date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
                                           AND date_add(dr.cy_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'CY'
      WHEN CAST(pb.snapshot_date AS date) BETWEEN date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
                                           AND date_add(dr.ly_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'LY'
    END AS period,

    CASE
      -- CY windows
      WHEN CAST(pb.snapshot_date AS date) >= date_add(dr.cy_comm_week_start, -7 * dr.x_weeks)
       AND CAST(pb.snapshot_date AS date) <  dr.cy_comm_week_start
        THEN 'PRE'
      WHEN CAST(pb.snapshot_date AS date) >= dr.cy_comm_week_start
       AND CAST(pb.snapshot_date AS date) <  dr.cy_rollout_week_start
        THEN 'COMM'
      WHEN CAST(pb.snapshot_date AS date) >= dr.cy_rollout_week_start
       AND CAST(pb.snapshot_date AS date) <= date_add(dr.cy_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'POST'

      -- LY windows
      WHEN CAST(pb.snapshot_date AS date) >= date_add(dr.ly_comm_week_start, -7 * dr.x_weeks)
       AND CAST(pb.snapshot_date AS date) <  dr.ly_comm_week_start
        THEN 'PRE'
      WHEN CAST(pb.snapshot_date AS date) >= dr.ly_comm_week_start
       AND CAST(pb.snapshot_date AS date) <  dr.ly_rollout_week_start
        THEN 'COMM'
      WHEN CAST(pb.snapshot_date AS date) >= dr.ly_rollout_week_start
       AND CAST(pb.snapshot_date AS date) <= date_add(dr.ly_rollout_week_start, 7 * dr.x_weeks + 6)
        THEN 'POST'
    END AS window,

    CASE
      WHEN datediff(CAST(pb.travel_date AS date), CAST(pb.snapshot_date AS date)) BETWEEN 0 AND 14 THEN '0-14'
      WHEN datediff(CAST(pb.travel_date AS date), CAST(pb.snapshot_date AS date)) BETWEEN 15 AND 60 THEN '15-60'
      WHEN datediff(CAST(pb.travel_date AS date), CAST(pb.snapshot_date AS date)) BETWEEN 61 AND 180 THEN '61-180'
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
)

SELECT
  period,
  country_name,
  window,
  snapshot_week_start,
  currency_iso_code,
  lead_time_bucket,

  /* Coverage / sanity checks */
  COUNT(*) AS snapshot_rows,
  COUNT(DISTINCT tour_option_id) AS tour_options,

  /* Weekly prices (aggregated from daily snapshots) */
  percentile_approx(final_price_red_per_adult, 0.5)   AS median_red_price_per_adult,
  percentile_approx(final_price_black_per_adult, 0.5) AS median_black_price_per_adult,

  /* Optional: weekly availability proxy */
  percentile_approx(timeslot_count, 0.5) AS median_timeslots

FROM windowed
WHERE period IS NOT NULL
  AND window IS NOT NULL
  AND lead_time_bucket IS NOT NULL
  AND final_price_red_per_adult IS NOT NULL
  AND final_price_red_per_adult > 0
GROUP BY 1,2,3,4,5,6
ORDER BY
  period,
  country_name,
  snapshot_week_start,
  CASE lead_time_bucket WHEN '0-14' THEN 1 WHEN '15-60' THEN 2 WHEN '61-180' THEN 3 ELSE 9 END,
  CASE window WHEN 'PRE' THEN 1 WHEN 'COMM' THEN 2 WHEN 'POST' THEN 3 ELSE 9 END;
