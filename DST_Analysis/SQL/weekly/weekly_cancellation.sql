CREATE OR REPLACE TABLE production.supply_analytics.dst_cancellation_weekly_dataset AS

WITH params AS (
  SELECT 
    DATE '2025-01-01' AS yoy_pre_start_date,
    DATE '2025-01-15' AS yoy_pre_end_date,
    DATE '2025-01-16' AS yoy_event_week_start,
    DATE '2025-01-30' AS yoy_post_end_date,
    DATE '2026-01-01' AS event_pre_start_date,
    DATE '2026-01-15' AS event_pre_end_date,
    DATE '2026-01-16' AS event_week_start_str,
    DATE '2026-01-30' AS event_post_end_date,
    ARRAY('Germany') AS countries,
    ARRAY() AS tour_ids
),

cancellation_base AS (
  SELECT
    fb.booking_id,
    fb.tour_id,
    tour.user_id AS supplier_id,
    dl.country_name,
    fb.nr,
    fb.gmv,
    fb.tickets,
    COALESCE(fb.status_id, -1) AS status_id,
    CAST(fb.date_of_checkout AS date) AS dt_checkout,
    CAST(fb.date_of_travel AS date) AS dt_travel,
    date_trunc('week', CAST(fb.date_of_checkout AS date)) AS week_start,
    CASE
      WHEN CAST(fb.date_of_checkout AS date) BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date THEN 'LY'
      WHEN CAST(fb.date_of_checkout AS date) BETWEEN params.event_pre_start_date AND params.event_post_end_date THEN 'CY'
    END AS period,
    CASE
      WHEN CAST(fb.date_of_checkout AS date) BETWEEN params.yoy_pre_start_date AND params.yoy_pre_end_date THEN 'Pre'
      WHEN CAST(fb.date_of_checkout AS date) BETWEEN params.yoy_event_week_start AND params.yoy_post_end_date THEN 'Post'
      WHEN CAST(fb.date_of_checkout AS date) BETWEEN params.event_pre_start_date AND params.event_pre_end_date THEN 'Pre'
      WHEN CAST(fb.date_of_checkout AS date) BETWEEN params.event_week_start_str AND params.event_post_end_date THEN 'Post'
    END AS phase,
    CASE
      WHEN CAST(fb.date_of_checkout AS date) BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date
        THEN CAST(datediff(date_trunc('week', CAST(fb.date_of_checkout AS date)), date_trunc('week', params.yoy_event_week_start)) / 7 AS INT)
      WHEN CAST(fb.date_of_checkout AS date) BETWEEN params.event_pre_start_date AND params.event_post_end_date
        THEN CAST(datediff(date_trunc('week', CAST(fb.date_of_checkout AS date)), date_trunc('week', params.event_week_start_str)) / 7 AS INT)
    END AS week_index,
    CASE
      WHEN CAST(fb.date_of_checkout AS date) BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date
        THEN params.yoy_event_week_start
      ELSE params.event_week_start_str
    END AS event_start
  FROM production.dwh.fact_booking_v2 AS fb
  CROSS JOIN params
  INNER JOIN production.dwh.dim_status AS s
    ON COALESCE(fb.status_id, -1) = s.status_id
  LEFT JOIN production.dwh.dim_tour AS tour
    ON fb.tour_id = tour.tour_id
  LEFT JOIN production.dwh.dim_location AS dl
    ON tour.location_id = dl.location_id
  WHERE
    s.status_display IN ('Active', 'Cancelled')
    AND (SIZE(params.countries) = 0 OR ARRAY_CONTAINS(params.countries, dl.country_name))
    AND (
      CAST(fb.date_of_checkout AS date) BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date
      OR CAST(fb.date_of_checkout AS date) BETWEEN params.event_pre_start_date AND params.event_post_end_date
    )
    AND (
      SIZE(params.tour_ids) = 0
      OR fb.tour_id IN (SELECT tour_id FROM params LATERAL VIEW explode(params.tour_ids) e AS tour_id)
    )
)

SELECT
  country_name,
  period,
  phase,
  week_start,
  week_index,

  COUNT(DISTINCT booking_id)                                                    AS total_bookings,
  COUNT(DISTINCT CASE WHEN status_id = 2 THEN booking_id END)                  AS cancelled_bookings,
  COUNT(DISTINCT CASE WHEN status_id = 2 THEN booking_id END)
    / NULLIF(COUNT(DISTINCT booking_id), 0)                                     AS cancellation_rate,
  SUM(nr)                                                                       AS total_nr,
  SUM(CASE WHEN status_id = 2 THEN nr ELSE 0 END)                              AS cancelled_nr,
  SUM(CASE WHEN status_id = 2 THEN nr ELSE 0 END)
    / NULLIF(SUM(nr), 0)                                                        AS cancellation_rate_nr,

  COUNT(DISTINCT CASE WHEN dt_travel BETWEEN event_start AND ADD_MONTHS(event_start, 3)
    THEN booking_id END)                                                        AS bookings_3m,
  COUNT(DISTINCT CASE WHEN dt_travel BETWEEN event_start AND ADD_MONTHS(event_start, 3)
    AND status_id = 2 THEN booking_id END)                                      AS cancelled_bookings_3m,
  COUNT(DISTINCT CASE WHEN dt_travel BETWEEN event_start AND ADD_MONTHS(event_start, 3)
    AND status_id = 2 THEN booking_id END)
    / NULLIF(COUNT(DISTINCT CASE WHEN dt_travel BETWEEN event_start AND ADD_MONTHS(event_start, 3)
    THEN booking_id END), 0)                                                    AS cancellation_rate_3m,

  COUNT(DISTINCT CASE WHEN dt_travel BETWEEN event_start AND ADD_MONTHS(event_start, 6)
    THEN booking_id END)                                                        AS bookings_6m,
  COUNT(DISTINCT CASE WHEN dt_travel BETWEEN event_start AND ADD_MONTHS(event_start, 6)
    AND status_id = 2 THEN booking_id END)                                      AS cancelled_bookings_6m,
  COUNT(DISTINCT CASE WHEN dt_travel BETWEEN event_start AND ADD_MONTHS(event_start, 6)
    AND status_id = 2 THEN booking_id END)
    / NULLIF(COUNT(DISTINCT CASE WHEN dt_travel BETWEEN event_start AND ADD_MONTHS(event_start, 6)
    THEN booking_id END), 0)                                                    AS cancellation_rate_6m,

  COUNT(DISTINCT tour_id)                                                       AS total_tours,
  COUNT(DISTINCT supplier_id)                                                   AS total_suppliers

FROM cancellation_base
WHERE period IS NOT NULL AND phase IS NOT NULL
GROUP BY country_name, period, phase, week_start, week_index
ORDER BY country_name, period, week_index;
