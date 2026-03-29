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

session_base AS (
  SELECT
    tour_cr.tour_id,
    tour.user_id                          AS supplier_id,
    dl.country_name,
    tour_cr.date_id,
    date_trunc('week', tour_cr.date_id)   AS week_start,
    tour_cr.bookings,
    tour_cr.clicks,
    tour_cr.customers,
    tour_cr.visitors,
    tour_cr.impressions,
    tour_cr.transactions,
    tour_cr.add_to_cart_visitors,
    tour_cr.unavailability_visitors,
    tour_cr.stars,
    tour_cr.reviews,
    CASE
      WHEN tour_cr.date_id BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date THEN 'LY'
      WHEN tour_cr.date_id BETWEEN params.event_pre_start_date AND params.event_post_end_date THEN 'CY'
    END AS period,
    CASE
      WHEN tour_cr.date_id BETWEEN params.yoy_pre_start_date AND params.yoy_pre_end_date THEN 'Pre'
      WHEN tour_cr.date_id BETWEEN params.yoy_event_week_start AND params.yoy_post_end_date THEN 'Post'
      WHEN tour_cr.date_id BETWEEN params.event_pre_start_date AND params.event_pre_end_date THEN 'Pre'
      WHEN tour_cr.date_id BETWEEN params.event_week_start_str AND params.event_post_end_date THEN 'Post'
    END AS phase,
    CASE
      WHEN tour_cr.date_id BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date
        THEN CAST(datediff(date_trunc('week', tour_cr.date_id), date_trunc('week', params.yoy_event_week_start)) / 7 AS INT)
      WHEN tour_cr.date_id BETWEEN params.event_pre_start_date AND params.event_post_end_date
        THEN CAST(datediff(date_trunc('week', tour_cr.date_id), date_trunc('week', params.event_week_start_str)) / 7 AS INT)
    END AS week_index
  FROM production.marketplace.agg_tour_cr AS tour_cr
  CROSS JOIN params
  LEFT JOIN production.dwh.dim_tour AS tour
    ON tour_cr.tour_id = tour.tour_id
  LEFT JOIN production.dwh.dim_location AS dl
    ON tour.location_id = dl.location_id
  WHERE
    (SIZE(params.countries) = 0 OR ARRAY_CONTAINS(params.countries, dl.country_name))
    AND (
      tour_cr.date_id BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date
      OR tour_cr.date_id BETWEEN params.event_pre_start_date AND params.event_post_end_date
    )
    AND (
      SIZE(params.tour_ids) = 0
      OR tour_cr.tour_id IN (SELECT tour_id FROM params LATERAL VIEW explode(params.tour_ids) e AS tour_id)
    )
    AND tour_cr.tour_id IS NOT NULL
)

SELECT
  country_name,
  period,
  phase,
  week_start,
  week_index,

  COALESCE(SUM(bookings), 0)                           AS bookings,
  COALESCE(SUM(clicks), 0)                             AS clicks,
  COALESCE(SUM(customers), 0)                          AS customers,
  COALESCE(SUM(visitors), 0)                           AS visitors,
  COALESCE(SUM(impressions), 0)                        AS impressions,
  COALESCE(SUM(add_to_cart_visitors), 0)               AS add_to_cart_visitors,
  COALESCE(SUM(unavailability_visitors), 0)            AS unavailability_visitors,
  COALESCE(SUM(stars), 0)                              AS total_stars,
  COALESCE(SUM(reviews), 0)                            AS total_reviews,

  COUNT(DISTINCT tour_id)                              AS total_tours,
  COUNT(DISTINCT supplier_id)                          AS total_suppliers,

  COALESCE(SUM(customers), 0)
    / NULLIF(COALESCE(SUM(visitors), 0), 0)            AS conversion_rate,

  LEAST(COALESCE(SUM(impressions), 0), COALESCE(SUM(clicks), 0))
    / NULLIF(COALESCE(SUM(impressions), 0), 0)         AS click_through_rate,

  LEAST(COALESCE(SUM(add_to_cart_visitors), 0), COALESCE(SUM(visitors), 0))
    / NULLIF(COALESCE(SUM(visitors), 0), 0)            AS add_to_cart_rate,

  LEAST(COALESCE(SUM(unavailability_visitors), 0), COALESCE(SUM(visitors), 0))
    / NULLIF(COALESCE(SUM(visitors), 0), 0)            AS unavailability_issue_rate,

  COALESCE(SUM(stars), 0)
    / NULLIF(COALESCE(SUM(reviews), 0), 0)             AS avg_star_rating,

  SUM(bookings)    / NULLIF(SUM(clicks), 0)            AS bookings_per_click,
  SUM(impressions) / NULLIF(COUNT(DISTINCT tour_id), 0) AS avg_impressions_per_tour

FROM session_base
WHERE period IS NOT NULL AND phase IS NOT NULL
GROUP BY country_name, period, phase, week_start, week_index
ORDER BY country_name, period, week_index;
