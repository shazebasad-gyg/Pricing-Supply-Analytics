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

supplier_ids AS (
  SELECT DISTINCT
    tour.user_id AS supplier_id
  FROM production.dwh.dim_tour AS tour
  CROSS JOIN params
  WHERE ARRAY_CONTAINS(params.tour_ids, tour.tour_id)
),

accuracy_status AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY TOUR_OPTION_ID, DATE_TRUNC('week', snapshot_date::date)
        ORDER BY snapshot_date DESC
      ) AS rn
    FROM production.supply.DIM_COMPETITOR_PRICE_GAP_STATUS_HISTORY
  )
  WHERE rn = 1
),

parity_base AS (
  SELECT
    tour.tour_id,
    tour.user_id AS supplier_id,
    dl.country_name,
    date_trunc('week', cpi.SNAPSHOT_DATE) AS week_start,
    CASE 
      WHEN cpi.SNAPSHOT_DATE BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date THEN 'LY'
      WHEN cpi.SNAPSHOT_DATE BETWEEN params.event_pre_start_date AND params.event_post_end_date THEN 'CY'
    END AS period,
    CASE 
      WHEN cpi.SNAPSHOT_DATE BETWEEN params.yoy_pre_start_date AND params.yoy_pre_end_date THEN 'Pre'
      WHEN cpi.SNAPSHOT_DATE BETWEEN params.yoy_event_week_start AND params.yoy_post_end_date THEN 'Post'
      WHEN cpi.SNAPSHOT_DATE BETWEEN params.event_pre_start_date AND params.event_pre_end_date THEN 'Pre'
      WHEN cpi.SNAPSHOT_DATE BETWEEN params.event_week_start_str AND params.event_post_end_date THEN 'Post'
    END AS phase,
    CASE
      WHEN cpi.SNAPSHOT_DATE BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date
        THEN CAST(datediff(date_trunc('week', cpi.SNAPSHOT_DATE), date_trunc('week', params.yoy_event_week_start)) / 7 AS INT)
      WHEN cpi.SNAPSHOT_DATE BETWEEN params.event_pre_start_date AND params.event_post_end_date
        THEN CAST(datediff(date_trunc('week', cpi.SNAPSHOT_DATE), date_trunc('week', params.event_week_start_str)) / 7 AS INT)
    END AS week_index,
    cpi.distributed_impressions,
    cpi.SUPPLIER_MEET,
    cpi.SUPPLIER_BEAT,
    cpi.SUPPLIER_PRICE,
    cpi.TIQETS_MEET,
    cpi.TIQETS_BEAT,
    cpi.TIQETS_PRICE,
    cpi.VIATOR_MEET,
    cpi.VIATOR_BEAT,
    cpi.VIATOR_PRICE,
    cpi.HEADOUT_MEET,
    cpi.HEADOUT_BEAT,
    cpi.HEADOUT_PRICE,
    cpi.OVERALL_BML
  FROM production.supply.DIM_PRICING_COMPETITIVENESS AS cpi
  CROSS JOIN params
  LEFT JOIN accuracy_status AS acc
    ON acc.TOUR_OPTION_ID = cpi.TOUR_OPTION_ID
    AND DATE_TRUNC('week', DATE(acc.SNAPSHOT_DATE)) = DATE_TRUNC('week', DATE(cpi.SNAPSHOT_DATE))
  LEFT JOIN production.dwh.dim_tour_option AS tour_option
    ON cpi.TOUR_OPTION_ID = tour_option.tour_option_id
  LEFT JOIN production.dwh.dim_tour AS tour
    ON tour_option.tour_id = tour.tour_id
  LEFT JOIN production.dwh.dim_location AS dl
    ON tour.location_id = dl.location_id
  WHERE
    (
      cpi.SNAPSHOT_DATE BETWEEN params.yoy_pre_start_date AND params.yoy_post_end_date
      OR cpi.SNAPSHOT_DATE BETWEEN params.event_pre_start_date AND params.event_post_end_date
    )
    AND (SIZE(params.countries) = 0 OR ARRAY_CONTAINS(params.countries, dl.country_name))
    AND (cpi.GYG_PRICE <> 0 OR cpi.GYG_PRICE IS NULL)
    AND cpi.GYG_STATUS = 'active'
    AND tour_option.status = 'active'
    AND tour.gyg_status = 'active'
    AND (
      cpi.SUPPLIER_VALID_FOR_COMPETITIVENESS
      OR cpi.TIQETS_VALID_FOR_COMPETITIVENESS
      OR cpi.VIATOR_VALID_FOR_COMPETITIVENESS
      OR cpi.HEADOUT_VALID_FOR_COMPETITIVENESS
    )
    AND (cpi.SUPPLIER_ACCURACY_PREDICTION <> 'Inaccurate' OR cpi.SUPPLIER_ACCURACY_PREDICTION IS NULL)
    AND (cpi.TIQETS_ACCURACY_PREDICTION <> 'Inaccurate' OR cpi.TIQETS_ACCURACY_PREDICTION IS NULL)
    AND (cpi.VIATOR_ACCURACY_PREDICTION <> 'Inaccurate' OR cpi.VIATOR_ACCURACY_PREDICTION IS NULL)
    AND (cpi.HEADOUT_ACCURACY_PREDICTION <> 'Inaccurate' OR cpi.HEADOUT_ACCURACY_PREDICTION IS NULL)
    AND (acc.END_OF_WEEK_SUPPLIER_ACCURACY_STATUS NOT IN ('Inaccurate Data', 'Inaccurate data') OR acc.END_OF_WEEK_SUPPLIER_ACCURACY_STATUS IS NULL)
    AND (acc.END_OF_WEEK_TIQETS_ACCURACY_STATUS NOT IN ('Inaccurate Data', 'Inaccurate data') OR acc.END_OF_WEEK_TIQETS_ACCURACY_STATUS IS NULL)
    AND (acc.END_OF_WEEK_VIATOR_ACCURACY_STATUS NOT IN ('Inaccurate Data', 'Inaccurate data', 'Inacurate data') OR acc.END_OF_WEEK_VIATOR_ACCURACY_STATUS IS NULL)
    AND (acc.END_OF_WEEK_HEADOUT_ACCURACY_STATUS NOT IN ('Inaccurate Data') OR acc.END_OF_WEEK_HEADOUT_ACCURACY_STATUS IS NULL)
    AND (
      SIZE(params.tour_ids) = 0
      OR tour.user_id IN (SELECT supplier_id FROM supplier_ids)
    )
)

SELECT
  country_name,
  period,
  phase,
  week_start,
  week_index,

  COUNT(DISTINCT tour_id) AS total_tours,
  COUNT(DISTINCT supplier_id) AS total_suppliers,

  SUM((SUPPLIER_MEET + SUPPLIER_BEAT) * distributed_impressions)
    / SUM(CASE WHEN SUPPLIER_PRICE IS NOT NULL THEN distributed_impressions ELSE 0 END)
    AS supplier_parity_rate,

  SUM((TIQETS_MEET + TIQETS_BEAT) * distributed_impressions)
    / SUM(CASE WHEN TIQETS_PRICE IS NOT NULL THEN distributed_impressions ELSE 0 END)
    AS tiqets_parity_rate,

  SUM((VIATOR_MEET + VIATOR_BEAT) * distributed_impressions)
    / SUM(CASE WHEN VIATOR_PRICE IS NOT NULL THEN distributed_impressions ELSE 0 END)
    AS viator_parity_rate,

  SUM((HEADOUT_MEET + HEADOUT_BEAT) * distributed_impressions)
    / SUM(CASE WHEN HEADOUT_PRICE IS NOT NULL THEN distributed_impressions ELSE 0 END)
    AS headout_parity_rate,

  SUM(CASE WHEN OVERALL_BML IN ('meet', 'beat') THEN distributed_impressions ELSE 0 END)
    / SUM(distributed_impressions)
    AS overall_parity_rate,

  SUM(distributed_impressions) AS total_impressions

FROM parity_base
WHERE period IS NOT NULL AND phase IS NOT NULL
GROUP BY country_name, period, phase, week_start, week_index
ORDER BY country_name, period, week_index;
