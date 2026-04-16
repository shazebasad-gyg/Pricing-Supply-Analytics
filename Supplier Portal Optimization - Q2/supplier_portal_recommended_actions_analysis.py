# Databricks notebook source
# MAGIC %md
# MAGIC # Supplier Portal Feature Audit: Recommended Actions
# MAGIC
# MAGIC **Author:** shazeb.asad | **Snapshot Date:** April 13, 2026
# MAGIC
# MAGIC **Source:** [Confluence](https://getyourguide.atlassian.net/wiki/spaces/DA/pages/4197253805)
# MAGIC
# MAGIC **Tables used:**
# MAGIC - `production.supply_analytics.dim_recommended_actions` — Primary source; one row per recommended action instance. Key fields: `supplier_id`, `activity_id`, `option_id`, `action_type`, `status`, `surfaced_at`, `resolved_at`, `expired_at`, `dismissed_at`, `dismissal_reason`. Partitioned by `created_date`.
# MAGIC - `production.supply_analytics.dim_supplier_summary` — Supplier master used to define the relevant supplier denominator and classify managed vs. unmanaged and connected vs. non-connected status
# MAGIC - `production.db_mirror_dbz.catalog__tour` — Activity master used to identify activities with active supplier and GYG status, and to determine activities in the activation period
# MAGIC
# MAGIC **Relevant supplier / activity definition (applied to reach queries):**
# MAGIC - Relevant activity: `catalog__tour.supplier_status = 'active'` AND `catalog__tour.gyg_status = 'active'`
# MAGIC - Relevant supplier: `user_status = Active`, has at least one relevant activity, AND (> 1 booking in L365 at supplier level OR any activity first online < 30 days ago)

# COMMAND ----------

SNAPSHOT_DATE  = "2026-04-13"
FEATURE_LAUNCH = "2025-05-15"

# ─── Relevant Activity ───────────────────────────────────────────
# All three must hold:
#   1. Activity supplier status = Active  (dim_tour.status)
#   2. Activity GYG status      = Active  (dim_tour.gyg_status)
#   3. First went online < 30 days ago  OR  received > 1 booking in L365
#      Online status sourced from dim_tour_history.is_online (MIN = first ever online)
#
# ─── Relevant Supplier ──────────────────────────────────────────
# All of the following must hold:
#   1. Supplier status    = Active      (dim_supplier_summary.user_status)
#   2. > 1 booking in L365 at supplier level  OR  any activity first online < 30 days
#   3. Has at least one relevant activity (as defined above)

RELEVANT_SUPPLIERS_CTE = f"""
activity_bookings_l365 AS (
  SELECT
    tour_id,
    COUNT(*) AS bookings_l365
  FROM production.dwh.fact_booking
  WHERE date_of_checkout >= DATE_SUB(DATE '{SNAPSHOT_DATE}', 365)
    AND is_fraud   = FALSE
    AND status_id IN (1, 2)
  GROUP BY tour_id
),
supplier_bookings_l365 AS (
  SELECT
    supplier_id,
    COUNT(*) AS bookings_l365
  FROM production.dwh.fact_booking
  WHERE date_of_checkout >= DATE_SUB(DATE '{SNAPSHOT_DATE}', 365)
    AND is_fraud   = FALSE
    AND status_id IN (1, 2)
  GROUP BY supplier_id
),
activity_first_online AS (
  -- First date each activity had is_online = TRUE in dim_tour_history
  SELECT
    tour_id,
    CAST(MIN(update_timestamp) AS DATE) AS first_online_date
  FROM production.dwh.dim_tour_history
  WHERE is_online = TRUE
  GROUP BY tour_id
),
relevant_activities AS (
  SELECT DISTINCT
    a.tour_id,
    a.user_id AS supplier_id
  FROM production.dwh.dim_tour a
  LEFT JOIN activity_bookings_l365 ab ON a.tour_id = ab.tour_id
  LEFT JOIN activity_first_online  fo ON a.tour_id = fo.tour_id
  WHERE lower(a.status)     = 'active'
    AND lower(a.gyg_status) = 'active'
    AND (
      fo.first_online_date >= DATE_SUB(DATE '{SNAPSHOT_DATE}', 30)
      OR COALESCE(ab.bookings_l365, 0) > 1
    )
),
activation_period_suppliers AS (
  -- Suppliers with any activity that first went online within the last 30 days
  SELECT DISTINCT a.user_id AS supplier_id
  FROM production.dwh.dim_tour a
  INNER JOIN activity_first_online fo ON a.tour_id = fo.tour_id
  WHERE fo.first_online_date >= DATE_SUB(DATE '{SNAPSHOT_DATE}', 30)
),
relevant_suppliers AS (
  SELECT DISTINCT s.supplier_id
  FROM production.supply_analytics.dim_supplier_summary s
  INNER JOIN relevant_activities         ra  ON s.supplier_id = ra.supplier_id
  LEFT  JOIN supplier_bookings_l365      sb  ON s.supplier_id = sb.supplier_id
  LEFT  JOIN activation_period_suppliers aps ON s.supplier_id = aps.supplier_id
  WHERE lower(s.user_status) = 'active'
    AND (
      COALESCE(sb.bookings_l365, 0) > 1
      OR aps.supplier_id IS NOT NULL
    )
)
"""


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q1: How many suppliers is the feature reaching?

# COMMAND ----------

# Table 1.1 — Top-line Reach
df_q1_1 = spark.sql(f"""
WITH
{RELEVANT_SUPPLIERS_CTE},
reached AS (
  SELECT DISTINCT supplier_id
  FROM production.supply_analytics.dim_recommended_actions
  WHERE created_date >= '{FEATURE_LAUNCH}'
    AND surfaced_at  >= '{FEATURE_LAUNCH}'
    AND surfaced_at  <= '{SNAPSHOT_DATE}'
)
SELECT
  COUNT(DISTINCT rs.supplier_id)                                                                                              AS relevant_suppliers,
  COUNT(DISTINCT r.supplier_id)                                                                                               AS reached_suppliers,
  CONCAT(ROUND(100.0 * COUNT(DISTINCT r.supplier_id) / COUNT(DISTINCT rs.supplier_id), 1), '%')                             AS reach_rate
FROM relevant_suppliers rs
LEFT JOIN reached r ON rs.supplier_id = r.supplier_id
""")

display(df_q1_1)

# COMMAND ----------

# Table 1.2 — Reach by Managed vs Unmanaged
df_q1_2 = spark.sql(f"""
WITH
{RELEVANT_SUPPLIERS_CTE},
reached AS (
  SELECT DISTINCT supplier_id
  FROM production.supply_analytics.dim_recommended_actions
  WHERE created_date >= '{FEATURE_LAUNCH}'
    AND surfaced_at  >= '{FEATURE_LAUNCH}'
    AND surfaced_at  <= '{SNAPSHOT_DATE}'
)
SELECT
  s.is_managed                                                                                                                                                AS segment,
  COUNT(DISTINCT rs.supplier_id)                                                                                                                             AS relevant_suppliers,
  COUNT(DISTINCT CASE WHEN r.supplier_id IS NOT NULL THEN rs.supplier_id END)                                                                               AS reached,
  CONCAT(ROUND(100.0 * COUNT(DISTINCT CASE WHEN r.supplier_id IS NOT NULL THEN rs.supplier_id END) / COUNT(DISTINCT rs.supplier_id), 1), '%')               AS reach_rate
FROM relevant_suppliers rs
JOIN  production.supply_analytics.dim_supplier_summary s ON rs.supplier_id = s.supplier_id
LEFT JOIN reached r ON rs.supplier_id = r.supplier_id
GROUP BY s.is_managed
ORDER BY reach_rate DESC
""")

display(df_q1_2)

# COMMAND ----------

# Table 1.2b — Reach by Connected vs Non-Connected
df_q1_2b = spark.sql(f"""
WITH
{RELEVANT_SUPPLIERS_CTE},
reached AS (
  SELECT DISTINCT supplier_id
  FROM production.supply_analytics.dim_recommended_actions
  WHERE created_date >= '{FEATURE_LAUNCH}'
    AND surfaced_at  >= '{FEATURE_LAUNCH}'
    AND surfaced_at  <= '{SNAPSHOT_DATE}'
)
SELECT
  s.is_connected                                                                                                                                              AS segment,
  COUNT(DISTINCT rs.supplier_id)                                                                                                                             AS relevant_suppliers,
  COUNT(DISTINCT CASE WHEN r.supplier_id IS NOT NULL THEN rs.supplier_id END)                                                                               AS reached,
  CONCAT(ROUND(100.0 * COUNT(DISTINCT CASE WHEN r.supplier_id IS NOT NULL THEN rs.supplier_id END) / COUNT(DISTINCT rs.supplier_id), 1), '%')               AS reach_rate
FROM relevant_suppliers rs
JOIN  production.supply_analytics.dim_supplier_summary s ON rs.supplier_id = s.supplier_id
LEFT JOIN reached r ON rs.supplier_id = r.supplier_id
GROUP BY s.is_connected
ORDER BY reach_rate DESC
""")

display(df_q1_2b)

# COMMAND ----------

# Table 1.3 — Reach by Action Type (since feature launch through snapshot date)
df_q1_3 = spark.sql(f"""
SELECT
  action_type,
  COUNT(DISTINCT supplier_id)                                                                        AS suppliers_reached,
  COUNT(*)                                                                                           AS actions_surfaced,
  ROUND(CAST(COUNT(*) AS DOUBLE) / COUNT(DISTINCT supplier_id), 1)                                 AS avg_actions_per_supplier
FROM production.supply_analytics.dim_recommended_actions
WHERE created_date >= '{FEATURE_LAUNCH}'
  AND surfaced_at  >= '{FEATURE_LAUNCH}'
  AND surfaced_at  <= '{SNAPSHOT_DATE}'
GROUP BY action_type
ORDER BY suppliers_reached DESC
""")

display(df_q1_3)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q2: Are suppliers acting on recommendations?

# COMMAND ----------

# Table 2.1 — Overall Status Funnel (309,460 total actions surfaced since launch)
df_q2_1 = spark.sql(f"""
SELECT
  CASE
    WHEN status = 'resolved'  THEN 'Resolved'
    WHEN status = 'expired'   THEN 'Expired'
    WHEN status = 'dismissed' THEN 'Dismissed'
    ELSE                           'Active / In progress'
  END                                                                                               AS status,
  COUNT(*)                                                                                          AS count,
  CONCAT(ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1), '%')                                 AS share_of_surfaced
FROM production.supply_analytics.dim_recommended_actions
WHERE created_date >= '{FEATURE_LAUNCH}'
  AND surfaced_at  >= '{FEATURE_LAUNCH}'
  AND surfaced_at  <= '{SNAPSHOT_DATE}'
GROUP BY status
ORDER BY count DESC
""")

display(df_q2_1)

# COMMAND ----------

# Table 2.1b — Derived Performance Metrics
# Resolution rate (28-day): resolved within 28 days of surfacing / all surfaced. Benchmark: 30%
# Dismissal rate: dismissed / actioned (resolved + dismissed). Benchmark: below 10%
# Expiry rate: expired / closed (resolved + expired + dismissed)
df_q2_1b = spark.sql(f"""
WITH actions AS (
  SELECT
    status,
    surfaced_at,
    resolved_at
  FROM production.supply_analytics.dim_recommended_actions
  WHERE created_date >= '{FEATURE_LAUNCH}'
    AND surfaced_at  >= '{FEATURE_LAUNCH}'
    AND surfaced_at  <= '{SNAPSHOT_DATE}'
),
totals AS (
  SELECT
    COUNT(*)                                                                                                               AS total_surfaced,
    SUM(CASE WHEN status = 'resolved'                                                      THEN 1 ELSE 0 END)             AS resolved_count,
    SUM(CASE WHEN status = 'expired'                                                       THEN 1 ELSE 0 END)             AS expired_count,
    SUM(CASE WHEN status = 'dismissed'                                                     THEN 1 ELSE 0 END)             AS dismissed_count,
    SUM(CASE WHEN status = 'resolved' AND DATEDIFF(resolved_at, surfaced_at) <= 28        THEN 1 ELSE 0 END)             AS resolved_28d
  FROM actions
)
SELECT
  CONCAT(ROUND(100.0 * resolved_28d    / total_surfaced, 1), '%')                                                        AS resolution_rate_28d,
  CONCAT(ROUND(100.0 * dismissed_count / NULLIF(resolved_count + dismissed_count, 0), 1), '%')                           AS dismissal_rate,
  CONCAT(ROUND(100.0 * expired_count   / NULLIF(resolved_count + expired_count + dismissed_count, 0), 1), '%')           AS expiry_rate
FROM totals
""")

display(df_q2_1b)

# COMMAND ----------

# Table 2.2 — Resolution Rate by Action Type
# Overall rate: all actions since launch. 28-day rate: resolved within 28 days of surfacing.
# Dismissal rate: over actioned (resolved + dismissed). Expiry rate: over closed (resolved + expired + dismissed).
df_q2_2 = spark.sql(f"""
SELECT
  action_type,
  COUNT(*)                                                                                                                                                AS surfaced,
  SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END)                                                                                                  AS resolved,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) / COUNT(*), 1), '%')                                                        AS overall_rate,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN status = 'resolved' AND DATEDIFF(resolved_at, surfaced_at) <= 28 THEN 1 ELSE 0 END) / COUNT(*), 1), '%')           AS rate_28d,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN status = 'dismissed' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN status IN ('resolved', 'dismissed') THEN 1 ELSE 0 END), 0), 1), '%')                                  AS dismissal_rate_actioned,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN status = 'expired' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN status IN ('resolved', 'expired', 'dismissed') THEN 1 ELSE 0 END), 0), 1), '%')                         AS expiry_rate_closed,
  ROUND(AVG(CASE WHEN status = 'resolved' AND resolved_at IS NOT NULL THEN DATEDIFF(resolved_at, surfaced_at) END), 1)                                  AS avg_days_to_resolve
FROM production.supply_analytics.dim_recommended_actions
WHERE created_date >= '{FEATURE_LAUNCH}'
  AND surfaced_at  >= '{FEATURE_LAUNCH}'
  AND surfaced_at  <= '{SNAPSHOT_DATE}'
GROUP BY action_type
ORDER BY surfaced DESC
""")

display(df_q2_2)

# COMMAND ----------

# Table 2.3 — Top Dismissal Reasons (among 10,890 dismissed actions)
df_q2_3 = spark.sql(f"""
SELECT
  action_type,
  COALESCE(dismissal_reason, 'None selected')                                                       AS dismissal_reason,
  COUNT(*)                                                                                          AS count
FROM production.supply_analytics.dim_recommended_actions
WHERE created_date >= '{FEATURE_LAUNCH}'
  AND surfaced_at  >= '{FEATURE_LAUNCH}'
  AND surfaced_at  <= '{SNAPSHOT_DATE}'
  AND status = 'dismissed'
GROUP BY action_type, COALESCE(dismissal_reason, 'None selected')
ORDER BY count DESC
LIMIT 20
""")

display(df_q2_3)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q3: What share of reached suppliers have ever resolved at least one action?

# COMMAND ----------

# Table 3.1 — Supplier-Level Conversion
# Base: all suppliers who received at least one action since launch (not restricted to relevant suppliers)
df_q3_1 = spark.sql(f"""
WITH reached AS (
  SELECT DISTINCT supplier_id
  FROM production.supply_analytics.dim_recommended_actions
  WHERE created_date >= '{FEATURE_LAUNCH}'
    AND surfaced_at  >= '{FEATURE_LAUNCH}'
    AND surfaced_at  <= '{SNAPSHOT_DATE}'
),
resolvers AS (
  SELECT DISTINCT supplier_id
  FROM production.supply_analytics.dim_recommended_actions
  WHERE created_date >= '{FEATURE_LAUNCH}'
    AND surfaced_at  >= '{FEATURE_LAUNCH}'
    AND surfaced_at  <= '{SNAPSHOT_DATE}'
    AND status = 'resolved'
)
SELECT
  COUNT(DISTINCT r.supplier_id)                                                                                              AS suppliers_reached,
  COUNT(DISTINCT res.supplier_id)                                                                                            AS resolved_at_least_one,
  CONCAT(ROUND(100.0 * COUNT(DISTINCT res.supplier_id) / COUNT(DISTINCT r.supplier_id), 1), '%')                           AS supplier_conversion_rate,
  COUNT(DISTINCT r.supplier_id) - COUNT(DISTINCT res.supplier_id)                                                          AS never_resolved
FROM reached r
LEFT JOIN resolvers res ON r.supplier_id = res.supplier_id
""")

display(df_q3_1)

# COMMAND ----------

# Table 3.2 — Supplier Conversion by Segment (Managed vs Non-Managed)
df_q3_2 = spark.sql(f"""
WITH reached AS (
  SELECT DISTINCT supplier_id
  FROM production.supply_analytics.dim_recommended_actions
  WHERE created_date >= '{FEATURE_LAUNCH}'
    AND surfaced_at  >= '{FEATURE_LAUNCH}'
    AND surfaced_at  <= '{SNAPSHOT_DATE}'
),
resolvers AS (
  SELECT DISTINCT supplier_id
  FROM production.supply_analytics.dim_recommended_actions
  WHERE created_date >= '{FEATURE_LAUNCH}'
    AND surfaced_at  >= '{FEATURE_LAUNCH}'
    AND surfaced_at  <= '{SNAPSHOT_DATE}'
    AND status = 'resolved'
)
SELECT
  s.is_managed                                                                                                                                             AS segment,
  COUNT(DISTINCT r.supplier_id)                                                                                                                           AS reached,
  COUNT(DISTINCT CASE WHEN res.supplier_id IS NOT NULL THEN r.supplier_id END)                                                                           AS resolved_at_least_one,
  CONCAT(ROUND(100.0 * COUNT(DISTINCT CASE WHEN res.supplier_id IS NOT NULL THEN r.supplier_id END) / COUNT(DISTINCT r.supplier_id), 1), '%')            AS conversion_rate
FROM reached r
JOIN  production.supply_analytics.dim_supplier_summary s  ON r.supplier_id = s.supplier_id
LEFT JOIN resolvers res ON r.supplier_id = res.supplier_id
GROUP BY s.is_managed
ORDER BY conversion_rate DESC
""")

display(df_q3_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q4: How is engagement trending over time?

# COMMAND ----------

# Table 4.1 — Monthly Trend: Volume, Reach, and Resolution
# 28-day rate: preferred cross-month metric; controls for time available to accumulate resolutions.
# Expiry rate: over closed (resolved + expired + dismissed). Dismissal rate: over actioned (resolved + dismissed).
# April 2026 is a partial month (data through 2026-04-13).
df_q4 = spark.sql(f"""
WITH actions AS (
  SELECT
    supplier_id,
    status,
    surfaced_at,
    resolved_at,
    DATE_TRUNC('month', surfaced_at) AS surfaced_month
  FROM production.supply_analytics.dim_recommended_actions
  WHERE created_date >= '{FEATURE_LAUNCH}'
    AND surfaced_at  >= '{FEATURE_LAUNCH}'
    AND surfaced_at  <= '{SNAPSHOT_DATE}'
)
SELECT
  DATE_FORMAT(surfaced_month, 'MMM yyyy')                                                                                                                AS month,
  COUNT(*)                                                                                                                                               AS actions_surfaced,
  COUNT(DISTINCT supplier_id)                                                                                                                            AS suppliers_reached,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN status = 'resolved' AND DATEDIFF(resolved_at, surfaced_at) <= 28 THEN 1 ELSE 0 END) / COUNT(*), 1), '%')          AS res_rate_28d,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) / COUNT(*), 1), '%')                                                       AS overall_res_rate,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN status = 'expired' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN status IN ('resolved', 'expired', 'dismissed') THEN 1 ELSE 0 END), 0), 1), '%')    AS expiry_rate_closed,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN status = 'dismissed' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN status IN ('resolved', 'dismissed') THEN 1 ELSE 0 END), 0), 1), '%')             AS dismissal_rate_actioned
FROM actions
GROUP BY surfaced_month
ORDER BY surfaced_month
""")

display(df_q4)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q5: Do suppliers who resolve one action go on to resolve more?

# COMMAND ----------

# Table 5.1 — Resolution Distribution Among Converting Suppliers
# Average: 4.2 resolved actions per supplier. Median: 2. Maximum: 760.
df_q5 = spark.sql(f"""
WITH resolver_counts AS (
  SELECT
    supplier_id,
    COUNT(*) AS total_resolved
  FROM production.supply_analytics.dim_recommended_actions
  WHERE created_date >= '{FEATURE_LAUNCH}'
    AND surfaced_at  >= '{FEATURE_LAUNCH}'
    AND surfaced_at  <= '{SNAPSHOT_DATE}'
    AND status = 'resolved'
  GROUP BY supplier_id
),
tiered AS (
  SELECT
    CASE
      WHEN total_resolved = 1              THEN 'Single-Use'
      WHEN total_resolved = 2              THEN 'Occasional'
      WHEN total_resolved BETWEEN 3 AND 9  THEN 'Regular'
      ELSE                                      'Power User'
    END AS tier,
    CASE
      WHEN total_resolved = 1              THEN '1'
      WHEN total_resolved = 2              THEN '2'
      WHEN total_resolved BETWEEN 3 AND 9  THEN '3–9'
      ELSE                                      '10+'
    END AS resolved_actions,
    CASE
      WHEN total_resolved = 1              THEN 1
      WHEN total_resolved = 2              THEN 2
      WHEN total_resolved BETWEEN 3 AND 9  THEN 3
      ELSE                                      4
    END AS tier_order
  FROM resolver_counts
)
SELECT
  tier,
  resolved_actions,
  COUNT(*)                                                                                            AS suppliers,
  CONCAT(ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1), '%')                                  AS pct_of_converting_suppliers,
  tier_order
FROM tiered
GROUP BY tier, resolved_actions, tier_order
ORDER BY tier_order
""")

display(df_q5)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q6: When within the action lifecycle do suppliers typically act?

# COMMAND ----------

# Table 6.1 — Time from Surfaced to Resolved
# 31.2% of resolutions happen within the first 3 days (first-session effect).
# 39.0% of resolutions take more than 30 days.
# Days 4–14 (16.7% of resolutions) = natural window for re-engagement nudges.
df_q6 = spark.sql(f"""
WITH resolved_actions AS (
  SELECT
    DATEDIFF(resolved_at, surfaced_at) AS days_to_resolve
  FROM production.supply_analytics.dim_recommended_actions
  WHERE created_date >= '{FEATURE_LAUNCH}'
    AND surfaced_at  >= '{FEATURE_LAUNCH}'
    AND surfaced_at  <= '{SNAPSHOT_DATE}'
    AND status       = 'resolved'
    AND resolved_at  IS NOT NULL
),
bucketed AS (
  SELECT
    CASE
      WHEN days_to_resolve = 0   THEN 'Same day'
      WHEN days_to_resolve <= 3  THEN 'Days 1–3'
      WHEN days_to_resolve <= 7  THEN 'Days 4–7'
      WHEN days_to_resolve <= 14 THEN 'Days 8–14'
      WHEN days_to_resolve <= 30 THEN 'Days 15–30'
      WHEN days_to_resolve <= 60 THEN 'Days 31–60'
      ELSE                            '60+ days'
    END AS time_window,
    CASE
      WHEN days_to_resolve = 0   THEN 1
      WHEN days_to_resolve <= 3  THEN 2
      WHEN days_to_resolve <= 7  THEN 3
      WHEN days_to_resolve <= 14 THEN 4
      WHEN days_to_resolve <= 30 THEN 5
      WHEN days_to_resolve <= 60 THEN 6
      ELSE                            7
    END AS sort_order
  FROM resolved_actions
)
SELECT
  time_window,
  COUNT(*)                                                                                           AS resolved_actions,
  CONCAT(ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1), '%')                                 AS pct_of_resolved,
  CONCAT(ROUND(100.0 * SUM(COUNT(*)) OVER (ORDER BY sort_order ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COUNT(*)) OVER (), 1), '%') AS cumulative_pct,
  sort_order
FROM bucketed
GROUP BY time_window, sort_order
ORDER BY sort_order
""")

display(df_q6)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q7: How deeply engaged are converting suppliers, and do single-use suppliers come back?

# COMMAND ----------

# Table 7.1 — Engagement Tier Distribution
# Snapshot date: 2026-04-13. Among 23,897 converting suppliers.
df_q7_1 = spark.sql(f"""
WITH resolver_counts AS (
  SELECT
    supplier_id,
    COUNT(*) AS total_resolved
  FROM production.supply_analytics.dim_recommended_actions
  WHERE created_date >= '{FEATURE_LAUNCH}'
    AND surfaced_at  >= '{FEATURE_LAUNCH}'
    AND surfaced_at  <= '{SNAPSHOT_DATE}'
    AND status = 'resolved'
  GROUP BY supplier_id
),
tiered AS (
  SELECT
    CASE
      WHEN total_resolved = 1              THEN 'Single-Use'
      WHEN total_resolved = 2              THEN 'Occasional'
      WHEN total_resolved BETWEEN 3 AND 9  THEN 'Regular'
      ELSE                                      'Power User'
    END AS tier,
    CASE
      WHEN total_resolved = 1              THEN '1 resolved action'
      WHEN total_resolved = 2              THEN '2 resolved actions'
      WHEN total_resolved BETWEEN 3 AND 9  THEN '3–9 resolved actions'
      ELSE                                      '10+ resolved actions'
    END AS definition,
    CASE
      WHEN total_resolved = 1              THEN 1
      WHEN total_resolved = 2              THEN 2
      WHEN total_resolved BETWEEN 3 AND 9  THEN 3
      ELSE                                      4
    END AS tier_order
  FROM resolver_counts
)
SELECT
  tier,
  definition,
  COUNT(*)                                                                                           AS suppliers,
  CONCAT(ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1), '%')                                 AS pct_of_converting_suppliers,
  tier_order
FROM tiered
GROUP BY tier, definition, tier_order
ORDER BY tier_order
""")

display(df_q7_1)

# COMMAND ----------

# Table 7.2 — Q3 2025 Single-Use Cohort: Tier Movement by Q1 2026
# Cohort: suppliers who had exactly one resolved action surfaced in Q3 2025 (2025-07-01 to 2025-09-30).
# Observation window: through end of Q1 2026 (2026-03-31). Cohort size: 3,727 suppliers.
df_q7_2 = spark.sql(f"""
WITH q3_single_use AS (
  -- Suppliers who had exactly one resolved action surfaced in Q3 2025
  SELECT supplier_id
  FROM production.supply_analytics.dim_recommended_actions
  WHERE created_date >= '2025-07-01'
    AND surfaced_at  >= '2025-07-01'
    AND surfaced_at  <  '2025-10-01'
    AND status = 'resolved'
  GROUP BY supplier_id
  HAVING COUNT(*) = 1
),
q1_2026_counts AS (
  -- Total resolved actions per cohort supplier by end of Q1 2026
  SELECT
    a.supplier_id,
    COUNT(*) AS total_resolved_by_q1_2026
  FROM production.supply_analytics.dim_recommended_actions a
  INNER JOIN q3_single_use c ON a.supplier_id = c.supplier_id
  WHERE a.created_date >= '{FEATURE_LAUNCH}'
    AND a.surfaced_at  >= '{FEATURE_LAUNCH}'
    AND a.surfaced_at  <  '2026-04-01'
    AND a.status = 'resolved'
  GROUP BY a.supplier_id
),
tiered AS (
  SELECT
    c.supplier_id,
    COALESCE(q.total_resolved_by_q1_2026, 1) AS total_resolved,
    CASE
      WHEN COALESCE(q.total_resolved_by_q1_2026, 1) = 1              THEN 'Still Single-Use'
      WHEN q.total_resolved_by_q1_2026 = 2                           THEN 'Occasional (2 resolved)'
      WHEN q.total_resolved_by_q1_2026 BETWEEN 3 AND 9               THEN 'Regular (3–9 resolved)'
      ELSE                                                                 'Power User (10+ resolved)'
    END AS q1_2026_tier,
    CASE
      WHEN COALESCE(q.total_resolved_by_q1_2026, 1) = 1              THEN 1
      WHEN q.total_resolved_by_q1_2026 = 2                           THEN 2
      WHEN q.total_resolved_by_q1_2026 BETWEEN 3 AND 9               THEN 3
      ELSE                                                                 4
    END AS tier_order
  FROM q3_single_use c
  LEFT JOIN q1_2026_counts q ON c.supplier_id = q.supplier_id
)
SELECT
  q1_2026_tier,
  COUNT(DISTINCT supplier_id)                                                                        AS suppliers,
  CONCAT(ROUND(100.0 * COUNT(DISTINCT supplier_id) / SUM(COUNT(DISTINCT supplier_id)) OVER (), 1), '%') AS pct_of_cohort,
  tier_order
FROM tiered
GROUP BY q1_2026_tier, tier_order
ORDER BY tier_order
""")

display(df_q7_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Appendix: Key Metrics Glossary
# MAGIC
# MAGIC | Term | Definition |
# MAGIC |------|-----------|
# MAGIC | **Relevant activity** | Activity where: `catalog__tour.supplier_status = 'active'` AND `catalog__tour.gyg_status = 'active'` |
# MAGIC | **Relevant supplier** | Supplier where: `user_status = Active`, has at least one relevant activity, AND (> 1 booking in L365 at supplier level OR any activity first online < 30 days) |
# MAGIC | **Activation period** | A 30-day window beginning when an activity first goes online. Activities in this period are included in the relevant supplier definition even with fewer than 2 bookings in L365 |
# MAGIC | **Feature launch** | 2025-05-15 — date when Recommended Actions was first surfaced. Lower bound of all analysis windows |
# MAGIC | **Snapshot date** | 2026-04-13 — upper bound of all analysis windows |
# MAGIC | **Reached supplier** | A supplier who has had at least one recommended action surfaced to them since feature launch |
# MAGIC | **Reach rate** | Share of relevant suppliers who have received at least one recommended action |
# MAGIC | **Supplier conversion rate** | Share of reached suppliers who have resolved at least one action. Distinct from action-level resolution rate |
# MAGIC | **Resolution rate (overall)** | Share of all surfaced actions where `status = 'resolved'`. Skewed upward for older action types that have had more time to accumulate resolutions |
# MAGIC | **Resolution rate (28-day)** | Share of actions resolved within 28 days of surfacing over all surfaced actions. Primary performance benchmark. Benchmark: 30% |
# MAGIC | **Avg days to resolve** | Mean days between `surfaced_at` and `resolved_at` for resolved actions only |
# MAGIC | **Expired** | Action where the underlying condition was no longer valid before the supplier resolved it (`status = 'expired'`) |
# MAGIC | **Dismissed** | Supplier explicitly closed the action without resolving it (`status = 'dismissed'`) |
# MAGIC | **Active / In progress** | Action currently visible to the supplier and not yet resolved, expired, or dismissed |
# MAGIC | **Actioned** | Action that was either resolved or dismissed |
# MAGIC | **Closed** | Action that reached a terminal state: resolved, expired, or dismissed |
# MAGIC | **Dismissal rate** | Dismissed / actioned (resolved + dismissed). Benchmark: below 10% |
# MAGIC | **Expiry rate** | Expired / closed (resolved + expired + dismissed). Reflects share of completed action cycles that ended without supplier intervention |
# MAGIC | **Managed supplier** | Supplier with a dedicated GYG account manager (`is_managed = 'Managed'` in `dim_supplier_summary`) |
# MAGIC | **Connected supplier** | Supplier with at least one connected option (`is_connected = 'Connected'` in `dim_supplier_summary`) |
# MAGIC | **Engagement tier** | Classification of converting suppliers by lifetime resolved action count: Single-Use (1), Occasional (2), Regular (3–9), Power User (10+) |
# MAGIC | **Q3 2025 single-use cohort** | Suppliers who had exactly one resolved action surfaced in Q3 2025 (2025-07-01 to 2025-09-30). Used to measure engagement progression over the following 6 months |
# MAGIC | **Partition filter** | Always filter `created_date >= '2025-05-15'` to avoid full table scans on the partitioned `dim_recommended_actions` table |

# COMMAND ----------


