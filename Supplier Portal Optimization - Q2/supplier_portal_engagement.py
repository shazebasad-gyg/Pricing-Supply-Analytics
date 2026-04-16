# Databricks notebook source
# MAGIC %md
# MAGIC # Supplier Portal Engagement: Who Logs In, How Often, and What Patterns Emerge
# MAGIC
# MAGIC **Author:** shazeb.asad | **Snapshot Date:** April 12, 2026
# MAGIC
# MAGIC **Source:** [Confluence](https://getyourguide.atlassian.net/wiki/spaces/DA/pages/4186865707)
# MAGIC
# MAGIC **Tables used:**
# MAGIC - `production.supply_analytics.dim_supplier_summary` — Supplier master with rolling login, booking, NR metrics
# MAGIC - `production.supply_analytics.fact_supplier_history` — Daily supplier snapshot partitioned by `date`
# MAGIC - `production.dwh.fact_booking` — Booking-level fact with GMV
# MAGIC - `production.dwh.dim_tour` — Activity dimension with supplier/GYG status
# MAGIC - `production.dwh.dim_tour_history` — Activity history with `is_online` flag and state change timestamps
# MAGIC
# MAGIC **Relevant supplier / activity definition (applied to all queries):**
# MAGIC - Relevant activity: `dim_tour.status = Active` AND `dim_tour.gyg_status = Active` AND (first went online < 30 days ago OR > 1 booking in L365)
# MAGIC - Relevant supplier: `user_status = Active`, has at least one relevant activity, AND (> 1 booking in L365 at supplier level OR any activity first online < 30 days ago)

# COMMAND ----------

SNAPSHOT_DATE = "2026-04-12"

# ─── Relevant Activity ─────────────────────────────────────────────────────
# All three must hold:
#   1. Activity supplier status = Active  (dim_tour.status)
#   2. Activity GYG status      = Active  (dim_tour.gyg_status)
#   3. First went online < 30 days ago  OR  received > 1 booking in L365
#      Online status sourced from dim_tour_history.is_online (MIN = first ever online)
#
# ─── Relevant Supplier ─────────────────────────────────────────────────────
# All three must hold:
#   1. Supplier status = Active  (dim_supplier_summary.user_status)
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

# Standalone CTE for queries that only need activity-level filtering
RELEVANT_ACTIVITIES_CTE = f"""
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
activity_first_online AS (
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
)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q1: How many suppliers are actively using the portal?

# COMMAND ----------

# Table 1.1 — Portal Login Rates by Supplier Cohort
df_q1 = spark.sql(f"""
WITH
{RELEVANT_SUPPLIERS_CTE},
supplier_base AS (
  SELECT
    s.supplier_id,
    s.last_login_date,
    CASE
      WHEN rs.supplier_id IS NOT NULL THEN 'Relevant (active activity, >1 booking L365 or online <30d)'
      ELSE 'Not relevant'
    END AS cohort,
    DATEDIFF(DATE '{SNAPSHOT_DATE}', s.last_login_date) AS days_since_login
  FROM production.supply_analytics.dim_supplier_summary s
  LEFT JOIN relevant_suppliers rs ON s.supplier_id = rs.supplier_id
  WHERE lower(user_status) = 'active'
  and lower(gyg_status) = 'active'
),
grand_total AS (
  SELECT COUNT(*) AS total FROM supplier_base
)
SELECT
  cohort,
  COUNT(*)                                                                                                            AS total_suppliers,
  CONCAT(ROUND(100.0 * COUNT(*) / MAX(grand_total.total), 1), '%')                                                   AS pct_of_all,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN days_since_login <=  7 THEN 1 ELSE 0 END) / COUNT(*), 1), '%')                  AS l7d,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN days_since_login <= 30 THEN 1 ELSE 0 END) / COUNT(*), 1), '%')                  AS l30d,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN days_since_login <= 90 THEN 1 ELSE 0 END) / COUNT(*), 1), '%')                  AS l90d
FROM supplier_base
CROSS JOIN grand_total
GROUP BY cohort
ORDER BY total_suppliers DESC
""")

display(df_q1)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q2: Is portal engagement growing or declining?

# COMMAND ----------

# Table 2.1 — Monthly Portal Reach and Commercial Coverage (Apr 2025 – Mar 2026)
df_q2 = spark.sql(f"""
WITH
{RELEVANT_SUPPLIERS_CTE},
monthly_supplier_logins AS (
  -- One row per relevant supplier per month: did they log in at least once?
  SELECT
    DATE_TRUNC('month', h.date)                    AS month,
    h.supplier_id,
    MAX(CASE WHEN h.logins > 0 THEN 1 ELSE 0 END)  AS logged_in_month
  FROM production.supply_analytics.fact_supplier_history h
  INNER JOIN relevant_suppliers rs ON h.supplier_id = rs.supplier_id
  WHERE h.date >= '2025-04-01'
    AND h.date <  '2026-04-01'
    AND h.is_active = TRUE
  GROUP BY DATE_TRUNC('month', h.date), h.supplier_id
),
monthly_mau AS (
  SELECT
    month,
    COUNT(DISTINCT CASE WHEN logged_in_month = 1 THEN supplier_id END) AS mau
  FROM monthly_supplier_logins
  GROUP BY month
),
monthly_gmv AS (
  SELECT
    DATE_TRUNC('month', b.date_of_checkout) AS month,
    SUM(b.gmv) / 1e6                        AS total_gmv_m
  FROM production.dwh.fact_booking b
  INNER JOIN relevant_suppliers rs ON b.supplier_id = rs.supplier_id
  WHERE b.date_of_checkout >= '2025-04-01'
    AND b.date_of_checkout <  '2026-04-01'
    AND b.is_fraud = FALSE
    AND b.gmv > 0
  GROUP BY DATE_TRUNC('month', b.date_of_checkout)
),
monthly_gmv_logged_in AS (
  -- GMV from relevant suppliers who also logged in that month
  SELECT
    DATE_TRUNC('month', b.date_of_checkout) AS month,
    SUM(b.gmv) / 1e6                        AS gmv_logged_in_m
  FROM production.dwh.fact_booking b
  INNER JOIN monthly_supplier_logins l
    ON  b.supplier_id = l.supplier_id
    AND DATE_TRUNC('month', b.date_of_checkout) = l.month
    AND l.logged_in_month = 1
  WHERE b.date_of_checkout >= '2025-04-01'
    AND b.date_of_checkout <  '2026-04-01'
    AND b.is_fraud = FALSE
    AND b.gmv > 0
  GROUP BY DATE_TRUNC('month', b.date_of_checkout)
)
SELECT
  DATE_FORMAT(m.month, 'MMM yyyy')                                          AS month,
  m.mau                                                                     AS absolute_mau,
  ROUND(g.total_gmv_m, 1)                                                   AS total_gmv_m_eur,
  ROUND(gl.gmv_logged_in_m, 1)                                              AS gmv_logged_in_m_eur,
  CONCAT(ROUND(100.0 * gl.gmv_logged_in_m / g.total_gmv_m, 1), '%')        AS gmv_weighted_login_rate
FROM monthly_mau m
JOIN monthly_gmv            g  ON m.month = g.month
JOIN monthly_gmv_logged_in  gl ON m.month = gl.month
ORDER BY m.month
""")

display(df_q2)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q3: How frequently do suppliers log in?

# COMMAND ----------

# Step 1: Compute NR tier boundaries (p25, p50, p75 of relevant supplier L365 NR)
df_percentiles = spark.sql(f"""
WITH
{RELEVANT_SUPPLIERS_CTE}
SELECT
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY s.nr_l365) AS p25,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY s.nr_l365) AS p50,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY s.nr_l365) AS p75
FROM production.supply_analytics.dim_supplier_summary s
INNER JOIN relevant_suppliers rs ON s.supplier_id = rs.supplier_id
WHERE s.gyg_status = 'active'
  AND s.nr_l365 > 0
""")

display(df_percentiles)

# COMMAND ----------

# Table 3.1 — Login Frequency Distribution by NR Tier (Jan–Mar 2026 average)
df_q3 = spark.sql(f"""
WITH
{RELEVANT_SUPPLIERS_CTE},
relevant_supplier_nr AS (
  SELECT
    s.supplier_id,
    s.nr_l365,
    CASE
      WHEN s.nr_l365 IS NULL OR s.nr_l365 = 0 THEN 'No NR'
      WHEN s.nr_l365 <    346                  THEN '€1–€346'
      WHEN s.nr_l365 <   1627                  THEN '€346–€1,627'
      WHEN s.nr_l365 <   8958                  THEN '€1,627–€8,958'
      ELSE                                          '€8,958+'
    END AS nr_tier,
    CASE
      WHEN s.nr_l365 IS NULL OR s.nr_l365 = 0 THEN 0
      WHEN s.nr_l365 <    346                  THEN 1
      WHEN s.nr_l365 <   1627                  THEN 2
      WHEN s.nr_l365 <   8958                  THEN 3
      ELSE                                          4
    END AS tier_order
  FROM production.supply_analytics.dim_supplier_summary s
  INNER JOIN relevant_suppliers rs ON s.supplier_id = rs.supplier_id
  WHERE s.gyg_status = 'active'
),
monthly_login_counts AS (
  -- Average monthly login count per relevant supplier over Jan–Mar 2026
  SELECT
    h.supplier_id,
    DATE_TRUNC('month', h.date) AS month,
    SUM(h.logins)               AS logins_in_month
  FROM production.supply_analytics.fact_supplier_history h
  INNER JOIN relevant_suppliers rs ON h.supplier_id = rs.supplier_id
  WHERE h.date >= '2026-01-01'
    AND h.date <  '2026-04-01'
    AND h.is_active = TRUE
  GROUP BY h.supplier_id, DATE_TRUNC('month', h.date)
),
avg_monthly_logins AS (
  SELECT
    supplier_id,
    AVG(logins_in_month) AS avg_monthly_logins
  FROM monthly_login_counts
  GROUP BY supplier_id
),
combined AS (
  SELECT
    g.supplier_id,
    g.nr_tier,
    g.tier_order,
    COALESCE(l.avg_monthly_logins, 0) AS avg_monthly_logins,
    CASE
      WHEN COALESCE(l.avg_monthly_logins, 0) = 0 THEN 'No Login'
      WHEN l.avg_monthly_logins < 3              THEN '1–2 Logins/month'
      WHEN l.avg_monthly_logins < 6              THEN '3–5 Logins/month'
      WHEN l.avg_monthly_logins < 11             THEN '6–10 Logins/month'
      ELSE                                            '10+ Logins/month'
    END AS login_bucket
  FROM relevant_supplier_nr g
  LEFT JOIN avg_monthly_logins l ON g.supplier_id = l.supplier_id
)
SELECT
  nr_tier                                                                                                                               AS nr_tier_l365,
  COUNT(*)                                                                                                                               AS total_suppliers,
  SUM(CASE WHEN login_bucket = 'No Login'          THEN 1 ELSE 0 END)                                                                  AS no_login,
  SUM(CASE WHEN login_bucket = '1–2 Logins/month'  THEN 1 ELSE 0 END)                                                                  AS logins_1_2,
  SUM(CASE WHEN login_bucket = '3–5 Logins/month'  THEN 1 ELSE 0 END)                                                                  AS logins_3_5,
  SUM(CASE WHEN login_bucket = '6–10 Logins/month' THEN 1 ELSE 0 END)                                                                  AS logins_6_10,
  SUM(CASE WHEN login_bucket = '10+ Logins/month'  THEN 1 ELSE 0 END)                                                                  AS logins_10_plus,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN login_bucket = '10+ Logins/month' THEN 1 ELSE 0 END) / COUNT(*), 1), '%')                         AS pct_10_plus
FROM combined
GROUP BY nr_tier, tier_order
ORDER BY tier_order
""")

display(df_q3)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q4: Which supplier segments are most and least engaged?

# COMMAND ----------

# Table 4.1 — Login Rates by Supplier Segment
df_q4 = spark.sql(f"""
WITH
{RELEVANT_SUPPLIERS_CTE}
SELECT
  s.segment                                                                                                                                     AS supplier_segment,
  COUNT(*)                                                                                                                                      AS total_suppliers,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN DATEDIFF(DATE '{SNAPSHOT_DATE}', s.last_login_date) <=  7 THEN 1 ELSE 0 END) / COUNT(*), 1), '%')         AS l7,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN DATEDIFF(DATE '{SNAPSHOT_DATE}', s.last_login_date) <= 30 THEN 1 ELSE 0 END) / COUNT(*), 1), '%')         AS l30,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN DATEDIFF(DATE '{SNAPSHOT_DATE}', s.last_login_date) <= 90 THEN 1 ELSE 0 END) / COUNT(*), 1), '%')         AS l90
FROM production.supply_analytics.dim_supplier_summary s
INNER JOIN relevant_suppliers rs ON s.supplier_id = rs.supplier_id
WHERE s.gyg_status = 'active'
GROUP BY s.segment
ORDER BY
  CASE s.segment
    WHEN 'Scale Seeker'        THEN 1
    WHEN 'Leisure Brand'       THEN 2
    WHEN 'Heritage Preserver'  THEN 3
    WHEN 'Independent Creator' THEN 4
    ELSE 5
  END
""")

display(df_q4)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q5: Does account management or connectivity change engagement?

# COMMAND ----------

# Table 5.1 — Login Rates by Managed / Connected Status
df_q5 = spark.sql(f"""
WITH
{RELEVANT_SUPPLIERS_CTE}
SELECT
  CASE
    WHEN s.is_managed = 'Managed' AND s.is_connected = 'Connected'    THEN 'Managed + Connected'
    WHEN s.is_managed = 'Managed' AND s.is_connected != 'Connected'   THEN 'Managed + Non-Connected'
    WHEN s.is_managed != 'Managed' AND s.is_connected = 'Connected'   THEN 'Non-Managed + Connected'
    ELSE                                                                    'Non-Managed + Non-Connected'
  END                                                                                                                                           AS segment,
  COUNT(*)                                                                                                                                      AS total_suppliers,
  CONCAT(ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1), '%')                                                                             AS pct_of_base,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN DATEDIFF(DATE '{SNAPSHOT_DATE}', s.last_login_date) <=  7 THEN 1 ELSE 0 END) / COUNT(*), 1), '%')         AS l7,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN DATEDIFF(DATE '{SNAPSHOT_DATE}', s.last_login_date) <= 30 THEN 1 ELSE 0 END) / COUNT(*), 1), '%')         AS l30,
  CONCAT(ROUND(100.0 * SUM(CASE WHEN DATEDIFF(DATE '{SNAPSHOT_DATE}', s.last_login_date) <= 90 THEN 1 ELSE 0 END) / COUNT(*), 1), '%')         AS l90,
  CONCAT('€', FORMAT_NUMBER(AVG(s.nr_l365) / 1000, 0), 'k')                                                                                   AS avg_nr_per_supplier
FROM production.supply_analytics.dim_supplier_summary s
INNER JOIN relevant_suppliers rs ON s.supplier_id = rs.supplier_id
WHERE s.gyg_status = 'active'
GROUP BY
  CASE
    WHEN s.is_managed = 'Managed' AND s.is_connected = 'Connected'    THEN 'Managed + Connected'
    WHEN s.is_managed = 'Managed' AND s.is_connected != 'Connected'   THEN 'Managed + Non-Connected'
    WHEN s.is_managed != 'Managed' AND s.is_connected = 'Connected'   THEN 'Non-Managed + Connected'
    ELSE                                                                    'Non-Managed + Non-Connected'
  END
ORDER BY
  CASE segment
    WHEN 'Managed + Connected'        THEN 1
    WHEN 'Managed + Non-Connected'    THEN 2
    WHEN 'Non-Managed + Connected'    THEN 3
    ELSE 4
  END
""")

display(df_q5)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Appendix: Key Metrics Glossary
# MAGIC
# MAGIC | Term | Definition |
# MAGIC |------|-----------|
# MAGIC | **Relevant activity** | Activity where: `dim_tour.status = Active`, `dim_tour.gyg_status = Active`, AND (first went online < 30 days OR > 1 booking in L365). Online status sourced from `dim_tour_history.is_online` |
# MAGIC | **Relevant supplier** | Supplier where: `user_status = Active`, has at least one relevant activity, AND (> 1 booking in L365 at supplier level OR any activity first online < 30 days) |
# MAGIC | **L7 / L30 / L90** | At least one portal login in the last 7 / 30 / 90 calendar days, derived from `last_login_date` in `dim_supplier_summary` |
# MAGIC | **GMV-weighted login rate** | % of total monthly platform GMV generated by suppliers who logged in at least once that month |
# MAGIC | **Absolute MAU** | Unique relevant suppliers with ≥1 login in a calendar month |
# MAGIC | **NR tier boundaries** | p25 = €346, p50 = €1,627, p75 = €8,958 (of relevant supplier NR distribution) |
# MAGIC | **GMV** | Gross Merchandise Value in EUR at checkout. Excludes fraud and zero-value bookings |
# MAGIC | **date_of_checkout** | Booking date field in `fact_booking` |
# MAGIC | **Connected** | Supplier uses a connectivity solution (`is_connected = 'Connected'`) |
# MAGIC | **Managed** | Supplier has a dedicated GYG account manager (`is_managed = 'Managed'`) |

# COMMAND ----------


