# DST Impact Estimation — `dst_impact_did.ipynb`

Measures supplier and marketplace reaction to the DST commission surcharge GetYourGuide
introduced on 2026-08-01 (analysis intervention date: **2026-08-03**, the Monday).
This notebook fits the synthetic-control counterfactuals and publishes the measurement
layer consumed by the Looker dashboard
([DST Impact Monitoring, dashboard 12980](https://getyourguide.looker.com/dashboards/12980)).

Owner: Shazeb Asad (Supply Analytics). Methodology record: Confluence
"DST Supply Impact Measurement — Design Document" (DA space).

---

## 1. The intervention

- **Treatment countries (5):** Italy, Spain, Britain, Turkey, France. **Germany is a control.**
- **Donor/control pool (12):** Portugal, Greece, Netherlands, Switzerland, Austria, Germany,
  United States, "Croatia, Republic of", Poland, United Arab Emirates, "Morocco, Kingdom of", Egypt.
- **Donor eligibility protocol:** freeze the current business-selected monitoring universe;
  require no August 2026 DST treatment, identical source definitions, a balanced 104-week
  pre-treatment NR history, adequate supplier support, and no known concurrent market-specific
  policy break. No real post-treatment outcome is used to add or remove a donor. The notebook
  publishes rolling pre-period validation, a MENA block ablation, and leave-one-donor-out
  sensitivity. These checks diagnose robustness; they do not tune the pool after seeing impact.
  In the final five-fold test, adding MENA donors worsened Turkey holdout RMSE by 45% on average;
  the earlier claim that they halved noise is therefore rejected.
- **Surcharge** (source of truth `production.supply_analytics.dst_commission_country`, final):
  TR 5.2632 · FR 3.0928 · IT 2.5402 · GB 1.7966 · ES 1.6412 — charged **on top of** base
  commission (`effective = base × (1 + surcharge/100)`), invoiced separately.
  It never flows into `fact_booking.nr/gmv`: observed commission is therefore a null-check,
  and a genuine drop = commission renegotiation (a reaction channel).
- **Scenarios:** `real` = 2026-08-03; `placebo` = 2025-10-06 (a pre-specified no-event window —
  every number the method produces on the placebo is measurement noise, the baseline all
  real reads are compared against). An earlier March placebo was discarded (Easter artifact).

## 2. Population: the locked supplier pool

`production.supply_analytics.dst_supplier_pool_pre_dst_v1` — every supplier with
`gyg_status = 'active'` immediately before treatment, reconstructed from
`production.dwh.dim_supplier_history` as of **2026-07-31**. It contains 94,843 suppliers
across the fixed 17-country universe and is created once with `CREATE TABLE IF NOT EXISTS`.
This is an intent-to-treat cohort: primary bookings join `fact_booking.supplier_id` directly
to the frozen supplier list, so post-launch/new-tour activity is included and changing the
fit window cannot change cohort membership. Tour attachment is used only for descriptive
supply, price, customer and supplier-reaction outputs.

## 3. Metrics (23)

Weekly (Mon–Sun), 52 clean pre-weeks + up to 8 post-weeks around the intervention date,
by **checkout date**. The mixed launch week beginning 2026-07-27 is excluded because DST
started on Saturday 2026-08-01; one extra historical week is queried to retain 52 clean
pre observations. The current post week remains visible as WTD, with the same elapsed
weekdays last year, but is excluded from the confirmatory ATT until Mon–Sun is complete.

- **Primary confirmatory outcome:** raw NR per frozen cohort supplier. One supplier-weighted
  pooled SDID across all five treated countries is the sole headline estimate. Country SDIDs
  are heterogeneity diagnostics and are gated on pre-fit and rolling holdout quality.
- **Secondary directional outcomes:** same-cohort YoY ratios (this week ÷ same week last
  year, −364d) for bookings, tickets, GMV, NR, visitors, conversion, add-to-cart,
  unavailability, and commission. Raw bookings/GMV and the other operational metrics remain
  visible as diagnostics; they are not additional confirmatory hypotheses.
- **Supply:** % active suppliers, % tours online per supplier, forward availability coverage
  (share of the next 90 days with open slots).
- **Price (forward-looking):** every Monday, each tour's advertised per-adult price for travel
  in the next 90 days, indexed to the tour's own pre-period average per (tour, currency)
  (FX-free), winsorised [0.5, 2.0]; base (list) and effective (after discounts) variants.
- GMV/NR winsorised at a per-country p99 booking cap computed from pre-treatment CY data
  only. `fact_booking` only (never `_v2`),
  `status_id IN (1, 2)` (gross incl. later-cancelled → time-stable weeks).

## 4. Method

1. **Primary formal Synthetic DiD:** fit non-negative unit weights summing to one to the
   raw pre-treatment NR path, and non-negative time weights summing to one by matching
   controls' average post outcome to their pre periods. Estimate the weighted two-way
   difference using only complete post weeks. The pooled treatment path weights the five
   treated countries by their frozen supplier counts, so the per-supplier ATT scales
   directly to treated-cohort EUR. No YoY transformation or normalization is used.
2. **Secondary normalized synthetic controls:** the existing operational and demand series
   remain normalized to their own pre-period means so their directional paths remain
   comparable across differently sized countries. These are exploratory, not additional
   causal claims. Primary runs use 52 weeks; `pre_weeks` supports 26/52/78 sensitivity runs.
3. **Inference and validation:** pooled placebo sensitivity enumerates every 5-of-12
   pseudo-treated donor assignment (792 assignments), reports
   `(1 + exceedances) / (1 + 792)`, and forms a placebo-distribution interval. This is
   explicitly not called an exact randomized-experiment p-value because policy assignment
   is not proven exchangeable. The same formal estimator is run at the fixed October 2025
   time placebo. Five non-overlapping four-week pre-treatment holdouts over 104 weeks select
   26/52/78 weeks without post-treatment tuning; the configured run blocks if 52 weeks is
   no longer selected. Country estimates failing the 10% in-sample and holdout RMSE gate are
   labelled `inconclusive_fit`.
4. **Display layer ("story units"):** every series is also published in intuitive units —
   per-supplier levels for volume metrics (via the same-week-LY base implied by the YoY
   ratio), EUR per tour for prices, natural units for shares. The primary twin is adjusted
   with the SDID time-weighted pre gap so its complete-post mean gap equals the formal ATT.
   Secondary twin gaps remain directional and are never labelled causal DiD.
5. **Financial decomposition:** the dedicated summary reports indirect base-NR SDID impact,
   direct rate-applied DST accrual, and their sum. The direct amount is explicitly labelled
   an accrual estimate because the available surcharge table contains rates, not an
   authoritative charge-level invoice field.

## 5. Supplier reaction flags (no fixed thresholds)

Six **independent** flags per supplier (a supplier can carry 0–6; flags are raw truth —
exited implies went_dark):

| Flag | Fires when |
|---|---|
| `exited` | active pre, zero active weeks post (unconditional) |
| `went_dark` | active-week share dropped beyond the control p95 line |
| `trimmed_portfolio` | tours-online share dropped beyond the control p95 line |
| `withdrew_capacity` | forward coverage dropped beyond the control p95 line |
| `repriced_up` | base price rose beyond the control p95 line |
| `commission_dropped` | realised commission rate fell beyond the control p95 line |

Calibration: control-median zero point, control-p95 tail scale → severity (1.0 = at the
line, capped 9.99), flag = severity ≥ 1. ~5% false-alarm rate per flag by construction,
self-calibrating per scenario window. `gmv_at_risk` = pre-period GMV when any flag fired —
the action-list sort key. Per-flag counts are read as **excess over the same-window control
rate** (never raw counts); no individual supplier can be attributed to DST — only aggregates.

## 6. Notebook cell map

| # | id | Purpose |
|---|---|---|
| 0 | — | `run_sql` helper (Databricks SQL connector, profile `ShazebAsad`, warehouse `774b0b6877dcd0c3`) |
| 1 | ce03ff09 | `INTERVENTION_DATE` + supply metrics query |
| 2 | b3693d3b | price metrics query |
| 3 | d2003064 | realised (bookings/GMV/NR) query |
| 4 | 1b8986fc | customer (traffic/conversion) query |
| 5 | — | merge → `dfm` (week × country panel) |
| 6–9 | — | descriptive charts |
| 10 | f75f749e | pooled/country formal raw-NR SDID + directional normalized SCM |
| 11 | — | pooled 5-of-12 placebo sensitivity |
| 12 | — | 26/52/78-week rolling pre-period validation and fit gates |
| 13 | — | actual-vs-synthetic panels |
| 14 | — | blocking pre-publication validation |
| 15 | publishsynth1 | staged atomic publication + pooled financial summary |
| 16 | — | donor weights, leave-one-out and MENA block ablation |
| 17 | aff61bff | supplier-level descriptive pre/post query (`df_sup`) |
| 18–19 | — | post-run and production-output validation |
| 20 | e8ef0926 | reaction flags + severities + action list |

## 7. What the publish cell writes

Per-scenario rows are first written to run-isolated staging tables, counted and validated,
then applied with one Delta `MERGE` per output. Production is never emptied before replacement.

- **`production.supply_analytics.dst_synthetic_control_series`** — scenario, metric,
  country, week: `actual`/`synthetic` (natural units) + `actual_absolute`/`synthetic_absolute`
  (story units, level-corrected twin) + `cohort_suppliers`.
- **`production.supply_analytics.dst_synthetic_control_results`** — scenario, metric,
  country: `did_pct_of_pre`, `pre_fit_rmse_pct`, `placebos_larger`, `donor_weights` (JSON),
  `placebo_p_value`, `did_absolute`, `cohort_suppliers`. For backward compatibility these
  legacy field names remain; only `nr_per_supplier` is formal SDID. Other rows are
  directional normalized-SCM gaps, and country placebo fields are descriptive ranks.
- **`production.supply_analytics.dst_sdid_summary`** — one row per scenario/method version:
  pooled formal-SDID ATT and interval, complete horizon, unit/time weights, country fit
  statuses, indirect base-NR impact, direct DST accrual estimate, and combined weekly EUR.

## 8. Runbook

- **Real runs are AUTOMATED**: Databricks job `dst_impact_did_daily` (id 667123464370801)
  runs this notebook from Git `main` every day at 07:30 Europe/Berlin, with
  `intervention_date = 2026-08-03`, and emails the owner on failure. Compute: m5.xlarge
  x2 workers under the UC job policy, 3h timeout (the run needs ~80 min on a small
  cluster - the original 90-min timeout caused a timeout failure on 2026-08-18).
  Pushing to `main` deploys the next run — no job edits needed. Manual real runs remain
  possible locally (set the date, Run All) and are safe **any day of the week**: week
  spines stop at the current week, and last-year comparisons are capped at the same
  elapsed days (partial-week YoY compares Mon–Wed vs Mon–Wed).
- **Placebo re-run:** only after population/method changes, so baselines stay comparable.
- Reading discipline: week-one gaps are direction-only; estimates firm up by ~3 complete
  post-weeks; dose ladder sanity check TR > FR > IT > GB > ES; below the noise band,
  signs are coin flips.
- **Auth:** `databricks auth login --host https://dbc-d10db17d-b6c4.cloud.databricks.com
  --profile ShazebAsad` when the refresh token dies.

## 9. Related systems

- **dbt** (`dap-dbt-transformations`): `agg_dst_impact_weekly` (weekly monitoring, daily DAG)
  and `dim_dst_supplier_impact` (flags + action list + forward book), same pool/scenarios/
  conventions. Key PRs: #3128 (models), #3146 (elapsed weeks), #3149 (flags),
  #3166 (locked pool), #3242 (forward book).
- **Looker** (`gyg-looker`, supply_operations model): explores `dst_impact_weekly`,
  `dst_supplier_impact`, `dst_synthetic_control`. Key PRs: #3822, #3826, #3833, #3865.
- New dbt sources need an entry in airflow `ci/python/sensors_to_ignore.yaml`
  (pattern: airflow #21378, #21572, #21803).

## 10. Gotchas learned the hard way

- `int_tour_online_daily`-style week spines must stop at `CURRENT_DATE()` or future weeks
  zero-fill into a fake collapse; LY windows must cap at the same elapsed point or partial
  weeks read as crashes.
- `vacancies` has sentinel 9,999,999 (unlimited) — flag, never sum.
- `daily_tour_price_snapshot` prices are LOCAL currency — index within (tour, currency).
- `dim_supplier_summary.is_managed/is_connected` are strings ('Managed', 'true'-style
  labels vary) — check values before comparing.
- `n_tours` can be 0 under the pool cohort — guard divisions with `NULLIF`.
- Looker shows cached results ("from cache · Nh ago") — clear cache before declaring
  numbers wrong; format/filter/window explain most "discrepancies".
