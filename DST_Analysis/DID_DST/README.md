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

## 3. Metrics

Weekly (Mon–Sun), 52 clean pre-weeks + up to 8 post-weeks around the intervention date,
by **checkout date**. The mixed launch week beginning 2026-07-27 is excluded because DST
started on Saturday 2026-08-01; one extra historical week is queried to retain 52 clean
pre observations. The current post week remains visible as WTD, with every market capped
to the same elapsed weekdays, but is excluded from the confirmatory ATT until Mon–Sun is complete.

- **Primary confirmatory outcome:** raw NR per frozen cohort supplier. One joint canonical
  block-treatment SDID across all five treated countries is the sole headline estimate.
  Each treated country receives weight 1/5, so the estimand is the average treated-country
  effect on NR per supplier, not an average effect across suppliers. Country SDIDs are
  heterogeneity diagnostics and are gated on pre-fit and rolling holdout quality.
- **Secondary directional outcomes:** current-period bookings, tickets, GMV and visitors
  per frozen cohort supplier, plus supplier-level conversion, add-to-cart,
  unavailability and base commission rates. No YoY transformation is used in the modeled
  outputs. Canonical SDID is fitted independently for every treated-country × outcome pair,
  including outcome-specific unit and time weights. These are driver diagnostics, not
  additional confirmatory hypotheses.
- **Customer rates:** calculate each supplier's weekly rate from that supplier's own
  visitors, then average equally across suppliers with at least one visitor. This prevents
  the largest suppliers from determining the country rate.
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
   equally weighted average treated-country pre-treatment NR path, and non-negative time
   weights summing to one by matching controls' average post outcome to their pre periods.
   The unit-weight regularisation uses
   `zeta = (N_treated * T_post)^0.25 * sigma`, with all five treated countries retained in
   `N_treated`. Estimate the weighted two-way difference using only complete post weeks.
   Supplier counts never enter this confirmatory estimator. No YoY transformation or
   normalization is used. SLSQP solutions are accepted only after optimiser success plus
   finite-objective and simplex checks; genuine failures run a projected-gradient fallback
   that must independently converge and pass the same checks.
2. **Metric-specific secondary SDID:** for every treated country and current-period
   secondary metric, independently estimate canonical non-negative unit weights and
   non-negative time weights, then calculate the weighted two-way difference using complete
   post weeks. Results are bookings/tickets/visitors or EUR per supplier, percentage points
   for shares/rates, and index points for prices. These are exploratory driver measures,
   not additional confirmatory claims. All production fits use 52 weeks; `pre_weeks`
   supports 26/52/78 sensitivity runs for the primary design.
3. **Inference and validation:** Section 5 inference reruns the exact joint equal-country
   estimator used for the primary point estimate. The default small-treated-sample method
   enumerates every 5-of-12 pseudo-treated control assignment (792 assignments), re-estimates
   both weight vectors and estimates variance from the natural-unit placebo ATTs. The
   reported 95% interval is the joint ATT ± 1.96 placebo standard errors. This variance
   assumes comparable error distributions across treated and control countries. A
   configurable country-cluster bootstrap instead resamples complete country trajectories
   with replacement, preserves treatment status and refits the full estimator; its interval
   is the bootstrap percentile interval. Country and joint placebo ranks remain descriptive
   sensitivity diagnostics, not exact randomization or confirmatory p-values. The same
   formal estimator is run at the fixed October 2025 time placebo. Five non-overlapping
   four-week pre-treatment holdouts over 104 weeks select 26/52/78 weeks without
   post-treatment tuning; the configured run blocks if 52 weeks is no longer selected.
   Country estimates failing the 10% in-sample and holdout RMSE gate are labelled
   `inconclusive_fit`.
   Every country-outcome fit additionally receives four rolling four-week pseudo-post
   tests within the 52 clean pre-weeks, using 36/40/44/48 expanding training weeks. A
   secondary result is labelled directionally reliable only when both its in-sample and
   mean holdout RMSE are at most 10%; this internal label does not change published schemas.
4. **Display layer ("story units"):** every series is also published in intuitive units —
   current per-supplier levels for volume metrics, EUR per tour for prices and natural
   units for shares/rates. Every twin uses its own country-metric SDID's time-weighted
   pre-period adjustment. Secondary twin gaps remain directional and are never promoted
   to confirmatory claims.
5. **Financial decomposition:** the dedicated summary reports indirect base-NR impact,
   direct rate-applied DST accrual, and their sum. Multiplying the equal-country
   confirmatory ATT by the total supplier count would change its estimand, so indirect EUR
   scaling uses a separately labelled supplier-count-weighted aggregate-outcome sensitivity.
   It is not the confirmatory estimate. The direct amount is explicitly labelled an accrual
   estimate because the available surcharge table contains rates, not an authoritative
   charge-level invoice field.

### Non-publishing common-outcome benchmark

Cell 21 tests one shared donor-weight vector per treated country using the concatenated
multiple-outcome SCM objective from Sun, Ben-Michael & Feller (2025). Each outcome is
unit-demeaned and scaled by its pre-period standard deviation before fitting. Four rolling
pre-DST holdouts compare common, NR-only and metric-specific donor weights; shared-factor
and leave-one-out-outcome diagnostics test whether one comparison can represent every
metric. The benchmark does not replace `synth_results` or write any table.

In the current run, mean holdout RMSE was 7.17% for common weights, 7.29% for NR-only
weights and 4.94% for metric-specific weights. Although three components explained 80.1%
of standardized pre-period variation, leave-one-out fit failed materially for several
country/metric pairs, especially Turkey's volume outcomes. The all-metric common design
is therefore not adopted.

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
| 10 | f75f749e | joint equal-country raw-NR SDID + country-metric diagnostic fits |
| 11 | — | matched joint placebo variance / configurable country-cluster bootstrap |
| 12 | — | primary 26/52/78 validation + country-metric rolling holdouts and fit labels |
| 13 | — | actual-vs-synthetic panels |
| 14 | — | blocking pre-publication validation |
| 15 | publishsynth1 | staged atomic publication + pooled financial summary |
| 16 | — | donor weights, leave-one-out and MENA block ablation |
| 17 | aff61bff | supplier-level descriptive pre/post query (`df_sup`) |
| 18–19 | — | post-run and production-output validation |
| 20 | e8ef0926 | reaction flags + severities + action list |
| 21 | — | non-publishing common multiple-outcome weight benchmark |

## 7. What the publish cell writes

Per-scenario rows are first written to run-isolated staging tables, counted and validated,
then applied with one Delta `MERGE` per output. Production is never emptied before replacement.

- **`production.supply_analytics.dst_synthetic_control_series`** — scenario, metric,
  country, week: `actual`/`synthetic` (natural units) + `actual_absolute`/`synthetic_absolute`
  (story units, level-corrected twin) + `cohort_suppliers`.
- **`production.supply_analytics.dst_synthetic_control_results`** — scenario, metric,
  country: `did_pct_of_pre`, `pre_fit_rmse_pct`, `placebos_larger`, `donor_weights` (JSON),
  `placebo_p_value`, `did_absolute`, `cohort_suppliers`. For backward compatibility these
  legacy field names remain. Every metric uses the canonical SDID estimator, while only
  `nr_per_supplier` is a confirmatory outcome. For every secondary metric,
  `did_pct_of_pre` contains the natural-unit SDID ATT despite its legacy
  name. Use `did_absolute` for dashboard presentation; it contains the same per-supplier
  DiD in story units, including EUR for GMV and prices. Country placebo fields are
  descriptive ranks. `donor_weights` is metric-specific because every outcome receives
  an independent canonical SDID fit.
- **`production.supply_analytics.dst_sdid_summary`** — one row per scenario/method version:
  joint equal-country canonical-SDID ATT and interval, complete horizon, unit/time weights,
  country fit statuses, separately labelled business-sensitivity indirect base-NR impact,
  direct DST accrual estimate, and combined weekly EUR. Legacy `pooled_*` column names are
  retained for downstream compatibility.

## 8. Runbook

- **Real runs are AUTOMATED**: Databricks job `dst_impact_did_daily` (id 667123464370801)
  runs this notebook from Git `main` every day at 07:30 Europe/Berlin, with
  `intervention_date = 2026-08-03`, and emails the owner on failure. Compute: m5.xlarge
  x2 workers under the UC job policy, 3h timeout (the run needs ~80 min on a small
  cluster - the original 90-min timeout caused a timeout failure on 2026-08-18).
  Pushing to `main` deploys the next run — no job edits needed. Manual real runs remain
  possible locally (set the date, Run All) and are safe **any day of the week**: week
  spines stop at the current week and all markets are capped at the same elapsed days.
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
