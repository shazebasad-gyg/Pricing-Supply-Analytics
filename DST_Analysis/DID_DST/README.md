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
- **Surcharge** (source of truth `production.supply_analytics.dst_commission_country`, final):
  TR 5.2632 · FR 3.0928 · IT 2.5402 · GB 1.7966 · ES 1.6412 — charged **on top of** base
  commission (`effective = base × (1 + surcharge/100)`), invoiced separately.
  It never flows into `fact_booking.nr/gmv`: observed commission is therefore a null-check,
  and a genuine drop = commission renegotiation (a reaction channel).
- **Scenarios:** `real` = 2026-08-03; `placebo` = 2025-10-06 (a validated no-event window —
  every number the method produces on the placebo is measurement noise, the baseline all
  real reads are compared against). An earlier March placebo was discarded (Easter artifact).

## 2. Population: the locked supplier pool

`production.supply_analytics.dst_supplier_pool` — every supplier with `gyg_status = 'active'`
as of **2026-08-04**, all 17 countries, 94,934 suppliers. Created once, never rebuilt.
**The pool IS the cohort** — no supplier-type or listing filters. Tours attach via LEFT JOIN
(a pool supplier with nothing listed stays in every denominator; their inactivity is signal).
The same pool is used by the dbt models and this notebook, in both scenarios, so baselines
and live reads always describe the same population.

## 3. Metrics (23)

Weekly (Mon–Sun), 8 pre-weeks + 8 post-weeks around the intervention date, by **checkout date**.

- **Causal (validated) demand metrics — same-cohort YoY ratios:** this week ÷ same week last
  year (−364d) on the same cohort tours: bookings, tickets, GMV, NR, visitors, conversion rate,
  add-to-cart rate, unavailability rate, commission rate per supplier. YoY cancels
  country-specific seasonality; raw levels failed the placebo (±20–40% ghosts) and are
  published only as "(raw — reference only)".
- **Supply:** % active suppliers, % tours online per supplier, forward availability coverage
  (share of the next 90 days with open slots).
- **Price (forward-looking):** every Monday, each tour's advertised per-adult price for travel
  in the next 90 days, indexed to the tour's own pre-period average per (tour, currency)
  (FX-free), winsorised [0.5, 2.0]; base (list) and effective (after discounts) variants.
- GMV/NR winsorised at the per-country p99 booking. `fact_booking` only (never `_v2`),
  `status_id IN (1, 2)` (gross incl. later-cancelled → time-stable weeks).

## 4. Method

1. **Synthetic control per metric × treatment country:** all series normalised to their own
   pre-period mean (levels released — the multiplicative analogue of the SDID intercept;
   the optimiser matches *trends*, not levels, which is what makes small donors usable for
   Turkey-sized outliers). Non-negative sum-to-1 weights, scipy SLSQP, fitted on the
   8 pre-weeks only.
2. **DiD:** (treatment post − pre) − (synthetic post − pre), in % of the treatment
   pre-period mean (`did_pct_of_pre`).
3. **Inference:** placebo-in-space — each donor fake-treated against the rest; reported as
   "placebos larger x/12". Plus the in-time placebo scenario for every published number.
4. **Display layer ("story units"):** every series is also published in intuitive units —
   per-supplier levels for volume metrics (via the same-week-LY base implied by the YoY
   ratio), EUR per tour for prices, natural units for shares. The displayed twin is
   **level-corrected** (its pre-period mean shifted to match the actual's) so the visible
   post-period gap ≡ the quoted DiD. Rule the whole dashboard follows: *every visible gap
   equals the quoted number*.
5. **Detection limits (from the placebo):** supply ±3.5%, price ±1%, bookings YoY ±3–6%,
   GMV/NR YoY ±3–8%, conversion ±3–5%. Week-one estimates run 2–3× noisier. France's
   October-placebo NR (±10%) is a Paris-2024-Olympics base effect, documented.

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
| 10 | f75f749e | synthetic control fit + DiD + placebo-in-space (`SYNTH_METRICS`, 23 metrics) |
| 11 | — | actual-vs-synthetic panels per metric |
| 12 | publishsynth1 | **publish**: writes both scenarios' outputs to Databricks (see §7) |
| 13 | — | donor weight usage |
| 14 | aff61bff | supplier-level pre/post query (`df_sup`) |
| 15 | — | validation checks (week counts adapt to elapsed weeks) |
| 16 | — | reconciliation vs `agg_dst_impact_weekly` (placebo-only; skips otherwise) |
| 17 | e8ef0926 | reaction flags + severities + action list |

## 7. What the publish cell writes

Per-scenario DELETE + INSERT (running one scenario never touches the other):

- **`production.supply_analytics.dst_synthetic_control_series`** — scenario, metric,
  country, week: `actual`/`synthetic` (natural units) + `actual_absolute`/`synthetic_absolute`
  (story units, level-corrected twin) + `cohort_suppliers`.
- **`production.supply_analytics.dst_synthetic_control_results`** — scenario, metric,
  country: `did_pct_of_pre`, `pre_fit_rmse_pct`, `placebos_larger`, `donor_weights` (JSON),
  `did_absolute` (avg weekly post gap in story units), `cohort_suppliers`.

## 8. Runbook

- **Real run:** set `INTERVENTION_DATE = '2026-08-03'` → Run All → confirm the publish cell
  prints `published scenario 'real': …`. Safe **any day of the week**: week spines stop at
  the current week, and last-year comparisons are capped at the same elapsed days
  (partial-week YoY compares Mon–Wed vs Mon–Wed). Re-run daily/weekly = the dashboard's
  measurement-layer refresh.
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
