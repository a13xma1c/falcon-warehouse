# Falcon ad network: small data warehouse (PySpark)

Daily job that loads the day folders of `dataset/` into a staging layer, dimensions, facts, an ML training mart and a data-quality report. Python + PySpark only at runtime (`urllib`/`json` for the external APIs, no pandas, requests, pyyaml or holidays packages). `pytest` is a dev dependency.

The description of the source data is in [`dataset/README.md`](dataset/README.md).

## How to run

Needs Java 17 (local Spark) and [uv](https://docs.astral.sh/uv/).

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17        # any JDK 11/17
uv sync                                               # pyspark==3.5.3 + pytest (dev)
ln -s ~/Downloads/dataset dataset                     # or set dataset_path in config/config.json

# one day, on top of what is already loaded (idempotent: running it twice changes nothing)
PYTHONPATH=src uv run --no-sync python -m falcon.run --date 2026-09-25
# a range / everything, in date order
PYTHONPATH=src uv run --no-sync python -m falcon.run --from 2026-08-24 --to 2026-10-25
PYTHONPATH=src uv run --no-sync python -m falcon.run --all
# like a scheduler: one process per day, resumable (what produced reports/full_run_log.txt)
scripts/run_all.sh

# reports and proofs
uv run python scripts/export_reports.py               # reports/dq_*.csv|md, table_row_counts.md
uv run python scripts/verify_history.py --day 2026-10-12 [--publisher ID]   # recompute history features from raw staging
uv run python scripts/rerun_proof.py --date 2026-09-15     # idempotency proof on the fully loaded warehouse (writes reports/idempotency_proof.*)
uv run pytest                                          # 28 tests (the integration test takes ~2 min)
```

All paths and settings (dataset, warehouse, reports, Spark memory/partitions, API base URLs, timeouts, retries, history windows, DQ thresholds) are in [`config/config.json`](config/config.json). No secrets are needed: both APIs are public. `--config` / `$FALCON_CONFIG` selects another file.

Do not start two Spark jobs at once on one machine (a concurrent pytest killed a full run with `Bad file descriptor` once).

A full run of the 62 folders takes about an hour on a laptop (`local[4]`, 6 GB driver). Each day in its own process, as `run_all.sh` does, is deliberate: looping all days in one JVM thrashed in GC around day 53.

## What was built

```
dataset/dt=D/...  ->  staging  ->  dims + events  ->  facts  ->  mart
                                     (dw/dim_*, dw/ev_*)  (dw/fact_*)  (mart/ml_training)
                                  dq/*  (checks, rejects, reconciliation)      ref/* (FX, holidays)
```

Everything lives under `outputs/warehouse/` as partitioned Parquet. Code is in `src/falcon/`.

| layer | table | grain / key | partition |
|---|---|---|---|
| staging | `staging/*` (requests, impressions, clicks, postbacks, orders, 8 dims, 2 enrichments) | raw rows of the folder, explicit schemas, no fixes; adds `dt` (folder) and `_src_file` | folder day |
| dim | `dw/dim_placement` | placement x snapshot day, key `(placement_id, dt)`; placement -> site -> publisher -> platform, site category (+ parent) | snapshot day |
| dim | `dw/dim_banner` | banner x snapshot day, key `(banner_id, dt)`; banner -> campaign -> advertiser, campaign category | snapshot day |
| dim | `dw/dim_site_enrichment`, `dim_campaign_enrichment` | enrichment row x snapshot day | snapshot day |
| events | `dw/ev_request`, `ev_impression`, `ev_click`, `ev_postback`, `ev_order` | typed, de-duplicated; keys `request_id`, `impression_id`, `click_id`, `postback_id`, `(order_id, site_id)` | **arrival day** |
| fact | `dw/fact_impression` | **one row per `fid`** (slot); click and conversion outcomes attached (`n_clicks`, `n_postbacks`, `revenue_usd`, first times) | request day |
| fact | `dw/fact_request` | one row per `request_id`; order amount in USD, outcome totals | request day |
| fact | `dw/fact_order` | one row per `(order_id, site_id)`; amount in USD | order day |
| mart | `mart/ml_training` | **one row per `fid`**, real (non-preview) requests only; labels `is_click`, `is_conversion` | request day |
| dq | `dq/dq_checks`, `dq/rejects`, `dq/reconciliation` | one row per check / rejected record / reconciliation line, per load day | load day |
| ref | `ref/fx_raw`, `ref/fx_daily`, `ref/holiday`, `ref/holiday_coverage` | FX per (date, currency), holidays per (country, date, name), coverage per (country, year) | none |

Row counts of every table: [`reports/table_row_counts.md`](reports/table_row_counts.md).

### Pipeline steps for one day (`falcon.pipeline.load_day`)

1. **Staging** (`staging.py`, `schemas.py`): every file of the folder is read with an explicit schema. Requests are JSON in PERMISSIVE mode with `_corrupt_record`; `data` is kept as a string and parsed later with `get_json_object`, because the payload has case-sensitive keys (`orderid`/`orderId`/`orderID`) and `spark.sql.caseSensitive=true` is set.
2. **Dimensions** (`dims.py`): each snapshot is processed on its own. Duplicate ids inside a snapshot are resolved (newest `updated_at`). All joins are left joins; a missing parent sets a `*_found` flag instead of dropping the row.
3. **Events** (`events.py`): typing, parsing, device/OS/browser from the user agent without UDFs (`ua.py`), duplicate and corrupt records go to `dq/rejects` with a reason.
4. **Facts** (`facts.py`): see late events below. Amounts are converted to USD with the FX rate of the order date.
5. **Mart** (`mart.py`): features of the rebuilt slots.
6. **DQ** (`dq.py`, `pipeline.py`): checks, rejects, manifest comparison and cumulative reconciliation.

### Idempotency and late events

* Every step replaces only its own partitions (`common.write_partitions`, `merge_partitions`). Running a day twice gives the same tables; duplicates are judged against folders *before* that day only.
* Events are partitioned by **arrival day** (the folder they came in). That keeps "what we knew on day D" reproducible.
* Facts and the mart are partitioned by **request day**. On load day D the job finds the **touched slots**: `fid`s of impressions, clicks and postbacks that arrived in folder D, plus `fid`s of earlier impressions whose request arrived in folder D. Exactly those rows are rebuilt from the event layer and merged in; only partitions that held an old row or receive a new one are rewritten. So a click on 2026-09-30 for an impression of 2026-09-26 updates the 09-26 partition of `fact_impression` and `ml_training`, and untouched days are not rewritten.
* The result depends only on the event layer, which is why the replay is identical: [`reports/idempotency_proof.md`](reports/idempotency_proof.md) fingerprints every table (row count, numeric sums, order-independent hash of all columns) before and after re-running a day on the fully loaded warehouse. `tests/test_pipeline_integration.py` checks the same on three real folders, plus that later folders change the labels of earlier days.
* A day is refused if an earlier existing folder has not been loaded (no silent gaps).

### ML mart and leakage rules

* Labels: `is_click`, `is_conversion` as observed so far.
* Features come only from what existed at request time: request context, placement/banner attributes (snapshot of the request day or the newest earlier one), enrichment rows with `created_at <= request time`, calendar, public holiday flag, device/OS/browser.
* From the impression row only the slot position, banner and placement are used. Impression time, status, render count, clicks and conversions are **not** features.
* Publisher history (click/conversion rate and slots over 7 and 28 days) for a request on day E uses only events that had **arrived** in a folder before E (`arrival_dt < E`) and slots from [E-w, E-1]. A conversion reported on day E or later does not count for E even if it belongs to a slot of E-3. Same-day arrivals are ignored, which is conservative, and it makes the features independent of when a mart row is rebuilt. `scripts/verify_history.py` recomputes the features from raw staging for two publishers ([`reports/history_verification.md`](reports/history_verification.md)).

## External data (loaded by code)

* **FX** (`external.py`): [Frankfurter](https://api.frankfurter.dev) (ECB rates, business days). It does not publish about 13 currencies seen in the orders (AED, SAR, VND, PKR, TWD, QAR, PEN, ...), so these come from the jsDelivr currency-api as a fallback. Weekends and holidays are forward-filled. Incremental: only missing currencies / dates are fetched. An order with a missing or non-positive amount, or without currency, gets `amount_usd = null` and `fx_status` says why; nothing is guessed.
* **Public holidays**: [Nager.Date](https://date.nager.at). Regional holidays are matched on `country-state` in `counties`. Nager does not cover some countries (AE, SA, IL, IN, ...). For these the flag is **null**, not false, using `ref/holiday_coverage` to tell "no holiday" from "unknown".
* Both HTTP calls have retries and timeouts from the config, and are injectable for tests.

## Data problems found

All of them are counted in [`reports/dq_summary.md`](reports/dq_summary.md) (only checks that found something; full per-day results in `dq_report.csv`).

* **Calendar**: 62 folders, the README says 63; `dt=2026-10-03` is missing and `2026-10-04` carries about two days of events (reported as `missing_day_folder`). A row's folder date can differ from its `created_at` date.
* **Corrupt JSON lines** in requests (20 in total, on 2 days). Rejected and listed.
* **Duplicates across folders**: 50 postbacks repeat a postback of an earlier folder. Rejected and listed.
* **Re-renders**: one `fid` has several impression rows, so the fact collapses them to one row (`n_impression_rows` keeps the count).
* **Late events**: clicks and postbacks arrive days after the impression; some impressions and requests arrive late as well (36k impressions, 24k requests, 14k orders on the 2026-10-04 folder alone).
* **Orphans**: clicks and postbacks without an impression yet, impressions without a request, impressions before their request, impression placement different from the request placement (about 7.5k slots; the impression's is used).
* **Dimension problems**: a placement snapshot can list one id twice with different templates (43 extra rows, newest `updated_at` wins); banners and placements missing in the dimension (24k impressions); missing enrichment for about a quarter of the slots.
* **Orders and requests**: order amount and currency are often null (about 30% of requests), amount without currency, zero/negative amounts. The `orders` table would fill only ~0.2% of the gaps, so it is not used for that.
* **Preview-tool requests** (`has_preview`, 74.6k) are kept in the fact and excluded from the mart.
* **Postbacks** with revenue 0 (146, kept, counted as conversion and flagged); some slots with more than 100 clicks (threshold `dq.max_events_per_slot` in the config; reported in `dq_final_state.md`).
* **Holiday coverage**: no data for several countries (null flag).

## Decisions and why

* **Arrival-day partitioning of events, request-day partitioning of facts.** The first keeps "what we knew on day D" reproducible and makes dedup against earlier folders well-defined; the second is what analysts and training jobs filter on.
* **Touched-slot rewrite instead of recomputing history.** Cost per day is proportional to what arrived, not to everything loaded; the result is a pure function of the event layer, so reruns are identical.
* **Nothing is dropped silently.** Rejected rows go to `dq/rejects`; everything else is kept and flagged. Left joins everywhere with `*_found` flags. `dq/reconciliation` proves per day and cumulatively that *staged impressions = impression rows of slots in `fact_impression` + rejected* (and the same for clicks, postbacks, requests, orders, mart rows). The numbers are as of the load day, so a replay writes the same lines. Result: [`reports/reconciliation_per_day.md`](reports/reconciliation_per_day.md), no failing line.
* **No guessing in features.** Missing currency means null amount; unknown holiday country means null; empty history window means null rate.
* **No collect on big data.** `collect()` is only used on small lists (dates of a day's partitions, FX currencies). No Python UDFs; user agents are parsed with built-in column expressions.
* **Job is a function of one date.** `load_day(spark, cfg, dt)`; the CLI and the scheduler-style `run_all.sh` only call it.

## Limits (please read)

* **The mart is not a leakage-free historical backtest.** The source keeps no history of dimension or enrichment attributes: a snapshot shows every row existing on that day with the values it has *today*. I use the snapshot of the request day as the "known" state, as the dataset README suggests, but placement/banner/site/campaign attributes still carry today's values. Everything built from *events* (history features, labels, arrival times) does respect arrival times.
* **Labels of the last days are immature.** A conversion can come days after the impression, so the most recent request days have fewer positives than they will eventually have. Train on matured days or add a maturity cutoff.
* **Same-day arrivals are ignored for history features** (conservative, on purpose).
* **Currency coverage**: orders in currencies with no rate in either source have `amount_usd = null`.
* **Holidays** are unknown for countries Nager.Date does not cover.

## What I would do differently at 100x data

* A table format with `MERGE` (Delta/Iceberg/Hudi) instead of partition-level rewrite, so a late click rewrites a file group, not a day partition.
* Keep dedup state (keys seen so far) in a compact keyed table or use bloom filters / bucketing on `fid` and the event keys, rather than joining against all earlier folders; partition pruning and file compaction on `fid`.
* Compute the publisher history incrementally (daily aggregates per publisher x request day, then window sums) rather than joining back to the slot-level fact.
* Run on a real cluster with adaptive execution, skew handling for large publishers, an orchestrator (Airflow/Dagster) with one task per day and retries, and metrics/alerts on the DQ results instead of a report.
* Replace the small driver-side `collect()` calls on date lists with metadata queries; fetch FX and holidays in a separate scheduled job with its own history table.
* Add a real point-in-time source for dimensions (SCD2 or CDC from the production database) to make the mart a true backtest.
* A maturity-aware label table (labels as of N days after the request) for training.

## Run outputs (in `reports/`)

| file | what |
|---|---|
| `full_run_log.txt` | log of the full run of all 62 folders |
| `idempotency_proof.md/json` | fingerprints of every table before and after re-running a day |
| `dq_report.csv`, `dq_summary.md`, `dq_final_state.md` | the data-quality report (all checks per day; summary of findings; state of the warehouse) |
| `dq_reconciliation.csv`, `reconciliation_per_day.md` | row reconciliation per day and table |
| `table_row_counts.md` | row counts of every output table |
| `history_verification.md` | recomputation of history features from raw data |
