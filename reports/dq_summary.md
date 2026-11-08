# Data quality summary (62 days loaded)

Only checks that found something. Row counts are summed over days (a late record is counted on the day it was loaded). Full per-day results: `dq_report.csv`.

| layer | table | check | rows | days | first day | example | what the pipeline did |
|---|---|---|---|---|---|---|---|
| dim | dim_placement | duplicate_id_in_snapshot | 43 | 43 | 2026-09-12 | 3d2c6824fe8d1a8a | kept the row with the newest updated_at (then created_at, then row hash) |
| event | ev_click | arrived_after_event_day | 1,664 | 1 | 2026-10-04 | 00204bf297f398a9 | kept; late |
| event | ev_impression | arrived_after_event_day | 36,169 | 3 | 2026-09-17 | 0017efec62733493 | kept; partitioned by arrival day (late) |
| event | ev_impression | banner_id_null | 269 | 25 | 2026-08-27 | 0dfcf8648a25c6bf | kept; banner dimension not found |
| event | ev_order | amount_null | 306,182 | 62 | 2026-08-24 | 001d06dc174f1eef | kept |
| event | ev_order | currency_null | 277,711 | 62 | 2026-08-24 | 001d06dc174f1eef | kept; amount_usd null |
| event | ev_order | arrived_after_event_day | 14,067 | 1 | 2026-10-04 | 00028ea0618f4257 | kept; late |
| event | ev_order | amount_non_positive | 2,814 | 62 | 2026-08-24 | 013254bcb0227633 | kept; amount_usd null |
| event | ev_order | placement_id_null | 992 | 62 | 2026-08-24 | 05e221d85490f8ba | kept (documented as possible) |
| event | ev_postback | revenue_zero | 146 | 19 | 2026-08-24 | 0f6933f2a0f1d0eb | kept; counted as conversion, flagged |
| event | ev_postback | arrived_after_event_day | 54 | 1 | 2026-10-04 | 02803411fe6711b3 | kept; late |
| event | ev_postback | rejected_duplicate_of_earlier_day | 50 | 30 | 2026-09-04 | fcbc807af262a6df | rejected, listed in dq/rejects |
| event | ev_request | customer_id_null | 847,475 | 62 | 2026-08-24 | 00055b4e4807272d | kept (normal, anonymous shopper) |
| event | ev_request | order_amount_null | 597,173 | 62 | 2026-08-24 | 00026d5232f0210e | kept; amount_usd null (orders table would fill only ~0.2% of gaps, not used) |
| event | ev_request | order_currency_null | 566,332 | 62 | 2026-08-24 | 00026d5232f0210e | kept; amount_usd null (orders table would fill only ~0.2% of gaps, not used) |
| event | ev_request | order_id_null | 278,585 | 62 | 2026-08-24 | 000fca72a9303ddf | kept |
| event | ev_request | session_id_null | 117,258 | 62 | 2026-08-24 | 0071413dfe1cf694 | kept |
| event | ev_request | has_preview_true | 74,639 | 29 | 2026-09-26 | 086d73e3c5c286bc | kept, flagged; excluded from ML mart |
| event | ev_request | order_amount_without_currency | 35,077 | 62 | 2026-08-24 | 000fca72a9303ddf | kept; amount_usd null (no guessing) |
| event | ev_request | arrived_after_event_day | 24,041 | 1 | 2026-10-04 | 000933362eff2ee2 | kept; partitioned by arrival day (late) |
| event | ev_request | order_amount_non_positive | 8,612 | 62 | 2026-08-24 | 023acf39747c86c8 | kept; amount_usd null (fx_status=non_positive) |
| event | ev_request | placement_id_null | 1,939 | 62 | 2026-08-24 | 14f085df50b4fc8a | kept; no placement dimension row |
| event | ev_request | user_agent_browser_other | 113 | 46 | 2026-08-24 | Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) | kept, browser=other/unknown |
| event | ev_request | rejected_corrupt_json_line | 20 | 2 | 2026-09-04 | NULL | rejected, listed in dq/rejects |
| event | ev_request | user_agent_device_unknown | 11 | 2 | 2026-08-25 | Mozilla/5.0 (compatible; OrderEditing/1.0) | kept, device_type=unknown |
| event | ev_request | user_agent_os_other | 11 | 2 | 2026-08-25 | Mozilla/5.0 (compatible; OrderEditing/1.0) | kept, os=other/unknown |
| fact | fact_impression | banner_not_in_dimension | 24,029 | 62 | 2026-08-24 | M2EzNGI5NDBiYWUzOTRhNTow | kept; campaign/advertiser null |
| fact | fact_impression | impression_before_request | 15,370 | 48 | 2026-08-25 | YjQ4MGQwOTMwMWU2YmMxYTow | kept; features use request time |
| fact | fact_impression | impression_placement_differs_from_request | 7,487 | 29 | 2026-09-26 | MDg2ZDczZTNjNWMyODZiYzow | kept; impression placement used for slot |
| fact | fact_impression | clicks_without_impression_on_arrival | 869 | 62 | 2026-08-24 | MDkyOTg1MzQ2YmU4NzJhZjow | kept in ev_*; no slot to attach to yet; attached automatically if the impression arrives later |
| fact | fact_impression | slot_without_request | 609 | 34 | 2026-09-04 | MDA0ODlmYTFhMjUxNDdiZDow | kept; request attributes null; becomes trainable if the request arrives later |
| fact | fact_impression | placement_not_in_dimension | 465 | 62 | 2026-08-24 | MDAwNTViNGU0ODA3MjcyZDow | kept; dimension columns null, flag placement_found=false |
| fact | fact_impression | postbacks_without_impression_on_arrival | 59 | 35 | 2026-08-27 | NDY3ZmU1MGNkMWU4ZjE0MTow | kept in ev_*; no slot to attach to yet; attached automatically if the impression arrives later |
| fact | fact_order | fx_no_amount | 306,182 | 62 | 2026-08-24 | 001d06dc174f1eef/17aa8c0a67aa2114 | kept; amount_usd null |
| fact | fact_order | fx_non_positive | 2,814 | 62 | 2026-08-24 | 013254bcb0227633/15f8983194ca5c97 | kept; amount_usd null |
| fact | fact_order | site_not_in_dimension | 381 | 62 | 2026-08-24 | 02e452cbfcb84415/6a5edf303473ac8d | kept; flagged only (site table is joined through placement) |
| fact | fact_request | fx_no_amount | 597,173 | 62 | 2026-08-24 | 00026d5232f0210e | kept; amount_usd null |
| fact | fact_request | fx_no_currency | 33,708 | 62 | 2026-08-24 | 000fca72a9303ddf | kept; amount_usd null |
| fact | fact_request | fx_non_positive | 8,612 | 62 | 2026-08-24 | 023acf39747c86c8 | kept; amount_usd null |
| fact | fact_request | placement_not_in_dimension | 385 | 62 | 2026-08-24 | 00055b4e4807272d | kept; site/publisher null, flags false |
| mart | ml_training | order_amount_usd_null | 789,181 | 62 | 2026-08-24 | M2E0MTI0M2U2NDk0YzM3ZTow | kept; null feature (reason in fx_status) |
| mart | ml_training | site_enrichment_missing | 492,731 | 62 | 2026-08-24 | M2E0MjFlNGIzNzRkMmM4MTow | kept; null attributes |
| mart | ml_training | campaign_enrichment_missing | 215,294 | 62 | 2026-08-24 | M2E1MzcxNTUzNThmM2MxZTo0 | kept; null attributes |
| mart | ml_training | history_empty_window_7d | 24,356 | 62 | 2026-08-24 | 1df029418ed2185d | kept; null rate (no earlier data, e.g. first days of the load) |
| mart | ml_training | banner_attributes_missing | 23,900 | 62 | 2026-08-24 | M2EzNGI5NDBiYWUzOTRhNTow | kept; null attributes |
| mart | ml_training | holiday_unknown_country | 6,394 | 62 | 2026-08-24 | AE | kept; null (country not covered / missing) |
| mart | ml_training | placement_attributes_missing | 465 | 62 | 2026-08-24 | MDAwNTViNGU0ODA3MjcyZDow | kept; null attributes |
| mart | ml_training | device_type_unknown | 1 | 1 | 2026-09-20 | N2Q1M2U1MjM1M2FhMzAzMTow | kept |
| staging | folder | missing_day_folder | 1 | 1 | 2026-10-04 | 2026-10-03 | reported; events of the missing day arrive in the next folder (partitioned by event time) |
