# Row counts of all output tables after the full run

| table | rows | dt partitions |
|---|---|---|
| staging/requests | 1,939,685 | 62 |
| staging/impressions | 2,552,611 | 62 |
| staging/clicks | 240,103 | 62 |
| staging/postbacks | 6,226 | 62 |
| staging/orders | 992,663 | 62 |
| staging/dim_publisher | 2,001 | 62 |
| staging/dim_site | 105,236 | 62 |
| staging/dim_placement | 107,430 | 62 |
| staging/dim_platform | 1,116 | 62 |
| staging/dim_category | 7,130 | 62 |
| staging/dim_banner | 71,587 | 62 |
| staging/dim_campaign | 20,373 | 62 |
| staging/dim_advertiser | 11,315 | 62 |
| staging/dim_site_enrichment | 92,457 | 62 |
| staging/dim_campaign_enrichment | 18,739 | 62 |
| dw/dim_placement | 107,387 | 62 |
| dw/dim_banner | 71,587 | 62 |
| dw/dim_site_enrichment | 92,457 | 62 |
| dw/dim_campaign_enrichment | 18,739 | 62 |
| dw/ev_request | 1,939,665 | 62 |
| dw/ev_impression | 2,552,611 | 62 |
| dw/ev_click | 240,103 | 62 |
| dw/ev_postback | 6,176 | 62 |
| dw/ev_order | 992,663 | 62 |
| dw/fact_impression | 2,123,218 | 63 |
| dw/fact_request | 1,939,665 | 63 |
| dw/fact_order | 992,663 | 63 |
| mart/ml_training | 2,051,050 | 63 |
| dq/dq_checks | 7,627 | 62 |
| dq/reconciliation | 1,798 | 62 |
| dq/rejects | 70 | 30 |
| ref/fx_raw | 2,061 |  |
| ref/fx_daily | 2,810 |  |
| ref/holiday | 2,179 |  |
| ref/holiday_coverage | 204 |  |
