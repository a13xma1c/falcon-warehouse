# Idempotency proof: re-running 2026-09-15 on the fully loaded warehouse

Result: **IDENTICAL** (job took 77.9 s)

| table | rows before | rows after | sums before | sums after | checksum equal |
|---|---:|---:|---|---|---|
| staging/requests | 1,939,685 | 1,939,685 | {} | {} | True |
| staging/impressions | 2,552,611 | 2,552,611 | {} | {} | True |
| staging/clicks | 240,103 | 240,103 | {} | {} | True |
| staging/postbacks | 6,226 | 6,226 | {'revenue': 252704.25} | {'revenue': 252704.25} | True |
| staging/orders | 992,663 | 992,663 | {'amount': 813561544.0699} | {'amount': 813561544.0699} | True |
| dw/dim_placement | 107,387 | 107,387 | {} | {} | True |
| dw/dim_banner | 71,587 | 71,587 | {} | {} | True |
| dw/dim_site_enrichment | 92,457 | 92,457 | {} | {} | True |
| dw/dim_campaign_enrichment | 18,739 | 18,739 | {} | {} | True |
| dw/ev_request | 1,939,665 | 1,939,665 | {'order_amount': 1308197976.91} | {'order_amount': 1308197976.91} | True |
| dw/ev_impression | 2,552,611 | 2,552,611 | {} | {} | True |
| dw/ev_click | 240,103 | 240,103 | {} | {} | True |
| dw/ev_postback | 6,176 | 6,176 | {'revenue': 248376.85} | {'revenue': 248376.85} | True |
| dw/ev_order | 992,663 | 992,663 | {'amount': 813561544.07} | {'amount': 813561544.07} | True |
| dw/fact_impression | 2,123,218 | 2,123,218 | {'n_impression_rows': 2552611.0, 'n_clicks': 239234.0, 'n_postbacks': 6117.0, 'revenue_usd': 247751.41} | {'n_impression_rows': 2552611.0, 'n_clicks': 239234.0, 'n_postbacks': 6117.0, 'revenue_usd': 247751.41} | True |
| dw/fact_request | 1,939,665 | 1,939,665 | {'amount_usd': 1305781151.549, 'n_slots': 2123200.0, 'n_clicked_slots': 83812.0, 'n_converted_slots': 5300.0, 'revenue_usd': 247751.41} | {'amount_usd': 1305781151.549, 'n_slots': 2123200.0, 'n_clicked_slots': 83812.0, 'n_converted_slots': 5300.0, 'revenue_usd': 247751.41} | True |
| dw/fact_order | 992,663 | 992,663 | {'amount_usd': 892801805.5052} | {'amount_usd': 892801805.5052} | True |
| mart/ml_training | 2,051,050 | 2,051,050 | {'is_click': 83319.0, 'is_conversion': 5295.0, 'order_amount_usd': 1224674921.6603, 'pub_conv_rate_7d': 4873.4522, 'pub_conv_rate_28d': 4810.3906} | {'is_click': 83319.0, 'is_conversion': 5295.0, 'order_amount_usd': 1224674921.6603, 'pub_conv_rate_7d': 4873.4522, 'pub_conv_rate_28d': 4810.3906} | True |
| dq/rejects | 70 | 70 | {} | {} | True |
| dq/dq_checks | 7,627 | 7,627 | {'n_bad': 5741913.0, 'n_total': 139626824.0} | {'n_bad': 5741913.0, 'n_total': 139626824.0} | True |
| dq/reconciliation | 1,798 | 1,798 | {'left_rows': 286960469.0, 'right_rows': 286960469.0} | {'left_rows': 286960469.0, 'right_rows': 286960469.0} | True |
| ref/fx_daily | 2,810 | 2,810 | {'rate_per_usd': 3014935.372} | {'rate_per_usd': 3014935.372} | True |
| ref/holiday | 2,179 | 2,179 | {} | {} | True |
