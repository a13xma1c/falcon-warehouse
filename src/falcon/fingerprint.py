"""Warehouse-wide fingerprints used by the idempotency proof and the integration test."""
from __future__ import annotations
from pyspark.sql import SparkSession, functions as F

from falcon.common import list_partitions, read_table

# table -> numeric columns whose sums are reported next to the row count
TABLES = {
    "staging/requests": [], "staging/impressions": [], "staging/clicks": [], "staging/postbacks": ["revenue"],
    "staging/orders": ["amount"],
    "dw/dim_placement": [], "dw/dim_banner": [], "dw/dim_site_enrichment": [], "dw/dim_campaign_enrichment": [],
    "dw/ev_request": ["order_amount"], "dw/ev_impression": [], "dw/ev_click": [], "dw/ev_postback": ["revenue"],
    "dw/ev_order": ["amount"],
    "dw/fact_impression": ["n_impression_rows", "n_clicks", "n_postbacks", "revenue_usd"],
    "dw/fact_request": ["amount_usd", "n_slots", "n_clicked_slots", "n_converted_slots", "revenue_usd"],
    "dw/fact_order": ["amount_usd"],
    "mart/ml_training": ["is_click", "is_conversion", "order_amount_usd", "pub_conv_rate_7d", "pub_conv_rate_28d"],
    "dq/rejects": [], "dq/dq_checks": ["n_bad", "n_total"], "dq/reconciliation": ["left_rows", "right_rows"],
    "ref/fx_daily": ["rate_per_usd"], "ref/holiday": [],
}


def fingerprint(spark: SparkSession, wh: str, only_dts: list[str] | None = None) -> dict:
    """{table: {rows, sums{col: value}, hash}} -- hash is an order-independent checksum of all columns."""
    out = {}
    for t, num in TABLES.items():
        df = read_table(spark, f"{wh}/{t}")
        if df is None:
            continue
        if only_dts is not None and "dt" in df.columns:
            df = df.filter(F.col("dt").isin(only_dts))
        aggs = [F.count(F.lit(1)).alias("rows"), F.sum(F.xxhash64(*[F.col(c) for c in sorted(df.columns)])).alias("hash")]
        aggs += [F.round(F.sum(F.col(c).cast("double")), 4).alias(f"sum_{c}") for c in num if c in df.columns]
        r = df.agg(*aggs).first().asDict()
        out[t] = {"rows": int(r.pop("rows")), "hash": int(r.pop("hash") or 0), "sums": {k[4:]: v for k, v in r.items()}}
    return out


def diff(a: dict, b: dict) -> list[str]:
    msgs = []
    for t in sorted(set(a) | set(b)):
        if a.get(t) != b.get(t):
            msgs.append(f"{t}: {a.get(t)} -> {b.get(t)}")
    return msgs


def partition_counts(spark: SparkSession, wh: str, table: str) -> dict[str, int]:
    df = read_table(spark, f"{wh}/{table}")
    return {} if df is None else {r["dt"]: r["n"] for r in df.groupBy("dt").agg(F.count(F.lit(1)).alias("n")).collect()}
