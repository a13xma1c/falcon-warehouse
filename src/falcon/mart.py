"""ML training mart.

    ml_training   grain: fid (one shown slot).  Key: fid.  Partition dt = UTC date of the request.
                  Rows: slots whose request is known and is not a preview-tool request.
                  Labels   : is_click, is_conversion      (as observed so far; recent days are not mature)
                  Features : everything known at the moment of the request.

No-leakage rules
----------------
* Request context, placement/banner attributes and enrichment come from data that existed at request time
  (dimension snapshot of the request day or the newest earlier one; enrichment rows with created_at <= request time).
  The snapshots carry today's attribute values, so this is *not* a leakage-free historical backtest (see README).
* Nothing is taken from the impression row except slot position (fixed when the request is made) and the
  banner/placement shown.  Impression time, status, number of renders, clicks and conversions are NOT features.
* History features of a request on day E use only events that had ARRIVED in a folder before day E
  (arrival_dt < E) and slots whose request day is in [E-w, E-1].  A conversion that is reported on day E or later
  does not count for E, even if it belongs to a slot of E-3.  This is conservative (same-day arrivals are ignored)
  and makes the features identical no matter when the mart row is rebuilt.
* Labels are the only columns that may change after the first load (late clicks / conversions).
"""
from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from falcon import external
from falcon.common import merge_partitions
from falcon.dq import Collector, Spec
from falcon.facts import attach_snapshot, dim_asof, ev

PLACEMENT_COLS = {
    "placement_type": "placement_type", "placement_template": "placement_template",
    "is_test_mode": "placement_is_test_mode", "surface_type": "placement_surface_type",
    "user_initiated": "placement_user_initiated", "site_category_name": "site_category",
    "site_parent_category_name": "site_parent_category", "site_ads_enabled": "site_ads_enabled",
    "site_order_volume": "site_order_volume", "site_average_order": "site_average_order",
    "platform_name": "platform", "publisher_rev_share_pct": "publisher_rev_share_pct",
    "publisher_status": "publisher_status"}
BANNER_COLS = {
    "banner_type": "banner_type", "banner_weight": "banner_weight", "banner_is_active": "banner_is_active",
    "campaign_category_name": "campaign_category", "campaign_weight": "campaign_weight",
    "campaign_priority": "campaign_priority", "campaign_cpa": "campaign_cpa", "cpa_source": "campaign_cpa_source",
    "advertiser_payout": "advertiser_payout"}
ENRICH_COLS = ["main_category", "subcategory", "desktop_traffic_share", "mobile_traffic_share", "male_share",
               "female_share", "age_18_to_24", "age_25_to_34", "age_35_to_44", "age_45_to_54", "age_55_to_64",
               "age_65_plus", "top_country"]


# ═══════════════════════════ enrichment: newest row that existed at request time ═══════════

def with_enrichment(spark, wh, df: DataFrame, dt: str, table: str, key_col: str, enr_key: str, prefix: str) -> DataFrame:
    """Add ``<prefix>_*`` attributes of the newest enrichment row with created_at <= request_at, taken from the
    enrichment snapshot of the request day (or the newest earlier one).  Rows without one keep nulls."""
    df = attach_snapshot(spark, wh, df, table, "dt", dt, f"{prefix}_snapshot_dt")
    snaps = [r[0] for r in df.select(f"{prefix}_snapshot_dt").where(F.col(f"{prefix}_snapshot_dt").isNotNull())
             .distinct().collect()]
    e = (ev(spark, wh, table).filter(F.col("dt").isin(snaps))
         .select(F.col(enr_key).alias("_ek"), F.col("dt").alias("_es"), F.col("created_at").alias("_ec"),
                 *[F.col(c).alias(f"_e_{c}") for c in ENRICH_COLS]))
    j = df.join(F.broadcast(e), (df[key_col] == F.col("_ek")) & (df[f"{prefix}_snapshot_dt"] == F.col("_es"))
                & (F.col("_ec") <= df["request_at"]), "left")
    w = Window.partitionBy("fid").orderBy(F.col("_ec").desc_nulls_last())
    j = j.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    for c in ENRICH_COLS:
        j = j.withColumnRenamed(f"_e_{c}", f"{prefix}_{c}")
    return (j.withColumn(f"{prefix}_enrichment_age_days", F.datediff(F.col("request_at"), F.col("_ec")))
            .withColumn(f"{prefix}_enrichment_found", F.col("_ec").isNotNull())
            .drop("_ek", "_es", "_ec"))


# ═══════════════════════════ history: arrival-aware conversion / click rates ═══════════════

def history_features(spark, wh, rows: DataFrame, windows: list[int], group_col: str, prefix: str) -> DataFrame:
    """Per (group, request day E) rates over [E-w, E-1], counting only what had arrived before E.

    Returns one row per (group, dt) with ``<prefix>_slots_{w}d``, ``<prefix>_click_rate_{w}d``,
    ``<prefix>_conv_rate_{w}d``.  Rates are null when the window has no slots (never 0/0)."""
    wmax = max(windows)
    pairs = rows.select(group_col, "dt").where(F.col(group_col).isNotNull()).distinct()
    fi = (spark.read.parquet(f"{wh}/dw/fact_impression")
          .where(F.col("request_found") & ~F.col("has_preview"))
          .select(group_col, F.col("dt").alias("_fdt"), "slot_arrival_dt", "request_arrival_dt",
                  "is_click", "first_click_arrival_dt", "is_conversion", "first_conv_arrival_dt"))
    p = pairs.withColumnRenamed("dt", "_e")
    lo = F.date_format(F.date_sub(F.to_date("_e"), wmax), "yyyy-MM-dd")
    j = p.join(fi, (p[group_col] == fi[group_col]) & (F.col("_fdt") >= lo) & (F.col("_fdt") < F.col("_e")),
               "left").drop(fi[group_col])
    known = (F.col("slot_arrival_dt") < F.col("_e")) & (F.col("request_arrival_dt") < F.col("_e"))
    aggs = []
    for w in windows:
        inw = known & (F.col("_fdt") >= F.date_format(F.date_sub(F.to_date("_e"), w), "yyyy-MM-dd"))
        n = F.sum(F.when(inw, 1).otherwise(0))
        c = F.sum(F.when(inw & F.col("is_click") & (F.col("first_click_arrival_dt") < F.col("_e")), 1).otherwise(0))
        v = F.sum(F.when(inw & F.col("is_conversion") & (F.col("first_conv_arrival_dt") < F.col("_e")), 1).otherwise(0))
        aggs += [n.alias(f"{prefix}_slots_{w}d"),
                 F.when(n > 0, c / n).alias(f"{prefix}_click_rate_{w}d"),
                 F.when(n > 0, v / n).alias(f"{prefix}_conv_rate_{w}d")]
    return (j.groupBy(group_col, "_e").agg(*aggs).withColumnRenamed("_e", "dt"))


# ═══════════════════════════ builder ═══════════════════════════════════════

def build_mart(spark, wh, cfg, slots: DataFrame, dt: str) -> DataFrame:
    """Mart rows for the rebuilt *slots* (output of facts.build_slots)."""
    s = slots.where(F.col("request_found") & ~F.col("has_preview"))
    s = s.persist()           # released by spark.catalog.clearCache() at the end of the day
    # request context (USD amount etc.) from fact_request
    days = [r[0] for r in s.select("dt").distinct().collect()]
    fr = (spark.read.parquet(f"{wh}/dw/fact_request").where(F.col("dt").isin(days))
          .select("request_id", "device_type", "os", "browser", "order_amount_raw", "order_amount",
                  "order_currency", "amount_usd", "fx_status", "language", "template", "offer_count",
                  "has_partner_offer", "is_gdpr_user", "payment_type", "province_code",
                  (F.col("customer_id").isNotNull()).alias("has_customer_id"),
                  (F.col("has_hashed_email") | F.col("has_hashed_phone") | F.col("has_hashed_shopify_id")).alias("has_user_identifier"),
                  F.col("country").alias("req_country"), F.col("state").alias("req_state"),
                  F.col("fx_rate_per_usd")))
    m = s.join(fr, "request_id", "left")

    # placement / banner attributes as-of the request day
    m = dim_asof(spark, wh, m, "dim_placement", "placement_id", "placement_id", "placement_snapshot_dt", PLACEMENT_COLS)
    m = dim_asof(spark, wh, m, "dim_banner", "banner_id", "banner_id", "banner_snapshot_dt", BANNER_COLS)

    # enrichment
    m = with_enrichment(spark, wh, m, dt, "dim_site_enrichment", "site_id", "site_id", "site")
    m = with_enrichment(spark, wh, m, dt, "dim_campaign_enrichment", "campaign_id", "campaign_id", "campaign")

    # time features (UTC) + holiday
    m = (m.withColumn("req_date", F.to_date("request_at"))
         .withColumn("req_hour", F.hour("request_at"))
         .withColumn("req_day_of_week", F.dayofweek("request_at"))
         .withColumn("req_is_weekend", F.dayofweek("request_at").isin(1, 7))
         .withColumn("req_day_of_month", F.dayofmonth("request_at"))
         .withColumn("req_month", F.month("request_at")))
    hol = spark.read.parquet(f"{wh}/ref/holiday")
    cov = spark.read.parquet(f"{wh}/ref/holiday_coverage")
    m = external.with_holiday_flag(m, "req_country", "req_state", "req_date", hol, cov).drop("req_date")

    # history (publisher level)
    hist = history_features(spark, wh, m, cfg["history"]["windows_days"], "publisher_id", "pub")
    m = m.join(F.broadcast(hist), ["publisher_id", "dt"], "left")

    cols = ([
        "fid", "request_id", "dt", "request_at",
        # labels
        F.col("is_click").cast("int").alias("is_click"), F.col("is_conversion").cast("int").alias("is_conversion"),
        # request context
        "position", F.col("req_country").alias("country"), F.col("req_state").alias("state"),
        F.col("amount_usd").alias("order_amount_usd"), "order_currency", "fx_status",
        "device_type", "os", "browser", "language", F.col("template").alias("request_template"),
        "offer_count", "has_partner_offer", "is_gdpr_user", "payment_type", "has_customer_id", "has_user_identifier",
        # time
        "req_hour", "req_day_of_week", "req_is_weekend", "req_day_of_month", "req_month", "is_public_holiday",
        # ids (for grouping / audit)
        "placement_id", "site_id", "publisher_id", "banner_id", "campaign_id", "advertiser_id"]
        + [F.col(v) for v in PLACEMENT_COLS.values()] + [F.col(v) for v in BANNER_COLS.values()]
        + [F.col(f"site_{c}") for c in ENRICH_COLS] + ["site_enrichment_age_days", "site_enrichment_found"]
        + [F.col(f"campaign_{c}") for c in ENRICH_COLS] + ["campaign_enrichment_age_days", "campaign_enrichment_found"]
        + [c for w in cfg["history"]["windows_days"] for c in
           (f"pub_slots_{w}d", f"pub_click_rate_{w}d", f"pub_conv_rate_{w}d")]
        + ["placement_found", "banner_found"]
        # audit columns: used by the DQ checks of the load, dropped before the table is written
        + ["slot_arrival_dt", "request_arrival_dt", "first_click_at", "first_conversion_at"])
    return m.select(*cols)


AUDIT_COLS = ["slot_arrival_dt", "request_arrival_dt", "first_click_at", "first_conversion_at"]


def check_mart(dq: Collector, mart: DataFrame, dt: str) -> None:
    """Checks on the mart rows that became complete today (slot AND request have both arrived, the later of the
    two arrivals is today), so that summing a check over days counts every row once."""
    k = F.col("fid")
    new = mart.where(F.greatest("slot_arrival_dt", "request_arrival_dt") == dt)
    dq.run(new, "mart", "ml_training", [
        Spec("order_amount_usd_null", F.col("order_amount_usd").isNull(), k, "kept; null feature (reason in fx_status)"),
        Spec("fx_no_rate", F.col("fx_status") == "no_rate", F.col("order_currency"), "kept; amount_usd null"),
        Spec("device_type_unknown", F.col("device_type") == "unknown", k, "kept"),
        Spec("holiday_unknown_country", F.col("is_public_holiday").isNull(), F.col("country"), "kept; null (country not covered / missing)"),
        Spec("placement_attributes_missing", ~F.col("placement_found"), k, "kept; null attributes"),
        Spec("banner_attributes_missing", ~F.col("banner_found"), k, "kept; null attributes"),
        Spec("site_enrichment_missing", ~F.col("site_enrichment_found"), k, "kept; null attributes"),
        Spec("campaign_enrichment_missing", ~F.col("campaign_enrichment_found"), k, "kept; null attributes"),
        Spec("history_empty_window_7d", F.col("pub_slots_7d").isNull() | (F.col("pub_slots_7d") == 0), F.col("publisher_id"),
             "kept; null rate (no earlier data, e.g. first days of the load)"),
    ])


def load_mart_day(spark, wh, cfg, dq: Collector, dt: str, slots: DataFrame, touched_fids: DataFrame) -> DataFrame:
    mart = build_mart(spark, wh, cfg, slots, dt).persist()
    check_mart(dq, mart, dt)
    merge_partitions(spark, f"{wh}/mart/ml_training", mart.drop(*AUDIT_COLS), "fid", touched_fids, "dt")
    return mart
