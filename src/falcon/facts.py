"""Fact tables.  All are partitioned by ``dt`` = UTC date of the REQUEST (event day).

    fact_impression  grain: fid (one shown slot).  Key: fid.
                     Several impression rows of one slot are collapsed (first render is the reference row;
                     n_impression_rows keeps the count).  Click and conversion outcomes are attached here.
                     Dimension keys are resolved as-of the request day (see ``dim_snapshot`` columns).
    fact_request     grain: request_id.  Key: request_id.  Order data of the request in USD plus outcome totals.
    fact_order       grain: (order_id, site_id).  Key: order_key = order_id|site_id.  Amount in USD.

Late events
-----------
Clicks, conversions, late impressions and late requests change rows that were written on earlier days.
On every load day D we compute the *touched slots* and rebuild exactly those rows from the event layer:

    touched fids = fids of impressions / clicks / postbacks that arrived in folder D
                 + fids of earlier impressions whose request arrived in folder D

The rebuilt rows replace the old ones (``merge_partitions``): only the partitions that held an old row or
receive a new row are rewritten, so old days are updated and untouched days are not.  The result depends
only on the event layer, so running a day twice gives the same tables.
"""
from __future__ import annotations
import datetime as _d
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from falcon import external
from falcon.common import (list_partitions, merge_partitions, read_table, snapshot_calendar)
from falcon.dq import Collector, Spec
from falcon.events import stg  # noqa: F401  (re-export for convenience)


def ev(spark: SparkSession, wh: str, table: str) -> DataFrame:
    return spark.read.parquet(f"{wh}/dw/{table}")


def ev_day(spark, wh, table, dt) -> DataFrame:
    return ev(spark, wh, table).filter(F.col("dt") == dt)


# ═══════════════════════════ dimension as-of helpers ═══════════════════════

def attach_snapshot(spark, wh, df: DataFrame, dim_table: str, date_col: str, dt: str, out_col: str) -> DataFrame:
    """Add *out_col* = newest dimension snapshot dt that is <= the date in *date_col* (null if none)."""
    snaps = list_partitions(spark, f"{wh}/dw/{dim_table}", "dt")
    cal = F.broadcast(snapshot_calendar(spark, [s for s in snaps if s <= dt], dt))
    return (df.join(cal, df[date_col].cast("string") == cal["cal_dt"], "left")
            .withColumnRenamed("snap_dt", out_col).drop("cal_dt"))


def dim_asof(spark, wh, df: DataFrame, dim_table: str, key_col: str, dim_key: str, snap_col: str,
             select: dict[str, str]) -> DataFrame:
    """Left join the snapshot (dim_key, dt = snap_col) of *dim_table*; *select* maps dim column -> output name."""
    snaps = [r[0] for r in df.select(snap_col).where(F.col(snap_col).isNotNull()).distinct().collect()]  # <= 62 values
    dim = ev(spark, wh, dim_table).filter(F.col("dt").isin(snaps))
    cols = [F.col(dim_key).alias("_dk"), F.col("dt").alias("_ds")] + [F.col(k).alias(v) for k, v in select.items()]
    return (df.join(F.broadcast(dim.select(*cols)), (df[key_col] == F.col("_dk")) & (df[snap_col] == F.col("_ds")), "left")
            .drop("_dk", "_ds"))


# ═══════════════════════════ touched slots ═════════════════════════════════

def touched_fids(spark, wh, dt) -> DataFrame:
    new_req = ev_day(spark, wh, "ev_request", dt).select("request_id")
    late = (ev(spark, wh, "ev_impression").select("fid", "request_id")
            .join(F.broadcast(new_req), "request_id").select("fid"))
    return (ev_day(spark, wh, "ev_impression", dt).select("fid")
            .unionByName(ev_day(spark, wh, "ev_click", dt).select("fid"))
            .unionByName(ev_day(spark, wh, "ev_postback", dt).select("fid"))
            .unionByName(late).distinct())


# ═══════════════════════════ fact_impression ═══════════════════════════════

def build_slots(spark, wh, tf: DataFrame, dt: str) -> DataFrame:
    """Rebuild the fact_impression rows for the slots in *tf* (column fid) from the event layer."""
    tfb = F.broadcast(tf)
    imp = ev(spark, wh, "ev_impression").join(tfb, "fid")
    first = F.min(F.struct("created_at", "impression_id", "banner_id", "placement_id", "status", "request_id",
                           "position")).alias("f")
    slots = (imp.groupBy("fid").agg(
        first, F.count(F.lit(1)).alias("n_impression_rows"), F.max("created_at").alias("last_impression_at"),
        F.min("dt").alias("slot_arrival_dt"), F.countDistinct("banner_id").alias("n_banners"),
        F.countDistinct("placement_id").alias("n_placements"))
        .select("fid", F.col("f.request_id").alias("request_id"), F.col("f.position").alias("position"),
                F.col("f.created_at").alias("first_impression_at"), "last_impression_at",
                F.col("f.banner_id").alias("banner_id"), F.col("f.placement_id").alias("placement_id"),
                F.col("f.status").alias("first_status"), "n_impression_rows", "n_banners", "n_placements",
                "slot_arrival_dt"))

    clk = (ev(spark, wh, "ev_click").join(tfb, "fid").groupBy("fid").agg(
        F.count(F.lit(1)).alias("n_clicks"), F.min("created_at").alias("first_click_at"),
        F.min("dt").alias("first_click_arrival_dt")))
    pb = (ev(spark, wh, "ev_postback").join(tfb, "fid").groupBy("fid").agg(
        F.count(F.lit(1)).alias("n_postbacks"), F.min("created_at").alias("first_conversion_at"),
        F.min("dt").alias("first_conv_arrival_dt"),
        F.round(F.sum(F.coalesce("revenue", F.lit(0.0))), 4).alias("revenue_usd"),
        F.sum(F.when(F.col("revenue") == 0, 1).otherwise(0)).alias("n_zero_revenue_postbacks")))
    s = (slots.join(clk, "fid", "left").join(pb, "fid", "left")
         .withColumn("n_clicks", F.coalesce("n_clicks", F.lit(0)))
         .withColumn("n_postbacks", F.coalesce("n_postbacks", F.lit(0)))
         .withColumn("revenue_usd", F.coalesce("revenue_usd", F.lit(0.0)))
         .withColumn("n_zero_revenue_postbacks", F.coalesce("n_zero_revenue_postbacks", F.lit(0)))
         .withColumn("is_click", F.col("n_clicks") > 0)
         .withColumn("is_conversion", F.col("n_postbacks") > 0))

    # request (arrives with the slot, or later: then request_found is false until it arrives)
    req_ids = F.broadcast(s.select("request_id").distinct())
    req = (ev(spark, wh, "ev_request").join(req_ids, "request_id")
           .select("request_id", F.col("created_at").alias("request_at"), F.col("dt").alias("request_arrival_dt"),
                   F.col("placement_id").alias("request_placement_id"), "has_preview", "country", "state"))
    s = s.join(req, "request_id", "left")
    s = (s.withColumn("request_found", F.col("request_at").isNotNull())
         .withColumn("request_dt", F.date_format("request_at", "yyyy-MM-dd"))
         .withColumn("slot_dt", F.date_format("first_impression_at", "yyyy-MM-dd"))
         .withColumn("dt", F.coalesce("request_dt", "slot_dt")))      # partition + as-of day

    # dimensions as-of that day
    s = attach_snapshot(spark, wh, s, "dim_placement", "dt", dt, "placement_snapshot_dt")
    s = dim_asof(spark, wh, s, "dim_placement", "placement_id", "placement_id", "placement_snapshot_dt",
                 {"site_id": "site_id", "publisher_id": "publisher_id", "site_found": "site_found",
                  "publisher_found": "publisher_found", "created_after_snapshot": "placement_created_after_snapshot"})
    s = s.withColumn("placement_found", F.col("site_id").isNotNull() | F.col("publisher_id").isNotNull()
                     | F.col("site_found").isNotNull())
    s = attach_snapshot(spark, wh, s, "dim_banner", "dt", dt, "banner_snapshot_dt")
    s = dim_asof(spark, wh, s, "dim_banner", "banner_id", "banner_id", "banner_snapshot_dt",
                 {"campaign_id": "campaign_id", "advertiser_id": "advertiser_id", "campaign_found": "campaign_found",
                  "advertiser_found": "advertiser_found", "created_after_snapshot": "banner_created_after_snapshot"})
    s = s.withColumn("banner_found", F.col("campaign_found").isNotNull())
    return s.select(
        "fid", "request_id", "position", "request_found", "request_at", "request_arrival_dt", "request_dt", "slot_dt",
        "first_impression_at", "last_impression_at", "first_status", "n_impression_rows", "n_banners", "n_placements",
        "slot_arrival_dt", "placement_id", "request_placement_id", "placement_snapshot_dt", "placement_found",
        F.coalesce("site_found", F.lit(False)).alias("site_found"),
        F.coalesce("publisher_found", F.lit(False)).alias("publisher_found"), "site_id", "publisher_id",
        F.coalesce("placement_created_after_snapshot", F.lit(False)).alias("placement_created_after_snapshot"),
        "banner_id", "banner_snapshot_dt", "banner_found",
        F.coalesce("campaign_found", F.lit(False)).alias("campaign_found"),
        F.coalesce("advertiser_found", F.lit(False)).alias("advertiser_found"), "campaign_id", "advertiser_id",
        F.coalesce("banner_created_after_snapshot", F.lit(False)).alias("banner_created_after_snapshot"),
        F.coalesce("has_preview", F.lit(False)).alias("has_preview"), "country", "state",
        "n_clicks", "is_click", "first_click_at", "first_click_arrival_dt",
        "n_postbacks", "is_conversion", "first_conversion_at", "first_conv_arrival_dt", "revenue_usd",
        "n_zero_revenue_postbacks", "dt")


def check_slots(dq: Collector, slots: DataFrame, dt: str) -> None:
    """Checks on slots that ARRIVED today (first impression row in today's folder) - each slot is checked once.
    Checks that depend on outcomes which can still change (click/conversion anomalies) are computed on the final
    table by scripts/export_reports.py (dq_final_state.md)."""
    k = F.col("fid")
    new = slots.where(F.col("slot_arrival_dt") == dt)
    dq.run(new, "fact", "fact_impression", [
        Spec("slot_without_request", ~F.col("request_found"), k, "kept; request attributes null; becomes trainable if the request arrives later"),
        Spec("slot_multiple_banners", F.col("n_banners") > 1, k, "kept; first render's banner is used"),
        Spec("slot_multiple_placements", F.col("n_placements") > 1, k, "kept; first render's placement is used"),
        Spec("placement_not_in_dimension", ~F.col("placement_found"), k, "kept; dimension columns null, flag placement_found=false"),
        Spec("placement_no_snapshot_yet", F.col("placement_snapshot_dt").isNull(), k, "kept; event older than first snapshot"),
        Spec("placement_created_after_snapshot", F.col("placement_created_after_snapshot"), k, "kept; flagged"),
        Spec("site_not_in_dimension", F.col("placement_found") & ~F.col("site_found"), k, "kept; site columns null"),
        Spec("publisher_not_in_dimension", F.col("placement_found") & ~F.col("publisher_found"), k, "kept; publisher columns null"),
        Spec("banner_not_in_dimension", ~F.col("banner_found"), k, "kept; campaign/advertiser null"),
        Spec("campaign_not_in_dimension", F.col("banner_found") & ~F.col("campaign_found"), k, "kept"),
        Spec("advertiser_not_in_dimension", F.col("campaign_found") & ~F.col("advertiser_found"), k, "kept"),
        Spec("banner_created_after_snapshot", F.col("banner_created_after_snapshot"), k, "kept; flagged"),
        Spec("impression_before_request", F.col("request_found") & (F.col("first_impression_at") < F.col("request_at")), k,
             "kept; features use request time"),
        Spec("impression_placement_differs_from_request", F.col("request_found") & F.col("request_placement_id").isNotNull()
             & (F.col("placement_id") != F.col("request_placement_id")), k, "kept; impression placement used for slot"),
    ])


def check_requests_orders(dq: Collector, reqs: DataFrame, orders: DataFrame, spark, wh, dt: str) -> None:
    """Request and order facts: dimension matches and currency conversion, on rows that arrived today."""
    new = reqs.where(F.col("arrival_dt") == dt)
    k = F.col("request_id")
    dq.run(new, "fact", "fact_request", [
        Spec("placement_not_in_dimension", F.col("placement_id").isNotNull() & ~F.col("placement_found"), k,
             "kept; site/publisher null, flags false"),
        Spec("publisher_not_in_dimension", F.col("placement_found") & ~F.col("publisher_found"), k, "kept"),
        Spec("fx_no_rate", F.col("fx_status") == "no_rate", F.col("order_currency"), "kept; amount_usd null"),
        Spec("fx_no_amount", F.col("fx_status") == "no_amount", k, "kept; amount_usd null"),
        Spec("fx_no_currency", F.col("fx_status") == "no_currency", k, "kept; amount_usd null"),
        Spec("fx_non_positive", F.col("fx_status") == "non_positive", k, "kept; amount_usd null"),
    ])
    site_ids = spark.read.parquet(f"{wh}/dw/dim_placement").select(F.col("site_id").alias("_s"), F.col("dt").alias("_sd"))
    snaps = F.broadcast(snapshot_calendar(spark, list_partitions(spark, f"{wh}/dw/dim_placement", "dt"), dt))
    o = orders.where(F.col("arrival_dt") == dt)
    o = (o.join(snaps, o["dt"] == snaps["cal_dt"], "left")
         .join(F.broadcast(site_ids.distinct()), (F.col("site_id") == F.col("_s")) & (F.col("snap_dt") == F.col("_sd")), "left")
         .withColumn("_site_known", F.col("_s").isNotNull()))
    # an order's site is known when some placement of the day's snapshot belongs to that site
    o = o.groupBy(*[c for c in orders.columns]).agg(F.max("_site_known").alias("site_known"))
    ko = F.col("order_key")
    dq.run(o, "fact", "fact_order", [
        Spec("site_not_in_dimension", ~F.col("site_known"), ko, "kept; flagged only (site table is joined through placement)"),
        Spec("fx_no_rate", F.col("fx_status") == "no_rate", F.col("currency"), "kept; amount_usd null"),
        Spec("fx_no_amount", F.col("fx_status") == "no_amount", ko, "kept; amount_usd null"),
        Spec("fx_no_currency", F.col("fx_status") == "no_currency", ko, "kept; amount_usd null"),
        Spec("fx_non_positive", F.col("fx_status") == "non_positive", ko, "kept; amount_usd null"),
    ])


# ═══════════════════════════ fact_request ══════════════════════════════════

def build_requests(spark, wh, treq: DataFrame, dt: str) -> DataFrame:
    r = ev(spark, wh, "ev_request").join(F.broadcast(treq), "request_id")
    r = (r.withColumnRenamed("dt", "arrival_dt").withColumn("dt", F.date_format("created_at", "yyyy-MM-dd")))
    r = r.withColumn("dt", F.coalesce("dt", "arrival_dt"))      # unparsable created_at: fall back to arrival day
    r = attach_snapshot(spark, wh, r, "dim_placement", "dt", dt, "placement_snapshot_dt")
    r = dim_asof(spark, wh, r, "dim_placement", "placement_id", "placement_id", "placement_snapshot_dt",
                 {"site_id": "site_id", "publisher_id": "publisher_id", "site_found": "site_found",
                  "publisher_found": "publisher_found"})
    r = (r.withColumn("placement_found", F.col("site_found").isNotNull())
         .withColumn("site_found", F.coalesce("site_found", F.lit(False)))
         .withColumn("publisher_found", F.coalesce("publisher_found", F.lit(False))))
    return r


def outcome_per_request(spark, wh, treq: DataFrame) -> DataFrame:
    f = spark.read.parquet(f"{wh}/dw/fact_impression").join(F.broadcast(treq), "request_id")
    return f.groupBy("request_id").agg(
        F.count(F.lit(1)).alias("n_slots"), F.sum("n_impression_rows").alias("n_impression_rows"),
        F.sum(F.col("is_click").cast("int")).alias("n_clicked_slots"),
        F.sum(F.col("is_conversion").cast("int")).alias("n_converted_slots"),
        F.round(F.sum("revenue_usd"), 4).alias("revenue_usd"))


# ═══════════════════════════ external data for the touched requests ════════

def refresh_external(spark, wh, cfg, req: DataFrame, get_fx=None, get_holiday=None) -> None:
    """Fetch FX rates and holidays needed by the requests in *req* (columns created_at, order_currency, country)."""
    cur = [r[0] for r in req.select("order_currency").where(F.col("order_currency").isNotNull()).distinct().collect()]
    rng = req.agg(F.min(F.to_date("created_at")).alias("a"), F.max(F.to_date("created_at")).alias("b")).first()
    if rng["a"] is not None:
        external.ensure_fx(spark, wh, cfg, rng["a"], rng["b"], cur, get=get_fx)
    ctry = [r[0] for r in req.select("country").where(F.col("country").isNotNull()).distinct().collect()]
    years = [r[0] for r in req.select(F.year("created_at")).where(F.col("created_at").isNotNull()).distinct().collect()]
    if ctry and years:
        external.ensure_holidays(spark, wh, cfg, ctry, years, get=get_holiday)


# ═══════════════════════════ orders ════════════════════════════════════════

def build_orders(spark, wh, dt: str) -> DataFrame:
    o = ev_day(spark, wh, "ev_order", dt).withColumnRenamed("dt", "arrival_dt")
    o = (o.withColumn("dt", F.coalesce(F.date_format("created_at", "yyyy-MM-dd"), F.col("arrival_dt")))
         .withColumn("order_key", F.concat_ws("|", "order_id", "site_id")))
    fx = spark.read.parquet(f"{wh}/ref/fx_daily")
    o = external.with_amount_usd(o, "amount", "currency", "dt", fx)
    return o.drop("_src_file")


# ═══════════════════════════ driver ════════════════════════════════════════

def load_facts_day(spark, wh, cfg, dq: Collector, dt: str, get_fx=None, get_holiday=None) -> dict:
    """Update fact_impression / fact_request / fact_order for folder *dt*.  Returns the touched fids + request ids
    (persisted DataFrames) and the rebuilt slot rows for the mart."""
    tf = touched_fids(spark, wh, dt).persist()
    n_tf = tf.count()

    # requests that must be rebuilt: arrived today, or own a rebuilt slot
    slots = build_slots(spark, wh, tf, dt).persist()
    n_slots = slots.count()
    new_req = ev_day(spark, wh, "ev_request", dt).select("request_id")
    treq = new_req.unionByName(slots.select("request_id")).where(F.col("request_id").isNotNull()).distinct().persist()
    treq.count()

    check_slots(dq, slots, dt)
    merge_partitions(spark, f"{wh}/dw/fact_impression", slots, "fid", tf, "dt")

    # external reference data for everything we are about to enrich
    reqs = build_requests(spark, wh, treq, dt).persist()
    refresh_external(spark, wh, cfg, reqs, get_fx, get_holiday)
    fx = spark.read.parquet(f"{wh}/ref/fx_daily")
    reqs = reqs.withColumn("req_date", F.to_date("created_at"))
    reqs = external.with_amount_usd(reqs, "order_amount", "order_currency", "req_date", fx).drop("req_date")
    out = outcome_per_request(spark, wh, treq)
    fr = (reqs.join(out, "request_id", "left")
          .withColumn("n_slots", F.coalesce("n_slots", F.lit(0)))
          .withColumn("n_impression_rows", F.coalesce("n_impression_rows", F.lit(0)))
          .withColumn("n_clicked_slots", F.coalesce("n_clicked_slots", F.lit(0)))
          .withColumn("n_converted_slots", F.coalesce("n_converted_slots", F.lit(0)))
          .withColumn("revenue_usd", F.coalesce("revenue_usd", F.lit(0.0)))
          .withColumn("has_impressions", F.col("n_slots") > 0)).drop("_is_corrupt", "created_at_raw", "_ex")
    merge_partitions(spark, f"{wh}/dw/fact_request", fr, "request_id", treq, "dt")

    orders = build_orders(spark, wh, dt).persist()
    check_requests_orders(dq, fr, orders, spark, wh, dt)
    merge_partitions(spark, f"{wh}/dw/fact_order", orders, "order_key",
                     orders.select("order_key"), "dt")
    return {"touched_fids": tf, "touched_requests": treq, "slots": slots, "requests": reqs, "orders": orders,
            "n_touched": n_tf, "n_slots": n_slots}
