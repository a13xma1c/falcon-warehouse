"""Event layer: cleaned, de-duplicated, typed events.  Partition ``dt`` = ARRIVAL day (the day folder
the row came in).  Nothing is dropped silently: every staged row is either kept or written to
``dq/rejects`` with a reason; per-day counts are reconciled in ``dq/reconciliation``.

    ev_request     grain request_id            parsed payload, device/os/browser
    ev_impression  grain impression_id         one render (a slot/fid can have several rows)
    ev_click       grain click_id
    ev_postback    grain postback_id
    ev_order       grain (order_id, site_id)

Rejection rules (same for all tables): missing key | undecodable fid | key already seen in an earlier
folder | key repeated inside the folder (first row kept; order: created_at, then a row hash so the
choice is deterministic).  Anything else is kept and only flagged in ``dq/dq_checks``.
Re-running a day replaces its partition; duplicates are judged against folders *before* that day only.
"""
from __future__ import annotations
from pyspark.sql import Column, DataFrame, SparkSession, Window, functions as F

from falcon import ua
from falcon.common import fid_request_id, fid_position, read_table, write_day_partition
from falcon.dq import Collector, Spec


def stg(spark: SparkSession, wh: str, table: str, dt: str) -> DataFrame:
    return spark.read.parquet(f"{wh}/staging/{table}").filter(F.col("dt") == dt)


def _clean(c: Column) -> Column:
    """trim; empty string -> null"""
    t = F.trim(c)
    return F.when(t == "", F.lit(None)).otherwise(t)


def _row_hash(df: DataFrame, exclude: tuple = ("_src_file", "dt")) -> Column:
    return F.sha2(F.to_json(F.struct(*[F.col(c) for c in df.columns if c not in exclude])), 256)


def _with_reasons(spark, wh, df: DataFrame, table: str, ids: list[str], dt: str, time_col: str,
                  extra: list[tuple[Column, str]] | None = None) -> DataFrame:
    """Adds ``reject_reason`` (null = accepted) and ``_ex`` (example key text)."""
    df = df.withColumn("_h", _row_hash(df))
    w = Window.partitionBy(*ids).orderBy(F.col(time_col).asc_nulls_last(), F.col("_h"))
    df = df.withColumn("_rn", F.row_number().over(w))
    prev = read_table(spark, f"{wh}/dw/{table}")
    if prev is not None:
        prev = prev.filter(F.col("dt") < dt).select(*ids).distinct().withColumn("_seen", F.lit(1))
        df = df.join(prev, ids, "left")
    else:
        df = df.withColumn("_seen", F.lit(None).cast("int"))
    chain = None
    conds = [(F.col(i).isNull(), f"null_{i}") for i in ids] + (extra or []) + [
        (F.col("_seen").isNotNull(), "duplicate_of_earlier_day"), (F.col("_rn") > 1, "duplicate_in_day")]
    for cond, name in conds:
        chain = F.when(cond, name) if chain is None else chain.when(cond, name)
    key_txt = F.concat_ws("|", *[F.coalesce(F.col(i).cast("string"), F.lit("NULL")) for i in ids])
    return df.withColumn("reject_reason", chain).withColumn("_ex", key_txt).drop("_h", "_rn", "_seen")


def _finish(spark, wh, dq: Collector, dt: str, table: str, stg_df: DataFrame, flagged: DataFrame,
            out: DataFrame, reasons: list[str], key: Column, n_stg: int) -> DataFrame:
    """Write accepted rows, register rejects and the reconciliation line.  Returns written df."""
    accepted = out.filter(F.col("reject_reason").isNull()).drop("reject_reason", "_ex")
    path = f"{wh}/dw/{table}"
    write_day_partition(spark, accepted, path, "dt", dt)
    written = spark.read.parquet(path).filter(F.col("dt") == dt)
    n_ok = written.count()
    dq.add_rejects(table, flagged, key)
    dq.reject_checks("event", table, flagged, reasons, n_stg, "rejected, listed in dq/rejects")
    n_rej = flagged.filter("reject_reason is not null").count()
    dq.recon(table, "staged = accepted + rejected", "staging rows", n_stg, "accepted + rejected", n_ok + n_rej)
    return written


# ═══════════════════════════════ requests ══════════════════════════════════

def parse_requests(df: DataFrame) -> DataFrame:
    d = F.col("data")

    def j(p: str) -> Column:
        return _clean(F.get_json_object(d, p))

    amount_txt = F.coalesce(j("$.orderData.amount"), j("$.orderData.order_value"))
    return df.select(
        F.col("request_id"), F.col("placement_id"), F.to_timestamp("created_at").alias("created_at"),
        F.col("created_at").alias("created_at_raw"),
        F.col("customer_id"), F.col("session_id"), F.col("user_agent"), F.col("city"), F.col("state"),
        F.col("country"), F.col("has_preview"),
        amount_txt.alias("order_amount_raw"), amount_txt.cast("double").alias("order_amount"),
        F.upper(j("$.orderData.currency")).alias("order_currency"),
        F.coalesce(j("$.orderData.orderid"), j("$.orderData.orderId"), j("$.orderData.orderID")).alias("order_id"),
        j("$.orderData.provinceCode").alias("province_code"), F.upper(j("$.orderData.country")).alias("order_country"),
        j("$.orderData.language").alias("language"), j("$.orderData.paymenttype").alias("payment_type"),
        j("$.orderData.shopdomain").alias("shop_domain"),
        F.get_json_object(d, "$.offerCount").cast("int").alias("offer_count"),
        F.get_json_object(d, "$.template").cast("int").alias("template"),
        F.get_json_object(d, "$.hasPartnerOffer").cast("boolean").alias("has_partner_offer"),
        F.get_json_object(d, "$.isGdprUser").cast("boolean").alias("is_gdpr_user"),
        j("$.pubSub1").alias("pub_sub1"), j("$.pubSub2").alias("pub_sub2"),
        j("$.userIdentifiers.hashedEmail").isNotNull().alias("has_hashed_email"),
        j("$.userIdentifiers.hashedPhone").isNotNull().alias("has_hashed_phone"),
        j("$.userIdentifiers.hashedCustomerShopifyId").isNotNull().alias("has_hashed_shopify_id"),
        ua.device_type(F.col("user_agent")).alias("device_type"),
        ua.os_name(F.col("user_agent")).alias("os"),
        ua.browser_name(F.col("user_agent")).alias("browser"),
        F.col("_corrupt_record").isNotNull().alias("_is_corrupt"),
        F.col("dt"))


def load_requests(spark, wh, dq: Collector, dt: str) -> DataFrame:
    s = stg(spark, wh, "requests", dt)
    p = parse_requests(s).persist()
    n = p.count()
    flagged = _with_reasons(spark, wh, p, "ev_request", ["request_id"], dt, "created_at",
                            extra=[(F.col("_is_corrupt"), "corrupt_json_line")])
    # corrupt lines also have null request_id: report them as corrupt, not as null id
    flagged = flagged.withColumn("reject_reason", F.when(F.col("_is_corrupt"), "corrupt_json_line")
                                 .otherwise(F.col("reject_reason"))).persist()
    out = flagged.drop("_is_corrupt", "created_at_raw")
    ev = _finish(spark, wh, dq, dt, "ev_request", s, flagged, out,
                 ["corrupt_json_line", "null_request_id", "duplicate_of_earlier_day", "duplicate_in_day"],
                 F.col("request_id"), n)
    ok = flagged.filter("reject_reason is null")
    d = F.to_date("created_at")
    dq.run(ok, "event", "ev_request", [
        Spec("created_at_unparsable", F.col("created_at").isNull() & F.col("created_at_raw").isNotNull(), F.col("created_at_raw"), "kept, created_at null"),
        Spec("arrived_after_event_day", d < F.to_date(F.lit(dt)), F.col("request_id"), "kept; partitioned by arrival day (late)"),
        Spec("placement_id_null", F.col("placement_id").isNull(), F.col("request_id"), "kept; no placement dimension row"),
        Spec("customer_id_null", F.col("customer_id").isNull(), F.col("request_id"), "kept (normal, anonymous shopper)"),
        Spec("session_id_null", F.col("session_id").isNull(), F.col("request_id"), "kept"),
        Spec("has_preview_true", F.col("has_preview"), F.col("request_id"), "kept, flagged; excluded from ML mart"),
        Spec("order_amount_unparsable", F.col("order_amount_raw").isNotNull() & F.col("order_amount").isNull(), F.col("order_amount_raw"), "kept, amount null"),
        Spec("order_amount_non_positive", F.col("order_amount") <= 0, F.col("request_id"), "kept; amount_usd null (fx_status=non_positive)"),
        Spec("order_amount_null", F.col("order_amount").isNull(), F.col("request_id"), "kept; amount_usd null (orders table would fill only ~0.2% of gaps, not used)"),
        Spec("order_currency_null", F.col("order_currency").isNull(), F.col("request_id"), "kept; amount_usd null (orders table would fill only ~0.2% of gaps, not used)"),
        Spec("order_amount_without_currency", F.col("order_amount").isNotNull() & F.col("order_currency").isNull(), F.col("request_id"), "kept; amount_usd null (no guessing)"),
        Spec("order_id_null", F.col("order_id").isNull(), F.col("request_id"), "kept"),
        Spec("user_agent_device_unknown", F.col("device_type") == "unknown", F.col("user_agent"), "kept, device_type=unknown"),
        Spec("user_agent_os_other", F.col("os").isin("other", "unknown"), F.col("user_agent"), "kept, os=other/unknown"),
        Spec("user_agent_browser_other", F.col("browser").isin("other", "unknown"), F.col("user_agent"), "kept, browser=other/unknown"),
    ])
    p.unpersist()
    flagged.unpersist()
    return ev


# ═══════════════════════════════ impressions / clicks / postbacks ══════════

def load_impressions(spark, wh, dq: Collector, dt: str) -> DataFrame:
    s = stg(spark, wh, "impressions", dt).persist()
    n = s.count()
    s = (s.withColumn("request_id", fid_request_id(F.col("fid"))).withColumn("position", fid_position(F.col("fid"))))
    flagged = _with_reasons(spark, wh, s, "ev_impression", ["impression_id"], dt, "created_at",
                            extra=[(F.col("request_id").isNull() | F.col("position").isNull(), "undecodable_fid")]).persist()
    out = flagged.drop("_src_file")
    ev = _finish(spark, wh, dq, dt, "ev_impression", s, flagged, out,
                 ["null_impression_id", "undecodable_fid", "duplicate_of_earlier_day", "duplicate_in_day"],
                 F.col("impression_id"), n)
    ok = flagged.filter("reject_reason is null")
    dq.run(ok, "event", "ev_impression", [
        Spec("banner_id_null", F.col("banner_id").isNull(), F.col("impression_id"), "kept; banner dimension not found"),
        Spec("placement_id_null", F.col("placement_id").isNull(), F.col("impression_id"), "kept"),
        Spec("arrived_after_event_day", F.to_date("created_at") < F.to_date(F.lit(dt)), F.col("impression_id"), "kept; partitioned by arrival day (late)"),
        Spec("slot_position_gt_9", F.col("position") > 9, F.col("fid"), "kept"),
    ])
    flagged.unpersist()
    s.unpersist()
    return ev


def _load_simple(spark, wh, dq, dt, stg_name, table, key, extra_specs, extra_reasons=None, extra_proj=None):
    s = stg(spark, wh, stg_name, dt).persist()
    n = s.count()
    s = s.withColumn("request_id", fid_request_id(F.col("fid"))).withColumn("position", fid_position(F.col("fid")))
    flagged = _with_reasons(spark, wh, s, table, [key], dt, "created_at",
                            extra=[(F.col("request_id").isNull() | F.col("position").isNull(), "undecodable_fid")]
                            + (extra_reasons or [])).persist()
    out = flagged.drop("_src_file")
    ev = _finish(spark, wh, dq, dt, table, s, flagged, out,
                 [f"null_{key}", "undecodable_fid", "duplicate_of_earlier_day", "duplicate_in_day"]
                 + [r for _, r in (extra_reasons or [])], F.col(key), n)
    dq.run(flagged.filter("reject_reason is null"), "event", table,
           [Spec("arrived_after_event_day", F.to_date("created_at") < F.to_date(F.lit(dt)), F.col(key), "kept; late")] + extra_specs)
    flagged.unpersist()
    s.unpersist()
    return ev


def load_clicks(spark, wh, dq, dt):
    return _load_simple(spark, wh, dq, dt, "clicks", "ev_click", "click_id", [])


def load_postbacks(spark, wh, dq, dt):
    return _load_simple(
        spark, wh, dq, dt, "postbacks", "ev_postback", "postback_id",
        [Spec("revenue_zero", F.col("revenue") == 0, F.col("postback_id"), "kept; counted as conversion, flagged"),
         Spec("revenue_null", F.col("revenue").isNull(), F.col("postback_id"), "kept; revenue treated as 0 in sums")],
        extra_reasons=[(F.col("revenue") < 0, "negative_revenue")])


# ═══════════════════════════════ orders ════════════════════════════════════

def load_orders(spark, wh, dq: Collector, dt: str) -> DataFrame:
    s = stg(spark, wh, "orders", dt).persist()
    n = s.count()
    s = (s.withColumn("created_at", F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
         .withColumn("currency", F.upper(_clean(F.col("currency")))))
    flagged = _with_reasons(spark, wh, s, "ev_order", ["order_id", "site_id"], dt, "created_at").persist()
    out = flagged.drop("_src_file")
    ev = _finish(spark, wh, dq, dt, "ev_order", s, flagged, out,
                 ["null_order_id", "null_site_id", "duplicate_of_earlier_day", "duplicate_in_day"],
                 F.concat_ws("|", "order_id", "site_id"), n)
    dq.run(flagged.filter("reject_reason is null"), "event", "ev_order", [
        Spec("arrived_after_event_day", F.to_date("created_at") < F.to_date(F.lit(dt)), F.col("order_id"), "kept; late"),
        Spec("amount_null", F.col("amount").isNull(), F.col("order_id"), "kept"),
        Spec("amount_non_positive", F.col("amount") <= 0, F.col("order_id"), "kept; amount_usd null"),
        Spec("currency_null", F.col("currency").isNull(), F.col("order_id"), "kept; amount_usd null"),
        Spec("placement_id_null", F.col("placement_id").isNull(), F.col("order_id"), "kept (documented as possible)"),
    ])
    flagged.unpersist()
    s.unpersist()
    return ev
