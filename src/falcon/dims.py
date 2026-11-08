"""Dimension tables of the warehouse.

Source: the staged daily snapshots (``staging/dim_*``, partition ``dt`` = folder date).
Every snapshot is processed on its own and stored under its own ``dt``:

    dim_placement          grain: placement_id x snapshot dt     key: (placement_id, dt)
                           placement -> site -> publisher -> platform, site category (+ parent)
    dim_banner             grain: banner_id    x snapshot dt     key: (banner_id, dt)
                           banner -> campaign -> advertiser, campaign category
    dim_site_enrichment    grain: enrichment row id x snapshot dt   key: (id, dt)
    dim_campaign_enrichment same for campaigns                      key: (id, dt)

Point-in-time rule (see README): an event of day M looks up the newest snapshot with
``dt <= M``.  Where two snapshots differ the later one wins from its day on.

All joins are LEFT joins; a missing parent never drops a row, it sets a ``*_found`` flag.
"""
from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from falcon.common import write_day_partition
from falcon.dq import Collector, Spec

TS_FMT = "yyyy-MM-dd HH:mm:ss"


def _ts(c: str):
    return F.to_timestamp(F.col(c), TS_FMT)


def _bool(c: str):
    """Postgres CSV booleans: t / f."""
    return F.when(F.col(c) == "t", F.lit(True)).when(F.col(c) == "f", F.lit(False))


def _latest_per_id(df: DataFrame, id_col: str = "id") -> DataFrame:
    """One row per id inside a snapshot: newest updated_at wins (ties: newest created_at,
    then a deterministic hash of the row so reruns pick the same row)."""
    w = Window.partitionBy(id_col).orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("created_at").desc_nulls_last(),
        F.sha2(F.to_json(F.struct(*[c for c in df.columns])), 256),
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ─────────────────────────────────────────────────────────────────────────────

def build_dim_placement(stg: dict[str, DataFrame], dt: str) -> DataFrame:
    """*stg*: dict of staged snapshot DataFrames (already filtered to this dt)."""
    pl = _latest_per_id(stg["placement"].select(
        "id", "name", "type", "template", "site_id", "surface_type",
        _bool("is_test_mode").alias("is_test_mode"),
        _bool("user_initiated").alias("user_initiated"),
        _ts("created_at").alias("created_at"),
        _ts("updated_at").alias("updated_at"),
    )).select(
        F.col("id").alias("placement_id"),
        F.col("name").alias("placement_name"),
        F.col("type").alias("placement_type"),
        F.col("template").alias("placement_template"),
        "is_test_mode", "site_id", "surface_type", "user_initiated",
        F.col("created_at").alias("placement_created_at"),
        F.col("updated_at").alias("placement_updated_at"),
    )

    cat = stg["category"].select(
        F.col("id").alias("cat_id"), F.col("name").alias("cat_name"), F.col("parent_category_id").alias("cat_parent_id"))
    parent = stg["category"].select(F.col("id").alias("par_id"), F.col("name").alias("par_name"))
    site_base = _latest_per_id(stg["site"].select(
        "id", "name", "domain", "status", _bool("ads_enabled").alias("ads_enabled"),
        "category_id", "publisher_id", "order_volume", "average_order",
        _ts("created_at").alias("created_at"), _ts("updated_at").alias("updated_at")))
    site = (site_base
            .join(cat, site_base["category_id"] == cat["cat_id"], "left")
            .join(parent, cat["cat_parent_id"] == parent["par_id"], "left")
            .select(
                F.col("id").alias("s_id"), F.col("name").alias("site_name"), F.col("domain").alias("site_domain"),
                F.col("status").alias("site_status"), F.col("ads_enabled").alias("site_ads_enabled"),
                F.col("category_id").alias("site_category_id"), F.col("cat_name").alias("site_category_name"),
                F.col("par_name").alias("site_parent_category_name"),
                F.col("publisher_id"), F.col("order_volume").alias("site_order_volume"),
                F.col("average_order").alias("site_average_order"), F.col("created_at").alias("site_created_at"),
                F.lit(True).alias("site_found")))

    pub_base = _latest_per_id(stg["publisher"].select(
        "id", "name", "status", _bool("ads_enabled").alias("ads_enabled"), "seller",
        "rev_share_percentage", "platform_id", _ts("created_at").alias("created_at"), _ts("updated_at").alias("updated_at")))
    plat = stg["platform"].select(F.col("id").alias("plat_id"), F.col("name").alias("platform_name"))
    pub = (pub_base.join(plat, pub_base["platform_id"] == plat["plat_id"], "left")
           .select(F.col("id").alias("p_id"), F.col("name").alias("publisher_name"),
                   F.col("status").alias("publisher_status"), F.col("ads_enabled").alias("publisher_ads_enabled"),
                   F.col("rev_share_percentage").alias("publisher_rev_share_pct"), F.col("platform_id"),
                   F.col("platform_name"), F.lit(True).alias("publisher_found")))

    out = (pl.join(site, pl["site_id"] == site["s_id"], "left").drop("s_id")
           .join(pub, F.col("publisher_id") == pub["p_id"], "left").drop("p_id"))
    return (out
            .withColumn("site_found", F.coalesce("site_found", F.lit(False)))
            .withColumn("publisher_found", F.coalesce("publisher_found", F.lit(False)))
            .withColumn("created_after_snapshot", F.to_date("placement_created_at") > F.to_date(F.lit(dt)))
            .withColumn("dt", F.lit(dt)))


def build_dim_banner(stg: dict[str, DataFrame], dt: str) -> DataFrame:
    b = _latest_per_id(stg["banner"].select(
        "id", "name", "title", "weight", "campaign_id", _bool("is_active").alias("is_active"), "banner_type",
        _ts("created_at").alias("created_at"), _ts("updated_at").alias("updated_at")))
    b = b.select(
        F.col("id").alias("banner_id"), F.col("name").alias("banner_name"), F.col("title").alias("banner_title"),
        F.col("weight").alias("banner_weight"), "campaign_id", F.col("is_active").alias("banner_is_active"),
        "banner_type", F.col("created_at").alias("banner_created_at"))

    cat = stg["category"].select(F.col("id").alias("cat_id"), F.col("name").alias("campaign_category_name"))
    c0 = _latest_per_id(stg["campaign"].select(
        "id", "name", "category_id", "weight", "priority", "cpa", "cpa_source", "advertiser_id",
        _ts("created_at").alias("created_at"), _ts("updated_at").alias("updated_at")))
    c = (c0.join(cat, c0["category_id"] == cat["cat_id"], "left")
         .select(F.col("id").alias("c_id"), F.col("name").alias("campaign_name"),
                 F.col("category_id").alias("campaign_category_id"), "campaign_category_name",
                 F.col("weight").alias("campaign_weight"), F.col("priority").alias("campaign_priority"),
                 F.col("cpa").alias("campaign_cpa"), "cpa_source", "advertiser_id", F.lit(True).alias("campaign_found")))

    a = _latest_per_id(stg["advertiser"].select(
        "id", "name", "payout", _ts("created_at").alias("created_at"), _ts("updated_at").alias("updated_at")))
    a = a.select(F.col("id").alias("a_id"), F.col("name").alias("advertiser_name"),
                 F.col("payout").alias("advertiser_payout"), F.lit(True).alias("advertiser_found"))

    out = (b.join(c, b["campaign_id"] == c["c_id"], "left").drop("c_id")
           .join(a, F.col("advertiser_id") == a["a_id"], "left").drop("a_id"))
    return (out
            .withColumn("campaign_found", F.coalesce("campaign_found", F.lit(False)))
            .withColumn("advertiser_found", F.coalesce("advertiser_found", F.lit(False)))
            .withColumn("created_after_snapshot", F.to_date("banner_created_at") > F.to_date(F.lit(dt)))
            .withColumn("dt", F.lit(dt)))


def build_enrichment(df: DataFrame, dt: str) -> DataFrame:
    """Typed copy of one enrichment snapshot (site or campaign)."""
    return (df.drop("_src_file", "dt")
            .withColumn("created_at", F.to_timestamp("created_at"))   # ISO-8601 with Z
            .withColumn("updated_at", F.to_timestamp("updated_at"))
            .withColumn("dt", F.lit(dt)))


# ─────────────────────────────────────────────────────────────────────────────

def check_dims(dq: Collector, s: dict[str, DataFrame]) -> None:
    """Checks on the raw snapshot of the day: duplicate / null ids and broken links between dimension tables."""
    for name, df in s.items():
        n = df.count()
        dups = df.groupBy("id").count().filter("count > 1")
        nd = dups.agg(F.sum(F.col("count") - 1)).first()[0] or 0
        ex = dups.agg(F.min("id")).first()[0] if nd else None
        dq.add("dim", f"dim_{name}", "duplicate_id_in_snapshot", nd, n, ex,
               "kept the row with the newest updated_at (then created_at, then row hash)")
        dq.add("dim", f"dim_{name}", "null_id", df.filter("id is null").count(), n, None, "kept; cannot be joined")

    def orphans(child, fk, parent, label):
        c, p = s[child], s[parent].select(F.col("id").alias("_pid")).distinct()
        bad = (c.where(F.col(fk).isNotNull()).join(p, F.col(fk) == F.col("_pid"), "left_anti"))
        n = c.where(F.col(fk).isNotNull()).count()
        r = bad.agg(F.count(F.lit(1)), F.min("id")).first()
        dq.add("dim", f"dim_{child}", label, r[0], n, r[1], "kept; parent columns null, *_found flag false")

    orphans("placement", "site_id", "site", "placement_site_not_found")
    orphans("site", "publisher_id", "publisher", "site_publisher_not_found")
    orphans("site", "category_id", "category", "site_category_not_found")
    orphans("publisher", "platform_id", "platform", "publisher_platform_not_found")
    orphans("banner", "campaign_id", "campaign", "banner_campaign_not_found")
    orphans("campaign", "advertiser_id", "advertiser", "campaign_advertiser_not_found")
    orphans("campaign", "category_id", "category", "campaign_category_not_found")


def load_dims_day(spark: SparkSession, wh: str, dt: str, dq: Collector | None = None) -> dict[str, int]:
    """Build and write all dimension snapshots for *dt*.  Idempotent: replaces partition dt."""
    def stg(name: str) -> DataFrame:
        return spark.read.parquet(f"{wh}/staging/dim_{name}").filter(F.col("dt") == dt)

    s = {n: stg(n) for n in ["placement", "site", "publisher", "platform", "category", "advertiser", "campaign", "banner"]}
    if dq is not None:
        check_dims(dq, s)
    outputs = {
        "dim_placement": build_dim_placement(s, dt),
        "dim_banner": build_dim_banner(s, dt),
        "dim_site_enrichment": build_enrichment(stg("site_enrichment"), dt),
        "dim_campaign_enrichment": build_enrichment(stg("campaign_enrichment"), dt),
    }
    counts = {}
    for name, df in outputs.items():
        path = f"{wh}/dw/{name}"
        write_day_partition(spark, df, path, "dt", dt)
        counts[name] = spark.read.parquet(f"{path}/dt={dt}").count()
    if dq is not None:
        dq.recon("dim_placement", "distinct staged ids = dim rows", "distinct placement ids", s["placement"].select("id").distinct().count(),
                 "dim_placement rows", counts["dim_placement"])
        dq.recon("dim_banner", "distinct staged ids = dim rows", "distinct banner ids", s["banner"].select("id").distinct().count(),
                 "dim_banner rows", counts["dim_banner"])
    return counts
