"""Staging layer.

Loads every raw input file for *dt* with an explicit schema.  No fixes, no
derivations — raw data as-is, plus two book-keeping columns:
  - ``dt``          folder date string, set from the filename, not from the record
  - ``_src_file``   relative source path (for audit / DQ)

Also:
  - ``_corrupt_record`` for JSON tables that used PERMISSIVE mode
  - Parquet files have `TimestampType` already; CSVs keep timestamps as strings
    (we cast them in later layers so staging stays truly raw).
"""
from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame, functions as F

from falcon import schemas
from falcon.io import write_layer


# ─────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────

def _tag(df: DataFrame, dt: str) -> DataFrame:
    """Add ``dt`` (the folder being loaded) and ``_src_file`` metadata columns."""
    return (
        df
        .withColumn("dt",        F.lit(dt))
        .withColumn("_src_file", F.regexp_extract(F.input_file_name(), r"(dt=[\d-]+/.*)", 1))
    )


def _read_json(spark: SparkSession, path: str, schema) -> DataFrame:
    return (
        spark.read
        .format("json")
        .schema(schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(path)
    )


def _read_parquet(spark: SparkSession, path: str, schema) -> DataFrame:
    return spark.read.format("parquet").schema(schema).load(path)


def _read_csv(spark: SparkSession, path: str, schema) -> DataFrame:
    return (
        spark.read
        .format("csv")
        .schema(schema)
        .option("header", "true")
        .option("nullValue", "")
        .load(path)
    )


# ─────────────────────────────────────────────────────────────────
# public API
# ─────────────────────────────────────────────────────────────────

def stage_day(spark: SparkSession, dataset_path: str, warehouse: str, dt: str) -> None:
    """Load all files for *dt* and write to the staging layer."""
    base = f"{dataset_path}/dt={dt}"

    # ---- event tables ---------------------------------------------------
    requests = _tag(_read_json(spark, f"{base}/requests/*.jsonl.gz", schemas.REQUEST_SCHEMA), dt)
    write_layer(requests, warehouse, "staging", "requests", ["dt"])

    impressions = _tag(_read_parquet(spark, f"{base}/impressions/*.parquet", schemas.IMPRESSION_SCHEMA), dt)
    write_layer(impressions, warehouse, "staging", "impressions", ["dt"])

    clicks = _tag(_read_parquet(spark, f"{base}/clicks/*.parquet", schemas.CLICK_SCHEMA), dt)
    write_layer(clicks, warehouse, "staging", "clicks", ["dt"])

    postbacks = _tag(_read_parquet(spark, f"{base}/postbacks/*.parquet", schemas.POSTBACK_SCHEMA), dt)
    write_layer(postbacks, warehouse, "staging", "postbacks", ["dt"])

    orders = _tag(_read_csv(spark, f"{base}/orders.csv", schemas.ORDER_SCHEMA), dt)
    write_layer(orders, warehouse, "staging", "orders", ["dt"])

    # ---- dimension tables -----------------------------------------------
    for tbl, sch in [
        ("publisher",  schemas.PUBLISHER_SCHEMA),
        ("site",       schemas.SITE_SCHEMA),
        ("placement",  schemas.PLACEMENT_SCHEMA),
        ("platform",   schemas.PLATFORM_SCHEMA),
        ("category",   schemas.CATEGORY_SCHEMA),
        ("advertiser", schemas.ADVERTISER_SCHEMA),
        ("campaign",   schemas.CAMPAIGN_SCHEMA),
        ("banner",     schemas.BANNER_SCHEMA),
    ]:
        df = _tag(_read_csv(spark, f"{base}/dims/{tbl}.csv", sch), dt)
        write_layer(df, warehouse, "staging", f"dim_{tbl}", ["dt"])

    # ---- enrichment tables ----------------------------------------------
    for tbl in ("site_enrichment", "campaign_enrichment"):
        df = _tag(_read_json(spark, f"{base}/enrichment/{tbl}.jsonl", schemas.ENRICHMENT_SCHEMA), dt)
        write_layer(df, warehouse, "staging", f"dim_{tbl}", ["dt"])
