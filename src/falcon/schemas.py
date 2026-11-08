"""Explicit Spark schemas for every raw input table."""
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    LongType, DoubleType, TimestampType, IntegerType,
)

# ------- JSON helpers -------------------------------------------------------
# Requests are JSON lines (.jsonl.gz).  We read as raw JSON then cast because
# Spark's JSON schema inference does not handle multi-day globs well with the
# mixed 'orderId'/'orderid' keys (both present after case-sensitive merge).
# We keep `data` as a flat STRING and parse the interesting sub-fields in the
# staging step so as to avoid the COLUMN_ALREADY_EXISTS error from
# spark.sql.caseSensitive + two keys that differ only in case.
REQUEST_SCHEMA = StructType([
    StructField("request_id",    StringType(),  True),
    StructField("placement_id",  StringType(),  True),
    StructField("created_at",    StringType(),  True),   # ISO string → cast in staging
    StructField("customer_id",   StringType(),  True),
    StructField("session_id",    StringType(),  True),
    StructField("user_agent",    StringType(),  True),
    StructField("city",          StringType(),  True),
    StructField("state",         StringType(),  True),
    StructField("country",       StringType(),  True),
    StructField("has_preview",   BooleanType(), True),
    StructField("data",          StringType(),  True),   # whole payload as STRING
    StructField("_corrupt_record", StringType(), True),
])

# ------- Parquet (schema is already declared in the file) -------------------
# We still declare explicit schemas to catch schema drift.
IMPRESSION_SCHEMA = StructType([
    StructField("impression_id", StringType(),   True),
    StructField("fid",           StringType(),   True),
    StructField("placement_id",  StringType(),   True),
    StructField("banner_id",     StringType(),   True),
    StructField("created_at",    TimestampType(), True),
    StructField("status",        StringType(),   True),
])

CLICK_SCHEMA = StructType([
    StructField("click_id",  StringType(),   True),
    StructField("fid",       StringType(),   True),
    StructField("created_at", TimestampType(), True),
    StructField("status",    StringType(),   True),
])

POSTBACK_SCHEMA = StructType([
    StructField("postback_id",     StringType(),   True),
    StructField("fid",             StringType(),   True),
    StructField("created_at",      TimestampType(), True),
    StructField("revenue",         DoubleType(),   True),
    StructField("postback_source", StringType(),   True),
])

# ------- CSV ----------------------------------------------------------------
ORDER_SCHEMA = StructType([
    StructField("order_id",      StringType(), True),
    StructField("site_id",       StringType(), True),
    StructField("placement_id",  StringType(), True),
    StructField("customer_id",   StringType(), True),
    StructField("created_at",    StringType(), True),
    StructField("amount",        DoubleType(), True),
    StructField("currency",      StringType(), True),
    StructField("payment_type",  StringType(), True),
])

PUBLISHER_SCHEMA = StructType([
    StructField("id",                   StringType(),  True),
    StructField("name",                 StringType(),  True),
    StructField("status",               StringType(),  True),
    StructField("ads_enabled",          StringType(),  True),  # 't'/'f'
    StructField("seller",               StringType(),  True),
    StructField("rev_share_percentage", DoubleType(),  True),
    StructField("platform_id",          StringType(),  True),
    StructField("created_at",           StringType(),  True),
    StructField("updated_at",           StringType(),  True),
])

SITE_SCHEMA = StructType([
    StructField("id",             StringType(),  True),
    StructField("name",           StringType(),  True),
    StructField("domain",         StringType(),  True),
    StructField("status",         StringType(),  True),
    StructField("ads_enabled",    StringType(),  True),
    StructField("category_id",    StringType(),  True),
    StructField("publisher_id",   StringType(),  True),
    StructField("order_volume",   DoubleType(),  True),
    StructField("average_order",  DoubleType(),  True),
    StructField("created_at",     StringType(),  True),
    StructField("updated_at",     StringType(),  True),
])

PLACEMENT_SCHEMA = StructType([
    StructField("id",             StringType(),  True),
    StructField("name",           StringType(),  True),
    StructField("type",           StringType(),  True),
    StructField("template",       StringType(),  True),
    StructField("is_test_mode",   StringType(),  True),
    StructField("site_id",        StringType(),  True),
    StructField("surface_type",   StringType(),  True),
    StructField("user_initiated", StringType(),  True),
    StructField("created_at",     StringType(),  True),
    StructField("updated_at",     StringType(),  True),
])

PLATFORM_SCHEMA = StructType([
    StructField("id",        StringType(),  True),
    StructField("name",      StringType(),  True),
    StructField("is_active", StringType(),  True),
])

CATEGORY_SCHEMA = StructType([
    StructField("id",                 StringType(), True),
    StructField("name",               StringType(), True),
    StructField("parent_category_id", StringType(), True),
])

ADVERTISER_SCHEMA = StructType([
    StructField("id",         StringType(),  True),
    StructField("name",       StringType(),  True),
    StructField("payout",     DoubleType(),  True),
    StructField("created_at", StringType(),  True),
    StructField("updated_at", StringType(),  True),
])

CAMPAIGN_SCHEMA = StructType([
    StructField("id",            StringType(),  True),
    StructField("name",          StringType(),  True),
    StructField("category_id",   StringType(),  True),
    StructField("weight",        IntegerType(), True),
    StructField("priority",      StringType(),  True),
    StructField("cpa",           DoubleType(),  True),
    StructField("cpa_source",    StringType(),  True),
    StructField("advertiser_id", StringType(),  True),
    StructField("created_at",    StringType(),  True),
    StructField("updated_at",    StringType(),  True),
])

BANNER_SCHEMA = StructType([
    StructField("id",          StringType(),  True),
    StructField("name",        StringType(),  True),
    StructField("title",       StringType(),  True),
    StructField("weight",      IntegerType(), True),
    StructField("campaign_id", StringType(),  True),
    StructField("is_active",   StringType(),  True),
    StructField("banner_type", StringType(),  True),
    StructField("created_at",  StringType(),  True),
    StructField("updated_at",  StringType(),  True),
])

ENRICHMENT_SCHEMA = StructType([
    StructField("id",                    StringType(),  True),
    StructField("site_id",               StringType(),  True),   # null for campaign_enrichment
    StructField("campaign_id",           StringType(),  True),   # null for site_enrichment
    StructField("created_at",            StringType(),  True),
    StructField("updated_at",            StringType(),  True),
    StructField("resolved_domain",        StringType(),  True),
    StructField("main_category",         StringType(),  True),
    StructField("subcategory",           StringType(),  True),
    StructField("desktop_traffic_share", DoubleType(),  True),
    StructField("mobile_traffic_share",  DoubleType(),  True),
    StructField("male_share",            DoubleType(),  True),
    StructField("female_share",          DoubleType(),  True),
    StructField("age_18_to_24",          DoubleType(),  True),
    StructField("age_25_to_34",          DoubleType(),  True),
    StructField("age_35_to_44",          DoubleType(),  True),
    StructField("age_45_to_54",          DoubleType(),  True),
    StructField("age_55_to_64",          DoubleType(),  True),
    StructField("age_65_plus",           DoubleType(),  True),
    StructField("top_country",           StringType(),  True),
])
