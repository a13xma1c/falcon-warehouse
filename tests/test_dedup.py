"""Deduplication rules of the event layer."""
import datetime as dt
from pyspark.sql import functions as F

from falcon.events import _with_reasons


def ts(s):
    return dt.datetime.fromisoformat(s)


SCHEMA = "click_id string, fid string, created_at timestamp, status string, dt string, _src_file string"


def _reasons(spark, wh, rows, day):
    df = spark.createDataFrame(rows, SCHEMA)
    out = _with_reasons(spark, wh, df, "ev_click", ["click_id"], day, "created_at")
    return {(r.click_id, r.created_at.isoformat() if r.created_at else None): r.reject_reason for r in out.collect()}


def test_duplicates_in_day_keep_earliest_row(spark, wh):
    rows = [("c1", "f", ts("2026-09-02 10:00:05"), "x", "2026-09-02", "f"),
            ("c1", "f", ts("2026-09-02 10:00:01"), "x", "2026-09-02", "f"),     # earlier: kept
            ("c2", "f", ts("2026-09-02 11:00:00"), "x", "2026-09-02", "f")]
    got = _reasons(spark, wh, rows, "2026-09-02")
    assert got[("c1", "2026-09-02T10:00:01")] is None
    assert got[("c1", "2026-09-02T10:00:05")] == "duplicate_in_day"
    assert got[("c2", "2026-09-02T11:00:00")] is None


def test_exact_duplicate_rows_keep_exactly_one(spark, wh):
    r = ("c1", "f", ts("2026-09-02 10:00:00"), "x", "2026-09-02", "f")
    df = spark.createDataFrame([r, r, r], SCHEMA)
    out = _with_reasons(spark, wh, df, "ev_click", ["click_id"], "2026-09-02", "created_at")
    assert out.filter("reject_reason is null").count() == 1
    assert out.filter("reject_reason = 'duplicate_in_day'").count() == 2


def test_key_seen_in_earlier_folder_is_rejected(spark, wh):
    prev = spark.createDataFrame([("c1", "2026-09-01")], "click_id string, dt string")
    prev.write.partitionBy("dt").parquet(f"{wh}/dw/ev_click")
    rows = [("c1", "f", ts("2026-09-01 23:59:59"), "x", "2026-09-02", "f"),
            ("c3", "f", ts("2026-09-02 08:00:00"), "x", "2026-09-02", "f")]
    got = _reasons(spark, wh, rows, "2026-09-02")
    assert got[("c1", "2026-09-01T23:59:59")] == "duplicate_of_earlier_day"
    assert got[("c3", "2026-09-02T08:00:00")] is None


def test_same_day_rerun_is_not_a_duplicate_of_itself(spark, wh):
    # the key is already in dt=2026-09-02 from the first run; the second run must still accept it
    prev = spark.createDataFrame([("c1", "2026-09-02")], "click_id string, dt string")
    prev.write.partitionBy("dt").parquet(f"{wh}/dw/ev_click")
    rows = [("c1", "f", ts("2026-09-02 08:00:00"), "x", "2026-09-02", "f")]
    assert _reasons(spark, wh, rows, "2026-09-02")[("c1", "2026-09-02T08:00:00")] is None


def test_null_key_is_rejected_with_its_own_reason(spark, wh):
    rows = [(None, "f", ts("2026-09-02 08:00:00"), "x", "2026-09-02", "f")]
    assert list(_reasons(spark, wh, rows, "2026-09-02").values()) == ["null_click_id"]


def test_composite_key(spark, wh):
    schema = "order_id string, site_id string, created_at timestamp, dt string, _src_file string"
    df = spark.createDataFrame([("o1", "s1", ts("2026-09-02 08:00:00"), "d", "f"),
                                ("o1", "s2", ts("2026-09-02 08:00:00"), "d", "f"),      # same order id, other site: ok
                                ("o1", "s1", ts("2026-09-02 09:00:00"), "d", "f")], schema)
    out = _with_reasons(spark, wh, df, "ev_order", ["order_id", "site_id"], "2026-09-02", "created_at")
    assert out.filter("reject_reason is null").count() == 2
    assert out.filter("reject_reason = 'duplicate_in_day'").count() == 1
