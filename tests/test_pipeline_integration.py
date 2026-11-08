"""Integration test: three real day folders, loaded in order, then re-run.

It checks the two promises of the assignment:
  * a later folder UPDATES earlier days (late clicks / conversions / impressions),
  * running the same day twice changes nothing (rows, sums and a checksum of every table).

Needs the dataset (config dataset_path); skipped when it is not there.  No network: FX and holiday
calls are served by a fake that is good enough for the join logic (rates are fixed numbers).
Takes about two minutes.
"""
import json
import os
import shutil
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from falcon import config, fingerprint as fp, pipeline
from falcon.spark import get_spark

DAYS = ["2026-09-24", "2026-09-25", "2026-09-26"]
CFG = config.load()
SRC = Path(CFG["dataset_path"])
if not SRC.is_absolute():
    SRC = Path(__file__).resolve().parents[1] / SRC

pytestmark = pytest.mark.skipif(not all((SRC / f"dt={d}").exists() for d in DAYS), reason="dataset not available")


def fake_fx(url):
    if url.endswith("/currencies"):
        return {c: c for c in ["EUR", "GBP", "AUD", "CAD", "NZD", "CHF", "DKK", "ILS", "SEK", "PLN", "JPY", "USD"]}
    if ".." in url:
        a, b = url.split("/v1/")[1].split("?")[0].split("..")
        import datetime as d
        start, end = d.date.fromisoformat(a), d.date.fromisoformat(b)
        syms = url.split("symbols=")[1].split(",")
        return {"rates": {(start + d.timedelta(i)).isoformat(): {s: 0.9 for s in syms}
                          for i in range((end - start).days + 1) if (start + d.timedelta(i)).weekday() < 5}}
    raise AssertionError(url)


def fake_holiday(url):
    if url.endswith("/AvailableCountries"):
        return [{"countryCode": "US"}, {"countryCode": "GB"}, {"countryCode": "DE"}]
    return [{"date": "2026-09-25", "name": "Test day", "global": True, "counties": None, "types": ["Public"]}]


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    root = tmp_path_factory.mktemp("falcon_it")
    ds = root / "dataset"
    ds.mkdir()
    for d in DAYS:
        os.symlink(SRC / f"dt={d}", ds / f"dt={d}")
    shutil.copy(SRC / "manifest.json", ds / "manifest.json")
    cfg = json.loads(json.dumps(CFG))
    cfg["dataset_path"], cfg["warehouse_path"] = str(ds), str(root / "wh")
    cfg["spark"].update({"master": "local[4]", "driver_memory": "4g"})
    spark = get_spark(cfg)
    spark.sparkContext.setLogLevel("ERROR")
    yield spark, cfg
    spark.stop()


def load(spark, cfg, d):
    return pipeline.load_day(spark, cfg, d, get_fx=fake_fx, get_holiday=fake_holiday)


def test_late_events_update_earlier_days_and_reruns_change_nothing(env):
    spark, cfg = env
    wh = cfg["warehouse_path"]

    # a day cannot be loaded before its predecessors
    with pytest.raises(RuntimeError, match="earlier folders not loaded"):
        load(spark, cfg, DAYS[1])

    load(spark, cfg, DAYS[0])
    after_day1 = {r["dt"]: r for r in spark.read.parquet(f"{wh}/dw/fact_impression")
                  .groupBy("dt").agg(F.sum("n_clicks").alias("clicks"), F.sum("n_postbacks").alias("pb"),
                                     F.count(F.lit(1)).alias("slots")).collect()}
    load(spark, cfg, DAYS[1])
    load(spark, cfg, DAYS[2])
    after_day3 = {r["dt"]: r for r in spark.read.parquet(f"{wh}/dw/fact_impression")
                  .groupBy("dt").agg(F.sum("n_clicks").alias("clicks"), F.sum("n_postbacks").alias("pb"),
                                     F.count(F.lit(1)).alias("slots")).collect()}

    # 1) the first day was updated by later folders: clicks / conversions of day-1 slots arrived on days 2 and 3
    d1 = DAYS[0]
    assert after_day3[d1]["clicks"] > after_day1[d1]["clicks"]
    assert after_day3[d1]["slots"] >= after_day1[d1]["slots"]
    # the same labels reach the mart
    ml = spark.read.parquet(f"{wh}/mart/ml_training").filter(F.col("dt") == d1)
    assert ml.agg(F.sum("is_click")).first()[0] > 0
    assert ml.count() == spark.read.parquet(f"{wh}/dw/fact_impression").filter(
        (F.col("dt") == d1) & F.col("request_found") & ~F.col("has_preview")).count()

    # 2) the fact table is keyed: one row per slot, one mart row per slot
    fi = spark.read.parquet(f"{wh}/dw/fact_impression")
    assert fi.count() == fi.select("fid").distinct().count()
    mt = spark.read.parquet(f"{wh}/mart/ml_training")
    assert mt.count() == mt.select("fid").distinct().count()

    # 3) every reconciliation line holds
    assert spark.read.parquet(f"{wh}/dq/reconciliation").filter("not ok").count() == 0

    # 4) re-running days (the last one and an old one) changes nothing
    before = fp.fingerprint(spark, wh)
    load(spark, cfg, DAYS[2])
    assert fp.diff(before, fp.fingerprint(spark, wh)) == []
    load(spark, cfg, DAYS[0])
    assert fp.diff(before, fp.fingerprint(spark, wh)) == []
