"""Currency conversion and holiday flag, with a fake HTTP layer (no network in tests)."""
import datetime as d
import json

import pytest
from pyspark.sql import functions as F

from falcon import external


def fake_get_factory(calls):
    def get(url):
        calls.append(url)
        if url.endswith("/currencies"):
            return {"EUR": "Euro", "GBP": "Pound", "JPY": "Yen", "USD": "Dollar"}
        if "frankfurter" in url and ".." in url:
            wanted = url.split("symbols=")[1].split(",")
            table = {"2026-09-04": {"EUR": 0.8, "GBP": 0.5, "JPY": 100.0},                 # Friday
                     "2026-09-07": {"EUR": 0.9, "GBP": 0.6, "JPY": 110.0}}                 # Monday
            return {"rates": {day: {c: v for c, v in rates.items() if c in wanted} for day, rates in table.items()}}
        if "currency-api" in url:
            day = url.split("@")[1].split("/")[0]
            return {"date": day, "usd": {"aed": 3.6725}}
        if url.endswith("/AvailableCountries"):
            return [{"countryCode": "US"}, {"countryCode": "DE"}]
        if "/PublicHolidays/2026/US" in url:
            return [{"date": "2026-09-07", "name": "Labor Day", "global": True, "counties": None, "types": ["Public"]},
                    {"date": "2026-11-03", "name": "Bank", "global": True, "counties": None, "types": ["Bank"]}]
        if "/PublicHolidays/2026/DE" in url:
            return [{"date": "2026-01-06", "name": "Epiphany", "global": False, "counties": ["DE-BY", "DE-BW"], "types": ["Public"]}]
        raise AssertionError(url)
    return get


CFG = {"external": {"fx": {"base_url": "https://frankfurter.test/v1", "base_currency": "USD", "lookback_days": 0},
                    "holidays": {"base_url": "https://nager.test/api/v3"}}}


def test_fx_forward_fill_over_weekend_and_usd(spark, wh):
    external.ensure_fx(spark, wh, CFG, d.date(2026, 9, 4), d.date(2026, 9, 8), ["EUR", "JPY", "USD"], get=fake_get_factory([]))
    fx = {(r.rate_date, r.currency): r.rate_per_usd for r in spark.read.parquet(f"{wh}/ref/fx_daily").collect()}
    assert fx[("2026-09-04", "EUR")] == 0.8
    assert fx[("2026-09-05", "EUR")] == 0.8 and fx[("2026-09-06", "EUR")] == 0.8      # weekend: last published
    assert fx[("2026-09-07", "EUR")] == 0.9 and fx[("2026-09-08", "EUR")] == 0.9
    assert all(fx[(f"2026-09-0{i}", "USD")] == 1.0 for i in range(4, 9))


def test_amount_usd_conversion_and_statuses(spark, wh):
    external.ensure_fx(spark, wh, CFG, d.date(2026, 9, 4), d.date(2026, 9, 8), ["EUR", "JPY", "USD"], get=fake_get_factory([]))
    fx = spark.read.parquet(f"{wh}/ref/fx_daily")
    rows = [("eur", 80.0, "EUR", "2026-09-04"),    # 80 / 0.8 = 100
            ("jpy", 11000.0, "JPY", "2026-09-07"),  # 11000 / 110 = 100
            ("usd", 42.5, "USD", "2026-09-05"),
            ("zero", 0.0, "EUR", "2026-09-04"),
            ("neg", -5.0, "EUR", "2026-09-04"),
            ("noamt", None, "EUR", "2026-09-04"),
            ("nocur", 10.0, None, "2026-09-04"),
            ("unknown", 10.0, "XXX", "2026-09-04"),
            ("early", 10.0, "EUR", "2026-09-03")]  # before the first published rate: no rate, no guessing
    df = spark.createDataFrame(rows, "id string, amount double, cur string, day string").withColumn("day", F.to_date("day"))
    out = {r.id: (r.amount_usd, r.fx_status) for r in external.with_amount_usd(df, "amount", "cur", "day", fx).collect()}
    assert out["eur"] == (100.0, "ok") and out["jpy"] == (100.0, "ok") and out["usd"] == (42.5, "ok")
    assert out["zero"] == (None, "non_positive") and out["neg"] == (None, "non_positive")
    assert out["noamt"] == (None, "no_amount") and out["nocur"] == (None, "no_currency")
    assert out["unknown"] == (None, "no_rate") and out["early"] == (None, "no_rate")
    assert len(out) == len(rows)       # never drops a row


def test_currency_missing_at_primary_source_uses_fallback(spark, wh):
    external.ensure_fx(spark, wh, CFG, d.date(2026, 9, 4), d.date(2026, 9, 5), ["AED"], get=fake_get_factory([]))
    r = spark.read.parquet(f"{wh}/ref/fx_daily").filter("currency = 'AED'").collect()
    assert {x.rate_per_usd for x in r} == {3.6725} and len(r) == 2


def test_fx_loader_is_idempotent_and_does_not_refetch_known_currency_history(spark, wh):
    calls = []
    get = fake_get_factory(calls)
    ranges = lambda: [c for c in calls if ".." in c]
    external.ensure_fx(spark, wh, CFG, d.date(2026, 9, 4), d.date(2026, 9, 8), ["EUR"], get=get)
    first = sorted(tuple(r) for r in spark.read.parquet(f"{wh}/ref/fx_raw").collect())
    assert len(first) == 2 and len(ranges()) == 1
    external.ensure_fx(spark, wh, CFG, d.date(2026, 9, 4), d.date(2026, 9, 8), ["EUR"], get=get)
    assert sorted(tuple(r) for r in spark.read.parquet(f"{wh}/ref/fx_raw").collect()) == first    # same rows, no duplicates
    assert len(ranges()) <= 2                                                                    # at most one range call per run


def test_holiday_flag(spark, wh):
    external.ensure_holidays(spark, wh, CFG, ["US", "DE", "AE"], [2026], get=fake_get_factory([]))
    hol, cov = spark.read.parquet(f"{wh}/ref/holiday"), spark.read.parquet(f"{wh}/ref/holiday_coverage")
    df = spark.createDataFrame([("US", "CA", "2026-09-07"),      # Labor Day -> true
                                ("US", "CA", "2026-09-08"),      # normal day -> false
                                ("US", "CA", "2026-11-03"),      # only a 'Bank' day -> false
                                ("DE", "BY", "2026-01-06"),      # regional holiday in Bavaria -> true
                                ("DE", "NW", "2026-01-06"),      # same day, other state -> false
                                ("AE", "DU", "2026-09-07"),      # country not supported -> null
                                ("US", "CA", "2027-01-01"),      # year not loaded -> null
                                (None, None, "2026-09-07")], "c string, s string, d string").withColumn("d", F.to_date("d"))
    out = external.with_holiday_flag(df, "c", "s", "d", hol, cov)
    got = {(r.c, r.s, str(r.d)): r.is_public_holiday for r in out.collect()}
    assert got[("US", "CA", "2026-09-07")] is True
    assert got[("US", "CA", "2026-09-08")] is False
    assert got[("US", "CA", "2026-11-03")] is False
    assert got[("DE", "BY", "2026-01-06")] is True and got[("DE", "NW", "2026-01-06")] is False
    assert got[("AE", "DU", "2026-09-07")] is None and got[("US", "CA", "2027-01-01")] is None
    assert got[(None, None, "2026-09-07")] is None
    assert out.count() == df.count()


def test_holiday_fetch_is_not_repeated(spark, wh):
    calls = []
    get = fake_get_factory(calls)
    external.ensure_holidays(spark, wh, CFG, ["US"], [2026], get=get)
    n = len(calls)
    external.ensure_holidays(spark, wh, CFG, ["US"], [2026], get=get)
    assert len(calls) == n
