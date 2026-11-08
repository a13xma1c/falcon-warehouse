"""External reference data, loaded by code (python stdlib ``urllib`` + PySpark only).

* FX rates      ``ref/fx_daily``   grain (rate_date, currency), rate_per_usd  (1 USD = rate_per_usd units)
                 primary source  : frankfurter (ECB reference rates, business days only)
                 fallback source : jsDelivr currency-api, for currencies frankfurter does not publish
                 Weekends / holidays are forward-filled from the last published day.
* Public holidays ``ref/holiday``  grain (country, holiday_date, name), from Nager.Date
                 ``ref/holiday_coverage`` grain (country, year): which country/years we fetched and whether
                 Nager supports the country (so "no holiday" and "unknown" can be told apart).

Both loaders are incremental and idempotent: they only fetch what is missing and rewrite the small
reference table.  All URLs / timeouts come from the config.  The HTTP function is injectable for tests.
"""
from __future__ import annotations
import datetime as _d
import json
import time
import urllib.error
import urllib.request
from typing import Callable, Iterable

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (ArrayType, BooleanType, DateType, DoubleType, StringType, StructField,
                               StructType, IntegerType)

from falcon.common import path_exists

HttpGet = Callable[[str], object]

FALLBACK_URL = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date}/v1/currencies/usd.json"


def make_http_get(timeout: int = 30, retries: int = 3, backoff: float = 2.0) -> HttpGet:
    """Return ``get(url) -> parsed JSON`` with retry/backoff (urllib only)."""
    def get(url: str):
        last = None
        for i in range(retries):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "falcon-warehouse/0.1"})
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    body = r.read()
                return json.loads(body) if body else None
            except urllib.error.HTTPError as e:
                if e.code in (400, 404):          # permanent: no retry
                    raise
                last = e
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
                last = e
            time.sleep(backoff * (i + 1))
        raise RuntimeError(f"GET {url} failed after {retries} tries: {last}")
    return get


def _daterange(a: _d.date, b: _d.date) -> list[_d.date]:
    return [a + _d.timedelta(i) for i in range((b - a).days + 1)]


# ═════════════════════════════════ FX ═══════════════════════════════════════

FX_RAW_SCHEMA = StructType([
    StructField("rate_date", StringType()), StructField("currency", StringType()),
    StructField("rate_per_usd", DoubleType()), StructField("source", StringType())])


def fetch_fx(get: HttpGet, cfg_fx: dict, start: _d.date, end: _d.date, currencies: Iterable[str],
             have: set[tuple[str, str]]) -> list[tuple]:
    """Rates for (date, currency) pairs published in [start, end] that are not in *have*.
    Returns rows (rate_date, currency, rate_per_usd, source)."""
    base = cfg_fx["base_url"].rstrip("/")
    bcur = cfg_fx.get("base_currency", "USD")
    wanted = sorted({c for c in currencies if c and c != bcur})
    if not wanted:
        return []
    supported = set(get(f"{base}/currencies").keys())
    primary = [c for c in wanted if c in supported]
    rows: list[tuple] = []
    if primary:
        need_dates = [d for d in _daterange(start, end)
                      if any((d.isoformat(), c) not in have for c in primary)]
        if need_dates:
            js = get(f"{base}/{start.isoformat()}..{end.isoformat()}?base={bcur}&symbols={','.join(primary)}")
            for day, rates in (js or {}).get("rates", {}).items():
                for c, v in rates.items():
                    if (day, c) not in have:
                        rows.append((day, c, float(v), "frankfurter"))
    fallback = [c for c in wanted if c not in supported]
    if fallback:
        for d in _daterange(start, end):
            miss = [c for c in fallback if (d.isoformat(), c) not in have]
            if not miss:
                continue
            try:
                js = get(FALLBACK_URL.format(date=d.isoformat()))
            except Exception:                       # day unavailable at the fallback source
                continue
            usd = (js or {}).get("usd", {})
            for c in miss:
                v = usd.get(c.lower())
                if v:
                    rows.append((d.isoformat(), c, float(v), "jsdelivr-currency-api"))
    return rows


def forward_fill(raw: list[tuple], start: _d.date, end: _d.date) -> list[tuple]:
    """Calendar-complete rates (rate_date, currency, rate_per_usd, source_date): for each currency and day
    the last published rate on or before that day.  Days before the first publication stay absent."""
    by_cur: dict[str, dict[str, tuple[float, str]]] = {}
    for day, cur, rate, src in raw:
        by_cur.setdefault(cur, {})[day] = (rate, src)
    out = []
    for cur, days in by_cur.items():
        last = None
        first_day = min(days)
        for d in _daterange(min(start, _d.date.fromisoformat(first_day)), end):
            k = d.isoformat()
            if k in days:
                last = (days[k][0], k)
            if last and d >= start:
                out.append((k, cur, last[0], last[1]))
    return out


def ensure_fx(spark: SparkSession, wh: str, cfg: dict, start: _d.date, end: _d.date,
              currencies: Iterable[str], get: HttpGet | None = None) -> None:
    """Make sure ``ref/fx_raw`` holds every published rate for *currencies* in [start - lookback, end],
    then rebuild the calendar-complete ``ref/fx_daily`` (USD = 1.0 included)."""
    fx = cfg["external"]["fx"]
    get = get or make_http_get(fx.get("timeout_seconds", 30), fx.get("retries", 3))
    start = start - _d.timedelta(days=fx.get("lookback_days", 7))
    raw_path, daily_path = f"{wh}/ref/fx_raw", f"{wh}/ref/fx_daily"
    old = spark.read.parquet(raw_path).collect() if path_exists(spark, raw_path) else []   # tiny table
    have = {(r.rate_date, r.currency) for r in old}
    # only what THIS call needs: a replay of a day asks for the same (currency, dates) as its first run, finds them
    # in `have`, and fetches nothing (the table is an append-only cache of immutable published rates)
    wanted = {c for c in currencies if c}
    new_rows = fetch_fx(get, fx, start, end, wanted, have)
    allrows = [(r.rate_date, r.currency, r.rate_per_usd, r.source) for r in old] + new_rows
    allrows = sorted(set(allrows))
    if new_rows or not path_exists(spark, raw_path):
        spark.createDataFrame(allrows, FX_RAW_SCHEMA).coalesce(1).write.mode("overwrite").parquet(raw_path + "__n")
        _swap(spark, raw_path + "__n", raw_path)
    # calendar-complete table over everything fetched so far (not just this call's range)
    first = min(_d.date.fromisoformat(r[0]) for r in allrows) if allrows else start
    first = min(first, start)
    prev_end = end
    if path_exists(spark, daily_path):
        prev_end = max(end, _d.date.fromisoformat(spark.read.parquet(daily_path).agg({"rate_date": "max"}).first()[0]))
    filled = forward_fill([(a, b, c, a) for a, b, c, _ in allrows], first, prev_end)
    usd = [((first + _d.timedelta(i)).isoformat(), "USD", 1.0, (first + _d.timedelta(i)).isoformat())
           for i in range((prev_end - first).days + 1)]
    df = spark.createDataFrame(filled + usd, "rate_date string, currency string, rate_per_usd double, source_date string")
    df.coalesce(1).write.mode("overwrite").parquet(daily_path + "__n")
    _swap(spark, daily_path + "__n", daily_path)


def _swap(spark: SparkSession, tmp: str, final: str) -> None:
    from falcon.common import _fs, delete_path
    delete_path(spark, final)
    p_tmp, fs = _fs(spark, tmp)
    fs.mkdirs(spark._jvm.org.apache.hadoop.fs.Path(final).getParent())
    if not fs.rename(p_tmp, spark._jvm.org.apache.hadoop.fs.Path(final)):
        raise RuntimeError(f"cannot move {tmp} to {final}")


def with_amount_usd(df, amount_col: str, currency_col: str, date_col: str, fx_daily):
    """Add ``amount_usd`` and ``fx_status`` to *df*.  *date_col* is a date column (event day).
    Never drops rows.  fx_status: ok | no_amount | non_positive | no_currency | no_rate."""
    fx = F.broadcast(fx_daily.select(F.col("rate_date").alias("_fx_d"), F.col("currency").alias("_fx_c"),
                                     F.col("rate_per_usd").alias("fx_rate_per_usd")))
    j = df.join(fx, (F.col(currency_col) == F.col("_fx_c")) & (F.col(date_col).cast("string") == F.col("_fx_d")), "left")
    ok = F.col(amount_col).isNotNull() & (F.col(amount_col) > 0) & F.col("fx_rate_per_usd").isNotNull()
    return (j.withColumn("amount_usd", F.when(ok, F.round(F.col(amount_col) / F.col("fx_rate_per_usd"), 4)))
            .withColumn("fx_status",
                        F.when(F.col(amount_col).isNull(), "no_amount")
                        .when(F.col(amount_col) <= 0, "non_positive")
                        .when(F.col(currency_col).isNull(), "no_currency")
                        .when(F.col("fx_rate_per_usd").isNull(), "no_rate")
                        .otherwise("ok"))
            .drop("_fx_d", "_fx_c"))


# ═══════════════════════════════ Holidays ═══════════════════════════════════

HOLIDAY_SCHEMA = StructType([
    StructField("country", StringType()), StructField("holiday_date", StringType()),
    StructField("name", StringType()), StructField("is_global", BooleanType()),
    StructField("counties", ArrayType(StringType()))])
COVERAGE_SCHEMA = StructType([
    StructField("country", StringType()), StructField("year", IntegerType()), StructField("supported", BooleanType())])


def ensure_holidays(spark: SparkSession, wh: str, cfg: dict, countries: Iterable[str], years: Iterable[int],
                    get: HttpGet | None = None) -> None:
    """Fetch Nager.Date public holidays for every (country, year) not fetched yet."""
    h = cfg["external"]["holidays"]
    get = get or make_http_get(h.get("timeout_seconds", 30), h.get("retries", 3))
    base = h["base_url"].rstrip("/")
    hol_path, cov_path = f"{wh}/ref/holiday", f"{wh}/ref/holiday_coverage"
    hol = [tuple(r) for r in spark.read.parquet(hol_path).collect()] if path_exists(spark, hol_path) else []
    cov = {(r.country, r.year): r.supported for r in spark.read.parquet(cov_path).collect()} if path_exists(spark, cov_path) else {}
    todo = sorted({(c, int(y)) for c in countries if c for y in years} - set(cov))
    if not todo:
        return
    avail = {x["countryCode"] for x in (get(f"{base}/AvailableCountries") or [])}
    for c, y in todo:
        if c not in avail:
            cov[(c, y)] = False
            continue
        try:
            items = get(f"{base}/PublicHolidays/{y}/{c}") or []
            cov[(c, y)] = True
        except urllib.error.HTTPError:
            items, cov[(c, y)] = [], False
        for it in items:
            if "Public" in (it.get("types") or ["Public"]):
                hol.append((c, it["date"], it.get("name"), bool(it.get("global", True)), it.get("counties") or []))
    hol = sorted({(a, b, c, d, tuple(e)) for a, b, c, d, e in hol})
    spark.createDataFrame([(a, b, c, d, list(e)) for a, b, c, d, e in hol], HOLIDAY_SCHEMA) \
        .coalesce(1).write.mode("overwrite").parquet(hol_path + "__n")
    _swap(spark, hol_path + "__n", hol_path)
    spark.createDataFrame([(c, y, s) for (c, y), s in sorted(cov.items())], COVERAGE_SCHEMA) \
        .coalesce(1).write.mode("overwrite").parquet(cov_path + "__n")
    _swap(spark, cov_path + "__n", cov_path)


def with_holiday_flag(df, country_col: str, state_col: str, date_col: str, holiday, coverage):
    """Add ``is_public_holiday``: true / false, or null when Nager.Date does not cover the country-year.

    A holiday counts if it is national, or regional and lists ``<country>-<state>`` in its counties.
    The flag is computed on the distinct (country, state, date) combinations and joined back, so the
    row count of *df* never changes."""
    combos = df.select(F.col(country_col).alias("_k_c"), F.col(state_col).alias("_k_s"),
                       F.col(date_col).alias("_k_d")).distinct()
    h = holiday.select(F.col("country").alias("_h_c"), F.col("holiday_date").alias("_h_d"), "is_global", "counties")
    j = combos.join(F.broadcast(h), (F.col("_k_c") == F.col("_h_c")) & (F.col("_k_d").cast("string") == F.col("_h_d")), "left")
    region = F.concat_ws("-", F.col("_k_c"), F.col("_k_s"))
    hit = F.col("_h_d").isNotNull() & (F.col("is_global") | F.array_contains(F.col("counties"), region))
    flags = j.groupBy("_k_c", "_k_s", "_k_d").agg(F.max(F.when(hit, 1).otherwise(0)).alias("_hit"))
    cov = coverage.filter("supported").select(F.col("country").alias("_cv_c"), F.col("year").alias("_cv_y"))
    flags = (flags.join(F.broadcast(cov), (F.col("_k_c") == F.col("_cv_c")) & (F.year("_k_d") == F.col("_cv_y")), "left")
             .select("_k_c", "_k_s", "_k_d",
                     F.when(F.col("_cv_c").isNull(), F.lit(None).cast("boolean"))
                     .otherwise(F.col("_hit") == 1).alias("is_public_holiday")))
    return df.join(flags, (df[country_col].eqNullSafe(F.col("_k_c"))) & (df[state_col].eqNullSafe(F.col("_k_s")))
                   & (df[date_col].eqNullSafe(F.col("_k_d"))), "left").drop("_k_c", "_k_s", "_k_d")
