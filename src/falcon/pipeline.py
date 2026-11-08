"""Daily job: ``load_day(spark, cfg, dt)`` processes ONE folder on top of what is already loaded.

Order inside a day (every step replaces only its own partitions, so re-running a day is safe):

  1. staging   raw copy of the folder with explicit schemas                 staging/*/dt=D
  2. dims      dimension snapshots of the day                               dw/dim_*/dt=D
  3. events    typed, de-duplicated events (partition = arrival day)        dw/ev_*/dt=D, dq/rejects
  4. facts     fact_impression / fact_request / fact_order, late-event rewrite of earlier days
  5. mart      ml_training rows of the rebuilt slots
  6. dq        folder-vs-manifest, cumulative reconciliation, dq/dq_checks

``run_range`` walks the folders in date order like a scheduler would.  A day is refused when an earlier
folder that exists on disk has not been loaded yet (no silent gaps).
"""
from __future__ import annotations
import datetime as _d
import json
import os
import re
import time
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

from falcon import dims, events, facts, mart, staging
from falcon.common import list_partitions, read_table
from falcon.dq import Collector, Spec

MANIFEST_TABLES = {"requests": "requests", "impressions": "impressions", "clicks": "clicks",
                   "postbacks": "postbacks", "orders": "orders", "publisher": "dim_publisher", "site": "dim_site",
                   "placement": "dim_placement", "platform": "dim_platform", "category": "dim_category",
                   "banner": "dim_banner", "campaign": "dim_campaign", "advertiser": "dim_advertiser",
                   "site_enrichment": "dim_site_enrichment", "campaign_enrichment": "dim_campaign_enrichment"}


def available_days(dataset: str) -> list[str]:
    return sorted(m.group(1) for n in os.listdir(dataset) if (m := re.fullmatch(r"dt=(\d{4}-\d{2}-\d{2})", n)))


def loaded_days(spark, wh) -> list[str]:
    """Days whose load finished: dq/reconciliation is the last thing a day writes."""
    return list_partitions(spark, f"{wh}/dq/reconciliation", "dt")


def _manifest(dataset: str) -> dict:
    p = Path(dataset) / "manifest.json"
    return json.loads(p.read_text())["rows"] if p.exists() else {}


def check_manifest(spark, wh, dq: Collector, dataset: str, dt: str) -> None:
    """staged rows of the day vs. the row counts promised by manifest.json."""
    man = _manifest(dataset).get(f"dt={dt}")
    if man is None:
        dq.add("staging", "folder", "manifest_entry_missing", 1, 1, dt, "reported")
        return
    for name, stg_name in MANIFEST_TABLES.items():
        n = spark.read.parquet(f"{wh}/staging/{stg_name}").filter(F.col("dt") == dt).count()
        dq.recon(f"staging.{stg_name}", "manifest rows = staged rows", "manifest", man.get(name, 0), "staging", n)
        dq.add("staging", stg_name, "staged_vs_manifest", abs(man.get(name, 0) - n), man.get(name, 0),
               None if man.get(name, 0) == n else f"manifest={man.get(name, 0)} staged={n}",
               "reported; staging is the raw copy")


def cumulative_reconciliation(spark, wh, dq: Collector, dt: str) -> None:
    """Warehouse totals AS OF folder *dt*: everything is cut at arrival day <= dt, and every table used here is
    partitioned by arrival day (or carries the arrival day), so the numbers do not depend on later folders and a
    replay of *dt* writes exactly the same lines."""
    def ev(t):
        return spark.read.parquet(f"{wh}/dw/{t}").filter(F.col("dt") <= dt)

    def stg_total(t):
        return spark.read.parquet(f"{wh}/staging/{t}").filter(F.col("dt") <= dt).count()

    def rej(t):
        r = read_table(spark, f"{wh}/dq/rejects")          # None while nothing was ever rejected
        return 0 if r is None else r.filter((F.col("dt") <= dt) & (F.col("source_table") == t)).count()

    fi = spark.read.parquet(f"{wh}/dw/fact_impression")
    fi_fids = fi.select("fid")
    imp = ev("ev_impression")
    imp_fids = imp.select("fid").distinct()

    n_in_fact = imp.join(fi_fids, "fid").count()
    dq.recon("ev_impression", "staged = rows of slots in fact_impression + rejected (as of day)", "staging rows",
             stg_total("impressions"), "impression rows in fact + rejected", n_in_fact + rej("ev_impression"))

    for ev_t, st_t in [("ev_click", "clicks"), ("ev_postback", "postbacks")]:
        e = ev(ev_t)
        attached = e.join(imp_fids, "fid").join(fi_fids, "fid").count()
        orphans = e.join(imp_fids, "fid", "left_anti").count()
        dq.recon(ev_t, "staged = attached to a slot in fact_impression + without impression + rejected (as of day)",
                 "staging rows", stg_total(st_t), "attached + orphans + rejected", attached + orphans + rej(ev_t))
        today = spark.read.parquet(f"{wh}/dw/{ev_t}").filter(F.col("dt") == dt)
        r = today.select("fid").join(imp_fids, "fid", "left_anti").agg(F.count(F.lit(1)), F.min("fid")).first()
        dq.add("fact", "fact_impression", f"{st_t}_without_impression_on_arrival", r[0], today.count(), r[1],
               "kept in ev_*; no slot to attach to yet; attached automatically if the impression arrives later")

    dq.recon("fact_request", "accepted requests = fact_request rows (as of day)", "ev_request", ev("ev_request").count(),
             "fact_request", spark.read.parquet(f"{wh}/dw/fact_request").filter(F.col("arrival_dt") <= dt).count())
    dq.recon("fact_order", "accepted orders = fact_order rows (as of day)", "ev_order", ev("ev_order").count(),
             "fact_order", spark.read.parquet(f"{wh}/dw/fact_order").filter(F.col("arrival_dt") <= dt).count())
    slots = fi.filter(F.col("slot_arrival_dt") <= dt)
    dq.recon("fact_impression", "distinct slots = rows (as of day)", "distinct fid", slots.select("fid").distinct().count(),
             "rows", slots.count())
    eligible = slots.filter((F.col("request_arrival_dt") <= dt) & ~F.col("has_preview")).select("fid")
    dq.recon("ml_training", "slots with a real request = mart rows (as of day)", "eligible slots in fact_impression",
             eligible.count(), "of them in ml_training",
             eligible.join(spark.read.parquet(f"{wh}/mart/ml_training").select("fid"), "fid").count())


def load_day(spark: SparkSession, cfg: dict, dt: str, get_fx=None, get_holiday=None, check_order: bool = True) -> dict:
    t0 = time.time()
    wh, ds = cfg["warehouse_path"], cfg["dataset_path"]
    avail = available_days(ds)
    if dt not in avail:
        raise ValueError(f"no folder dt={dt} in {ds}")
    if check_order:
        done = set(loaded_days(spark, wh))
        missing = [d for d in avail if d < dt and d not in done]
        if missing:
            raise RuntimeError(f"refusing to load {dt}: earlier folders not loaded yet: {missing[:5]}")
    dq = Collector(dt)
    staging.stage_day(spark, ds, wh, dt)
    dims_counts = dims.load_dims_day(spark, wh, dt, dq)
    events.load_requests(spark, wh, dq, dt)
    events.load_impressions(spark, wh, dq, dt)
    events.load_clicks(spark, wh, dq, dt)
    events.load_postbacks(spark, wh, dq, dt)
    events.load_orders(spark, wh, dq, dt)
    r = facts.load_facts_day(spark, wh, cfg, dq, dt, get_fx, get_holiday)
    m = mart.load_mart_day(spark, wh, cfg, dq, dt, r["slots"], r["touched_fids"])
    check_manifest(spark, wh, dq, ds, dt)
    # gap in the calendar: a day without a folder (reported on the next existing day)
    prev = [d for d in avail if d < dt]
    if prev:
        gap = (_d.date.fromisoformat(dt) - _d.date.fromisoformat(prev[-1])).days - 1
        if gap > 0:
            dq.add("staging", "folder", "missing_day_folder", gap, gap,
                   (_d.date.fromisoformat(dt) - _d.timedelta(days=1)).isoformat(),
                   "reported; events of the missing day arrive in the next folder (partitioned by event time)")
    dq.write_rejects(spark, wh)
    cumulative_reconciliation(spark, wh, dq, dt)
    dq.write(spark, wh)
    for k in ("touched_fids", "touched_requests", "slots", "requests", "orders"):
        r[k].unpersist()
    m.unpersist()
    spark.catalog.clearCache()          # nothing cached may live across days (driver memory on a long run)
    return {"dt": dt, "seconds": round(time.time() - t0, 1), "touched_slots": r["n_touched"],
            "rebuilt_slots": r["n_slots"], "dims": dims_counts}


def run_range(spark, cfg, start: str | None = None, end: str | None = None) -> list[dict]:
    days = [d for d in available_days(cfg["dataset_path"]) if (not start or d >= start) and (not end or d <= end)]
    out = []
    for d in days:
        res = load_day(spark, cfg, d)
        print(json.dumps(res), flush=True)
        out.append(res)
    return out
