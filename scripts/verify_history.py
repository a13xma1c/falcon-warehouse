"""Independent leakage check of the history features, on the real warehouse.

    uv run python scripts/verify_history.py --day 2026-10-12 [--publisher <id>]

The mart computes publisher history from fact_impression (arrival columns).  This script recomputes the same
numbers a second way, from RAW STAGING ONLY, as a job that ran on the morning of --day would have seen them:

    * only staging folders dt < DAY are read (events that arrived on DAY or later do not exist for it),
    * a slot counts for a request day D in [DAY-w, DAY-1],
    * clicks / conversions count if their folder is before DAY,
    * the publisher comes from the placement and site snapshots of the request day.

If the two numbers differ, a feature used information from the future (or from another place).
Prints a table and exits non-zero on any difference.  Writes reports/history_verification.md.
"""
import argparse
import datetime as _d
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pyspark.sql import Window, functions as F  # noqa: E402

from falcon import config  # noqa: E402
from falcon.common import fid_request_id, list_partitions  # noqa: E402
from falcon.spark import get_spark  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True)
    ap.add_argument("--publisher")
    ap.add_argument("--config")
    a = ap.parse_args()
    cfg = config.load(a.config)
    spark = get_spark(cfg)
    spark.sparkContext.setLogLevel("ERROR")
    wh, day = cfg["warehouse_path"], a.day
    windows = cfg["history"]["windows_days"]

    mart = spark.read.parquet(f"{wh}/mart/ml_training").filter(F.col("dt") == day)
    pub = a.publisher or mart.groupBy("publisher_id").count().orderBy(F.desc("count")).first()["publisher_id"]
    cols = [c for w in windows for c in (f"pub_slots_{w}d", f"pub_click_rate_{w}d", f"pub_conv_rate_{w}d")]
    got = mart.filter(F.col("publisher_id") == pub).select(*cols).distinct().collect()
    assert len(got) == 1, f"history must be identical for all rows of one publisher and day, got {len(got)} variants"
    got = got[0].asDict()

    def stg(t):                      # raw staging, folders strictly before DAY
        return spark.read.parquet(f"{wh}/staging/{t}").filter(F.col("dt") < day)

    first_row = Window.partitionBy("fid").orderBy("created_at", "impression_id")
    imp = (stg("impressions").withColumn("rid", fid_request_id(F.col("fid"))).where(F.col("rid").isNotNull())
           .withColumn("_r", F.row_number().over(first_row)).where("_r = 1")
           .select("fid", "rid", "placement_id"))
    slot_arr = stg("impressions").groupBy("fid").agg(F.min("dt").alias("slot_arr"))
    req = (stg("requests").where("request_id is not null and not coalesce(has_preview, false)")
           .withColumn("_r", F.row_number().over(Window.partitionBy("request_id").orderBy("dt")))
           .where("_r = 1")
           .select("request_id", F.substring("created_at", 1, 10).alias("req_day"), F.col("dt").alias("req_arr")))
    clk = stg("clicks").groupBy("fid").agg(F.min("dt").alias("click_arr"))
    pb = stg("postbacks").groupBy("fid").agg(F.min("dt").alias("conv_arr"))
    s = imp.join(slot_arr, "fid").join(req, imp.rid == req.request_id).join(clk, "fid", "left").join(pb, "fid", "left")
    s = s.where((F.col("slot_arr") < day) & (F.col("req_arr") < day))

    # publisher through the placement -> site snapshots of the request day (newest snapshot <= that day)
    snaps = list_partitions(spark, f"{wh}/staging/dim_placement", "dt")
    days = sorted({r[0] for r in s.select("req_day").distinct().collect()})     # <= 30 values
    cal = spark.createDataFrame([(d, max(x for x in snaps if x <= d)) for d in days if any(x <= d for x in snaps)],
                                "req_day string, snap string")
    # a snapshot can list one placement id twice (documented data problem): only the (placement, site) pair matters
    # here, so duplicates must not multiply the slots
    pl = spark.read.parquet(f"{wh}/staging/dim_placement").select(
        F.col("id").alias("pid"), "site_id", F.col("dt").alias("psnap")).distinct()
    site = spark.read.parquet(f"{wh}/staging/dim_site").select(
        F.col("id").alias("sid"), "publisher_id", F.col("dt").alias("ssnap")).distinct()
    s = (s.join(cal, "req_day")
         .join(pl, (s.placement_id == pl.pid) & (F.col("snap") == pl.psnap))
         .join(site, (pl.site_id == site.sid) & (F.col("snap") == site.ssnap))
         .filter(F.col("publisher_id") == pub))

    lines = [f"# History verification: publisher `{pub}`, request day {day}", "",
             "Recomputed from raw staging folders with dt < day only; mart values come from `ml_training`.", "",
             "| window | slots (staging) | slots (mart) | click rate (staging) | click rate (mart) "
             "| conv rate (staging) | conv rate (mart) | match |",
             "|---|---:|---:|---:|---:|---:|---:|---|"]
    ok = True
    fmt = lambda x: "null" if x is None else f"{x:.6f}"
    for w in windows:
        lo = (_d.date.fromisoformat(day) - _d.timedelta(days=w)).isoformat()
        win = s.filter((F.col("req_day") >= lo) & (F.col("req_day") < day))
        r = win.agg(F.count(F.lit(1)).alias("n"), F.sum(F.col("click_arr").isNotNull().cast("int")).alias("c"),
                    F.sum(F.col("conv_arr").isNotNull().cast("int")).alias("v")).first()
        n, c, v = r["n"], r["c"] or 0, r["v"] or 0
        exp = (n, (c / n) if n else None, (v / n) if n else None)
        m = (got[f"pub_slots_{w}d"], got[f"pub_click_rate_{w}d"], got[f"pub_conv_rate_{w}d"])
        same = exp[0] == m[0] and all((x is None and y is None) or (x is not None and y is not None and abs(x - y) < 1e-9)
                                      for x, y in zip(exp[1:], m[1:]))
        ok &= same
        lines.append(f"| {w}d | {exp[0]:,} | {m[0]:,} | {fmt(exp[1])} | {fmt(m[1])} | {fmt(exp[2])} | {fmt(m[2])} "
                     f"| {'yes' if same else 'NO'} |")
    out = Path(cfg["reports_path"])
    out.mkdir(parents=True, exist_ok=True)
    (out / "history_verification.md").write_text(
        "\n".join(lines) + f"\n\nResult: **{'all match' if ok else 'DIFFERENCES FOUND'}**\n")
    print("\n".join(lines))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
