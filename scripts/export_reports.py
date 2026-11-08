"""Export the run outputs to reports/ (run after the full load).

    uv run python scripts/export_reports.py

Writes
  reports/dq_report.csv                 every check, every day  (dt, layer, table, check_name, n_bad, n_total, example, action)
  reports/dq_summary.md                 problems found: total rows, days affected, first example, action
  reports/dq_reconciliation.csv         reconciliation lines per day, with ok flag
  reports/reconciliation_per_day.md     staged impressions = fact + rejected, per day
  reports/table_row_counts.md           row counts of every output table
"""
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pyspark.sql import functions as F  # noqa: E402

from falcon import config  # noqa: E402
from falcon.common import list_partitions, read_table  # noqa: E402
from falcon.spark import get_spark  # noqa: E402

OUTPUT_TABLES = [
    "staging/requests", "staging/impressions", "staging/clicks", "staging/postbacks", "staging/orders",
    "staging/dim_publisher", "staging/dim_site", "staging/dim_placement", "staging/dim_platform",
    "staging/dim_category", "staging/dim_banner", "staging/dim_campaign", "staging/dim_advertiser",
    "staging/dim_site_enrichment", "staging/dim_campaign_enrichment",
    "dw/dim_placement", "dw/dim_banner", "dw/dim_site_enrichment", "dw/dim_campaign_enrichment",
    "dw/ev_request", "dw/ev_impression", "dw/ev_click", "dw/ev_postback", "dw/ev_order",
    "dw/fact_impression", "dw/fact_request", "dw/fact_order", "mart/ml_training",
    "dq/dq_checks", "dq/reconciliation", "dq/rejects", "ref/fx_raw", "ref/fx_daily", "ref/holiday",
    "ref/holiday_coverage"]


def write_csv(df, path):
    rows = df.collect()                       # report tables are small (days x checks)
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(df.columns)
        w.writerows([[("" if v is None else v) for v in r] for r in rows])


def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join("" if v is None else str(v).replace("|", "/") for v in r) + " |" for r in rows]
    return "\n".join(out)


def main():
    cfg = config.load(sys.argv[1] if len(sys.argv) > 1 else None)
    spark = get_spark(cfg)
    spark.sparkContext.setLogLevel("ERROR")
    wh, out = cfg["warehouse_path"], Path(cfg["reports_path"])
    out.mkdir(parents=True, exist_ok=True)

    checks = spark.read.parquet(f"{wh}/dq/dq_checks")
    write_csv(checks.orderBy("dt", "layer", "table", "check_name"), out / "dq_report.csv")
    rec = spark.read.parquet(f"{wh}/dq/reconciliation")
    write_csv(rec.select("dt", "table", "rule", "left_name", "left_rows", "right_name", "right_rows", "diff", "ok")
              .orderBy("dt", "table"), out / "dq_reconciliation.csv")

    # problems summary: only checks that fired
    s = (checks.filter("n_bad > 0").groupBy("layer", "table", "check_name", "action")
         .agg(F.sum("n_bad").alias("rows"), F.countDistinct("dt").alias("days"), F.min("dt").alias("first_day"),
              F.min(F.struct("dt", "example")).alias("ex"))
         .select("layer", "table", "check_name", "rows", "days", "first_day", F.col("ex.example").alias("example"), "action")
         .orderBy("layer", "table", F.desc("rows")))
    rows = [(r.layer, r.table, r.check_name, f"{r.rows:,}", r.days, r.first_day, r.example, r.action) for r in s.collect()]
    n_days = checks.select("dt").distinct().count()
    (out / "dq_summary.md").write_text(
        f"# Data quality summary ({n_days} days loaded)\n\nOnly checks that found something. Row counts are summed over days "
        f"(a late record is counted on the day it was loaded). Full per-day results: `dq_report.csv`.\n\n"
        + md_table(["layer", "table", "check", "rows", "days", "first day", "example", "what the pipeline did"], rows) + "\n")

    # per-day impression reconciliation: staged = accepted + rejected
    day = (rec.filter("`table` = 'ev_impression' and rule = 'staged = accepted + rejected'")
           .select("dt", "left_rows", "right_rows", "diff", "ok").orderBy("dt").collect())
    rej_df = read_table(spark, f"{wh}/dq/rejects")
    rej = {} if rej_df is None else {r.dt: r.n for r in rej_df.filter("source_table = 'ev_impression'")
                                     .groupBy("dt").agg(F.count(F.lit(1)).alias("n")).collect()}
    acc = {r.dt: r.n for r in spark.read.parquet(f"{wh}/dw/ev_impression").groupBy("dt").agg(F.count(F.lit(1)).alias("n")).collect()}
    (out / "reconciliation_per_day.md").write_text(
        "# Impressions: staging = event layer + rejected, per day\n\n"
        + md_table(["day", "staged", "accepted", "rejected", "accepted + rejected", "ok"],
                   [(r.dt, f"{r.left_rows:,}", f"{acc.get(r.dt, 0):,}", f"{rej.get(r.dt, 0):,}",
                     f"{acc.get(r.dt, 0) + rej.get(r.dt, 0):,}", r.ok) for r in day]) + "\n\n"
        f"Reconciliation lines: {rec.count()}  |  failing: {rec.filter('not ok').count()}\n")

    # outcome checks that depend on events that can still arrive: evaluated once, on the final table
    fi = spark.read.parquet(f"{wh}/dw/fact_impression")
    cap = cfg["dq"]["max_events_per_slot"]
    tot = fi.count()
    final = fi.agg(
        F.sum((F.col("first_click_at") < F.col("first_impression_at")).cast("int")).alias("click_before_impression"),
        F.sum((F.col("first_conversion_at") < F.col("first_impression_at")).cast("int")).alias("conversion_before_impression"),
        F.sum((F.col("is_conversion") & ~F.col("is_click")).cast("int")).alias("conversion_without_click"),
        F.sum((F.col("n_clicks") > cap).cast("int")).alias("slot_with_extreme_clicks"),
        F.sum((F.col("n_postbacks") > cap).cast("int")).alias("slot_with_extreme_postbacks"),
        F.sum((F.col("is_conversion") & (F.col("revenue_usd") == 0)).cast("int")).alias("conversion_revenue_zero"),
        F.sum(F.col("is_click").cast("int")).alias("slots_with_click"),
        F.sum(F.col("is_conversion").cast("int")).alias("slots_with_conversion"),
        F.sum(F.col("n_clicks")).alias("click_rows_in_fact"), F.sum(F.col("n_postbacks")).alias("postback_rows_in_fact")).first().asDict()
    ex = fi.filter(F.col("n_clicks") > cap).orderBy(F.desc("n_clicks")).select("fid", "n_clicks").first()
    (out / "dq_final_state.md").write_text(
        "# Outcome checks on the final fact_impression (computed once, after the last day)\n\n"
        f"Slots: {tot:,}\n\n" + md_table(["check", "slots"], [(k, f"{v:,}") for k, v in final.items()]) + "\n\n"
        + (f"Largest click count on one slot: {ex['n_clicks']:,} clicks on `{ex['fid']}`.\n" if ex else ""))

    counts = []
    for t in OUTPUT_TABLES:
        df = read_table(spark, f"{wh}/{t}")
        n = 0 if df is None else df.count()
        parts = len(list_partitions(spark, f"{wh}/{t}", "dt"))
        counts.append((t, f"{n:,}", parts or ""))
    (out / "table_row_counts.md").write_text(
        "# Row counts of all output tables after the full run\n\n" + md_table(["table", "rows", "dt partitions"], counts) + "\n")
    print((out / "table_row_counts.md").read_text())
    print("failing reconciliation lines:", rec.filter("not ok").count())


if __name__ == "__main__":
    main()
