"""Idempotency proof on the finished warehouse.

    uv run python scripts/rerun_proof.py --date 2026-09-15

1. fingerprint every table (rows, numeric sums, order-independent checksum of all columns)
2. run the daily job for DATE again, on top of the fully loaded warehouse
3. fingerprint again and compare; write reports/idempotency_proof.{json,md}
"""
import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from falcon import config, fingerprint as fp, pipeline  # noqa: E402
from falcon.spark import get_spark  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--config")
    ap.add_argument("--warmup", action="store_true",
                    help="run the day once first (not part of the proof); use after a code change so that the "
                         "'before' state was produced by the current code")
    a = ap.parse_args()
    cfg = config.load(a.config)
    spark = get_spark(cfg)
    spark.sparkContext.setLogLevel("ERROR")
    wh = cfg["warehouse_path"]
    if a.warmup:
        pipeline.load_day(spark, cfg, a.date)
    before = fp.fingerprint(spark, wh)
    t0 = time.time()
    res = pipeline.load_day(spark, cfg, a.date)
    after = fp.fingerprint(spark, wh)
    problems = fp.diff(before, after)
    out = Path(cfg["reports_path"])
    out.mkdir(parents=True, exist_ok=True)
    (out / "idempotency_proof.json").write_text(json.dumps(
        {"rerun_date": a.date, "seconds": round(time.time() - t0, 1), "identical": not problems, "differences": problems,
         "before": before, "after": after, "job_result": res}, indent=1, default=str))
    lines = [f"# Idempotency proof: re-running {a.date} on the fully loaded warehouse", "",
             f"Result: **{'IDENTICAL' if not problems else 'DIFFERENT'}** (job took {round(time.time() - t0, 1)} s)", "",
             "| table | rows before | rows after | sums before | sums after | checksum equal |", "|---|---:|---:|---|---|---|"]
    for t in before:
        b, c = before[t], after.get(t, {})
        lines.append(f"| {t} | {b['rows']:,} | {c.get('rows', 0):,} | {b['sums']} | {c.get('sums')} | {b['hash'] == c.get('hash')} |")
    if problems:
        lines += ["", "## Differences", *[f"- {p}" for p in problems]]
    (out / "idempotency_proof.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
