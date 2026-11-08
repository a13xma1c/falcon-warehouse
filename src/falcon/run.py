"""CLI.

    uv run python -m falcon.run --date 2026-09-25                 # one day, on top of what is loaded
    uv run python -m falcon.run --from 2026-08-24 --to 2026-10-25  # a range, in date order
    uv run python -m falcon.run --all
    uv run python -m falcon.run --date 2026-09-25 --config my.json

The Java/JVM must be on the PATH (JAVA_HOME) for local Spark.
"""
from __future__ import annotations
import argparse
import json

from falcon import config, pipeline
from falcon.spark import get_spark


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="Falcon warehouse daily job")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", help="load a single day folder dt=YYYY-MM-DD")
    g.add_argument("--all", action="store_true", help="load all folders in date order")
    g.add_argument("--from", dest="start", help="first day of a range")
    ap.add_argument("--to", dest="end", help="last day of a range (with --from)")
    ap.add_argument("--config", help="path to config.json (default: config/config.json or $FALCON_CONFIG)")
    a = ap.parse_args(argv)
    cfg = config.load(a.config)
    spark = get_spark(cfg)
    spark.sparkContext.setLogLevel("ERROR")
    if a.date:
        print(json.dumps(pipeline.load_day(spark, cfg, a.date)))
    elif a.all:
        pipeline.run_range(spark, cfg)
    else:
        pipeline.run_range(spark, cfg, a.start, a.end)


if __name__ == "__main__":
    main()
