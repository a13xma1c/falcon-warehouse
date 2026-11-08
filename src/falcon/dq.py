"""Data-quality collector.

Every pipeline step reports *checks*; each check is one row in ``dq/dq_checks``:

    dt          load day (folder date) the check ran for
    layer       staging | event | fact | dim | mart | external
    table       table the check is about
    check_name  stable identifier
    n_bad       rows that failed the check
    n_total     rows the check looked at
    example     one example (key or value) of a failing row, null when n_bad = 0
    action      what the pipeline did with those rows (rejected / kept + flagged / ...)

All checks of a table are evaluated in ONE Spark aggregation (``Collector.run``), so reporting
costs one pass per table, not one pass per check.  Checks with n_bad = 0 are written too: the
report shows what was looked at, not only what failed.

Other outputs:
    dq/rejects          one row per rejected record: dt, source_table, reason, record_key, detail
    dq/reconciliation   rows in = rows out + rejected, per day and table
"""
from __future__ import annotations
from dataclasses import dataclass
from pyspark.sql import Column, DataFrame, SparkSession, functions as F

from falcon.common import write_day_partition

CHECK_SCHEMA = ("dt string, layer string, `table` string, check_name string, n_bad long, n_total long, "
                "example string, action string")
REJECT_SCHEMA = "dt string, source_table string, reason string, record_key string, detail string"
RECON_SCHEMA = ("dt string, `table` string, rule string, left_name string, left_rows long, right_name string, "
                "right_rows long, diff long, ok boolean")


@dataclass
class Spec:
    name: str
    cond: Column           # true -> the row is bad
    example: Column        # string describing the row (usually the key)
    action: str


class Collector:
    def __init__(self, dt: str):
        self.dt = dt
        self.checks: list[tuple] = []
        self.rejects: list[DataFrame] = []
        self.recons: list[tuple] = []

    # ---- checks ------------------------------------------------------------
    def run(self, df: DataFrame, layer: str, table: str, specs: list[Spec]) -> int:
        """Evaluate all *specs* on *df* in a single pass.  Returns df row count."""
        aggs = [F.count(F.lit(1)).alias("_n")]
        for i, s in enumerate(specs):
            aggs.append(F.sum(F.when(s.cond, 1).otherwise(0)).alias(f"b{i}"))
            aggs.append(F.min(F.when(s.cond, s.example.cast("string"))).alias(f"e{i}"))   # min = deterministic example
        row = df.agg(*aggs).first()
        n = row["_n"] or 0
        for i, s in enumerate(specs):
            bad = int(row[f"b{i}"] or 0)
            self.checks.append((self.dt, layer, table, s.name, bad, n, row[f"e{i}"] if bad else None, s.action))
        return n

    def add(self, layer: str, table: str, name: str, n_bad: int, n_total: int, example, action: str) -> None:
        self.checks.append((self.dt, layer, table, name, int(n_bad), int(n_total),
                            None if example is None else str(example), action))

    # ---- rejects -----------------------------------------------------------
    def add_rejects(self, source_table: str, df: DataFrame, key: Column, detail: Column | None = None) -> None:
        """*df* must have a ``reject_reason`` column (non-null = rejected)."""
        self.rejects.append(
            df.filter(F.col("reject_reason").isNotNull())
            .select(F.lit(self.dt).alias("dt"), F.lit(source_table).alias("source_table"),
                    F.col("reject_reason").alias("reason"), key.cast("string").alias("record_key"),
                    (detail if detail is not None else F.lit(None)).cast("string").alias("detail")))

    def reject_checks(self, layer: str, table: str, df: DataFrame, reasons: list[str], total: int, action: str) -> None:
        """One check row per possible rejection reason (zero rows included)."""
        rows = {r["reject_reason"]: (r["n"], r["ex"]) for r in
                df.filter(F.col("reject_reason").isNotNull()).groupBy("reject_reason")
                .agg(F.count(F.lit(1)).alias("n"), F.min("_ex").alias("ex")).collect()}
        for reason in sorted(set(reasons) | set(rows)):
            n, ex = rows.get(reason, (0, None))
            self.add(layer, table, f"rejected_{reason}", n, total, ex, action)

    # ---- reconciliation ----------------------------------------------------
    def recon(self, table: str, rule: str, left_name: str, left: int, right_name: str, right: int) -> None:
        self.recons.append((self.dt, table, rule, left_name, int(left), right_name, int(right),
                            int(left - right), left == right))

    # ---- output ------------------------------------------------------------
    def write_rejects(self, spark: SparkSession, wh: str) -> None:
        """Rejects first: the cumulative reconciliation reads them back."""
        rej = None
        for d in self.rejects:
            rej = d if rej is None else rej.unionByName(d)
        if rej is None:
            rej = spark.createDataFrame([], REJECT_SCHEMA)
        write_day_partition(spark, rej, f"{wh}/dq/rejects", "dt", self.dt)

    def write(self, spark: SparkSession, wh: str) -> None:
        write_day_partition(spark, spark.createDataFrame(self.checks, CHECK_SCHEMA), f"{wh}/dq/dq_checks", "dt", self.dt)
        write_day_partition(spark, spark.createDataFrame(self.recons, RECON_SCHEMA), f"{wh}/dq/reconciliation", "dt", self.dt)
