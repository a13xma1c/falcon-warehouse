"""Shared helpers: Hadoop-FS partition handling, idempotent partition writes,
point-in-time snapshot calendar and fid decoding.  Python stdlib + PySpark only."""
from __future__ import annotations
import base64
import datetime as _d
from pyspark.sql import DataFrame, SparkSession, Column, functions as F


# ───────────────────────── filesystem (local, HDFS, S3A, DBFS all work) ─────

def _fs(spark: SparkSession, path: str):
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    return p, p.getFileSystem(spark._jsc.hadoopConfiguration())


def path_exists(spark: SparkSession, path: str) -> bool:
    p, fs = _fs(spark, path)
    return fs.exists(p)


def delete_path(spark: SparkSession, path: str) -> None:
    p, fs = _fs(spark, path)
    if fs.exists(p):
        fs.delete(p, True)


def list_partitions(spark: SparkSession, path: str, part_col: str = "dt") -> list[str]:
    """Sorted partition values under *path* (directory listing only, no data read)."""
    p, fs = _fs(spark, path)
    if not fs.exists(p):
        return []
    prefix = part_col + "="
    names = [s.getPath().getName() for s in fs.listStatus(p) if s.isDirectory()]
    return sorted(n[len(prefix):] for n in names if n.startswith(prefix))


def read_table(spark: SparkSession, path: str) -> DataFrame | None:
    """Parquet table (partitioned or flat), or None if nothing was ever written there."""
    p, fs = _fs(spark, path)
    if not fs.exists(p):
        return None
    if any(not s.getPath().getName().startswith(("_", ".")) for s in fs.listStatus(p)):
        return spark.read.parquet(path)
    return None


# ───────────────────────── partition writes ─────────────────────────────────

def write_partitions(
    spark: SparkSession, df: DataFrame, path: str, part_col: str, replace: list[str] | None = None,
) -> list[str]:
    """Replace partitions of the parquet table at *path* with the rows of *df*.

    * *df* may read from *path* itself: it is first materialised in a sibling
      folder, and only then are partition directories swapped (rename).
    * *replace*: partitions to drop before installing the new data (defaults to
      the partitions present in *df*).  A partition listed here that has no rows
      in *df* ends up empty, i.e. stale data never survives.
    * Partitions not touched are never rewritten.
    Returns the sorted list of partitions that were installed.
    """
    path = path.rstrip("/")
    tmp = path + "__tmp"
    delete_path(spark, tmp)
    df.write.mode("overwrite").partitionBy(part_col).parquet(tmp)
    new_parts = list_partitions(spark, tmp, part_col)
    drop = sorted(set(replace or []) | set(new_parts))
    for v in drop:
        delete_path(spark, f"{path}/{part_col}={v}")
    if new_parts:
        _, fs = _fs(spark, path)
        fs.mkdirs(spark._jvm.org.apache.hadoop.fs.Path(path))
        for v in new_parts:
            src = spark._jvm.org.apache.hadoop.fs.Path(f"{tmp}/{part_col}={v}")
            dst = spark._jvm.org.apache.hadoop.fs.Path(f"{path}/{part_col}={v}")
            if not fs.rename(src, dst):
                raise RuntimeError(f"cannot move {src} to {dst}")
    delete_path(spark, tmp)
    return drop


def write_day_partition(spark: SparkSession, df: DataFrame, path: str, part_col: str, value: str) -> None:
    """Idempotent write of one partition (a re-run that yields no rows clears it)."""
    write_partitions(spark, df, path, part_col, replace=[value])


def merge_partitions(
    spark: SparkSession, path: str, new_df: DataFrame, key: str, touched_keys: DataFrame, part_col: str = "dt",
) -> list[str]:
    """Upsert *new_df* into the table at *path*; only affected partitions are rewritten.

    * *touched_keys* (one column named *key*): all keys whose old row must be replaced.
      Keys of *new_df* must be a subset of it; a touched key with no row in *new_df*
      is deleted from the table.
    * A key can move between partitions: old and new partitions are both rewritten.
    * Deterministic and idempotent: same inputs, same table.
    Returns the sorted list of rewritten partitions.
    """
    existing = read_table(spark, path)
    new_df = new_df.persist()
    try:
        new_parts = {r[0] for r in new_df.select(part_col).distinct().collect()}   # a handful of dates
        if existing is None:
            return write_partitions(spark, new_df, path, part_col)
        tk = F.broadcast(touched_keys.distinct())
        old_parts = {r[0] for r in existing.join(tk, key).select(part_col).distinct().collect()}
        affected = sorted(new_parts | old_parts)
        keep = existing.filter(F.col(part_col).isin(affected)).join(tk, key, "left_anti")
        out = keep.unionByName(new_df.select(*keep.columns))
        return write_partitions(spark, out, path, part_col, replace=affected)
    finally:
        new_df.unpersist()


# ───────────────────────── point-in-time snapshot calendar ──────────────────

def snapshot_calendar(spark: SparkSession, snapshot_dts: list[str], upto: str) -> DataFrame:
    """(cal_dt, snap_dt): for every calendar day from the first snapshot to *upto*, the newest
    snapshot taken on or before that day.  Days before the first snapshot are absent."""
    snaps = sorted(snapshot_dts)
    if not snaps:
        return spark.createDataFrame([], "cal_dt string, snap_dt string")
    cur, end, i, rows = _d.date.fromisoformat(snaps[0]), _d.date.fromisoformat(upto), 0, []
    while cur <= end:
        while i + 1 < len(snaps) and snaps[i + 1] <= cur.isoformat():
            i += 1
        rows.append((cur.isoformat(), snaps[i]))
        cur += _d.timedelta(days=1)
    return spark.createDataFrame(rows, "cal_dt string, snap_dt string")


# ───────────────────────── fid decoding ─────────────────────────────────────

_FID_RE = r"^(.+):(\d+)$"


def fid_raw(fid: Column) -> Column:
    """base64url -> text ('request_id:position'); undecodable input gives a string that fails the regex."""
    return F.unbase64(F.translate(fid, "-_", "+/")).cast("string")


def fid_request_id(fid: Column) -> Column:
    r = F.regexp_extract(fid_raw(fid), _FID_RE, 1)
    return F.when(r == "", F.lit(None)).otherwise(r)


def fid_position(fid: Column) -> Column:
    p = F.regexp_extract(fid_raw(fid), _FID_RE, 2)
    return F.when(p == "", F.lit(None)).otherwise(p.cast("int"))


def encode_fid(request_id: str, position: int) -> str:
    """Python inverse of the decoder (used by tests)."""
    return base64.urlsafe_b64encode(f"{request_id}:{position}".encode()).decode().rstrip("=")
