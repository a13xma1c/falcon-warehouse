"""SparkSession factory.  One session per process."""
from __future__ import annotations
from pyspark.sql import SparkSession


def get_spark(cfg: dict) -> SparkSession:
    sc = cfg.get("spark", {})
    builder = (
        SparkSession.builder
        .appName(sc.get("app_name", "falcon-warehouse"))
        .master(sc.get("master", "local[*]"))
        .config("spark.driver.memory",          sc.get("driver_memory", "4g"))
        .config("spark.sql.shuffle.partitions", sc.get("shuffle_partitions", 8))
        .config("spark.sql.session.timeZone",   "UTC")
        .config("spark.sql.caseSensitive",       "true")
        # write only the new partition, not the whole table
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # keep dt=YYYY-MM-DD partition values as strings (ISO strings sort correctly)
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.ui.enabled",             "false")
        # the driver keeps job/stage/SQL status in memory even without the UI: bound it for long runs
        .config("spark.ui.retainedJobs",          50)
        .config("spark.ui.retainedStages",        50)
        .config("spark.ui.retainedTasks",         500)
        .config("spark.sql.ui.retainedExecutions", 50)
    )
    for k, v in sc.get("extra_conf", {}).items():
        builder = builder.config(k, v)
    return builder.getOrCreate()
