"""I/O helpers for the warehouse.

Convention:
- Staging:   outputs/warehouse/staging/<table>/dt=YYYY-MM-DD/
- Dimensions:outputs/warehouse/dims/<table>/dt=YYYY-MM-DD/
- Facts:     outputs/warehouse/facts/<table>/dt=YYYY-MM-DD/
- Mart:      outputs/warehouse/mart/<table>/dt=YYYY-MM-DD/
- DQ:        outputs/warehouse/dq/<table>/dt=YYYY-MM-DD/

Every layer is partitioned by `dt` and uses dynamic partition overwrite so
re-running a date replaces only that date's partition.
"""
from __future__ import annotations
from pathlib import Path
from pyspark.sql import DataFrame


def table_path(warehouse: str, layer: str, table: str) -> str:
    return str(Path(warehouse) / layer / table)


def write_layer(
    df: DataFrame,
    warehouse: str,
    layer: str,
    table: str,
    partition_cols: list[str] | None = None,
    mode: str = "overwrite",
    fmt: str = "parquet",
) -> None:
    """Write *df* to ``<warehouse>/<layer>/<table>``, partitioned by *partition_cols*."""
    path = table_path(warehouse, layer, table)
    pcols = partition_cols if partition_cols is not None else ["dt"]
    (
        df.write
        .format(fmt)
        .partitionBy(*pcols)
        .mode(mode)
        .save(path)
    )


def read_layer(
    spark,
    warehouse: str,
    layer: str,
    table: str,
    fmt: str = "parquet",
) -> DataFrame:
    return spark.read.format(fmt).load(table_path(warehouse, layer, table))


def read_layer_dt(
    spark,
    warehouse: str,
    layer: str,
    table: str,
    dt: str,
    fmt: str = "parquet",
) -> DataFrame:
    path = f"{table_path(warehouse, layer, table)}/dt={dt}"
    return spark.read.format(fmt).load(path)
