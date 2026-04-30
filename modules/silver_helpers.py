# =============================================================================
# F1-Pulse | Silver Layer — I/O & Quality Helpers
# Module:   modules/silver_helpers.py
# Author:   Jafar891
# Updated:  2026
#
# Reusable utilities for reading Bronze tables, writing Silver tables,
# schema validation, and data-quality reporting.
# =============================================================================

import logging

from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger("f1_pulse.silver")


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def read_bronze(
    spark: SparkSession,
    catalog: str,
    bronze_schema: str,
    table_name: str,
) -> DataFrame:
    """
    Read a Bronze Delta table with existence and emptiness checks.

    Args:
        spark:         Active SparkSession.
        catalog:       Unity Catalog name  (e.g. "f1_project").
        bronze_schema: Bronze schema name  (e.g. "bronze").
        table_name:    Table name          (e.g. "raw_laps_2025").

    Returns:
        DataFrame — never None; raises on failure.

    Raises:
        RuntimeError: If the table cannot be read.
    """
    full_table = f"{catalog}.{bronze_schema}.{table_name}"
    try:
        df = spark.read.table(full_table)
        count = df.count()
        log.info(f"  📥 Read {full_table}  ({count:,} rows)")
        if count == 0:
            log.warning(
                f"  ⚠️  Table {full_table} is empty — "
                "downstream results may be incomplete."
            )
        return df
    except Exception as e:
        log.error(f"  ❌ Failed to read {full_table}: {e}")
        raise RuntimeError(f"Cannot read Bronze table '{full_table}'") from e


def write_silver(
    df: DataFrame,
    catalog: str,
    silver_schema: str,
    table_name: str,
) -> None:
    """
    Persist a DataFrame to a Silver Delta table (idempotent overwrite).

    Args:
        df:            Transformed Spark DataFrame.
        catalog:       Unity Catalog name  (e.g. "f1_project").
        silver_schema: Silver schema name  (e.g. "silver").
        table_name:    Target table name   (e.g. "enriched_laps").

    Raises:
        RuntimeError: If the write fails.
    """
    full_table = f"{catalog}.{silver_schema}.{table_name}"
    row_count = df.count()
    try:
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(full_table)
        )
        log.info(f"  ✅ Written → {full_table}  ({row_count:,} rows)")
    except Exception as e:
        log.error(f"  ❌ Failed to write {full_table}: {e}")
        raise RuntimeError(f"Cannot write Silver table '{full_table}'") from e


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

def assert_columns(
    df: DataFrame,
    required: list[str],
    table_label: str,
) -> None:
    """
    Raise ValueError early if expected columns are absent.
    Catches schema drift before transformations run.

    Args:
        df:          DataFrame to validate.
        required:    List of column names that must be present.
        table_label: Human-readable label used in the error message.

    Raises:
        ValueError: Lists every missing column in one shot.
    """
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Schema drift detected in '{table_label}'. "
            f"Missing columns: {missing}. "
            f"Available: {df.columns}"
        )


# ---------------------------------------------------------------------------
# Data-quality reporting
# ---------------------------------------------------------------------------

def log_quality_check(label: str, total: int, kept: int) -> None:
    """
    Emit an INFO log summarising row-level filtering.

    Args:
        label: Human-readable dataset label (e.g. "Laps (valid only)").
        total: Row count before filtering.
        kept:  Row count after filtering.
    """
    dropped = total - kept
    pct = (dropped / total * 100) if total > 0 else 0.0
    log.info(
        f"  📊 {label}: {total:,} → {kept:,} rows kept  "
        f"({dropped:,} dropped, {pct:.1f}%)"
    )