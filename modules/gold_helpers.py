# =============================================================================
# F1-Pulse | Gold Layer — I/O Helpers
# Module:   modules/gold_helpers.py
# Author:   Jafar891
# Updated:  2026
#
# Reusable utilities for reading Silver tables and writing Gold tables.
# Mirrors the interface of silver_helpers.py for consistency.
# =============================================================================

import logging

from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger("f1_pulse.gold")


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def read_silver(
    spark: SparkSession,
    catalog: str,
    silver_schema: str,
    table_name: str,
) -> DataFrame:
    """
    Read a Silver Delta table.

    Raises ValueError (not just a warning) if the table is empty, because
    Gold aggregations over zero rows produce meaningless results.

    Args:
        spark:         Active SparkSession.
        catalog:       Unity Catalog name   (e.g. "f1_pulse").
        silver_schema: Silver schema name   (e.g. "silver").
        table_name:    Table name           (e.g. "enriched_laps").

    Returns:
        Non-empty DataFrame.

    Raises:
        RuntimeError: If the table cannot be read.
        ValueError:   If the table exists but is empty.
    """
    full_table = f"{catalog}.{silver_schema}.{table_name}"
    try:
        df = spark.read.table(full_table)
        count_ = df.count()
        log.info(f"  📥 Read {full_table}  ({count_:,} rows)")
        if count_ == 0:
            raise ValueError(
                f"Silver table '{full_table}' is empty — "
                "cannot produce Gold metrics."
            )
        return df
    except ValueError:
        raise
    except Exception as e:
        log.error(f"  ❌ Failed to read {full_table}: {e}")
        raise RuntimeError(f"Cannot read Silver table '{full_table}'") from e


def write_gold(
    df: DataFrame,
    catalog: str,
    gold_schema: str,
    table_name: str,
) -> None:
    """
    Persist a DataFrame to a Gold Delta table (idempotent overwrite).

    Args:
        df:          Aggregated Spark DataFrame.
        catalog:     Unity Catalog name  (e.g. "f1_pulse").
        gold_schema: Gold schema name    (e.g. "gold").
        table_name:  Target table name   (e.g. "driver_performance_metrics").

    Raises:
        RuntimeError: If the write fails.
    """
    full_table = f"{catalog}.{gold_schema}.{table_name}"
    try:
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(full_table)
        )
        log.info(f"  ✅ Written → {full_table}  ({df.count():,} rows)")
    except Exception as e:
        log.error(f"  ❌ Failed to write {full_table}: {e}")
        raise RuntimeError(f"Cannot write Gold table '{full_table}'") from e


# ---------------------------------------------------------------------------
# Lap-validity summary (used in notebook for a quick diagnostic log)
# ---------------------------------------------------------------------------

def log_validity_summary(silver_laps: DataFrame) -> None:
    """
    Emit INFO counts of valid vs. flagged laps read from Silver.

    Args:
        silver_laps: The enriched_laps Silver DataFrame (must have is_valid_lap).
    """
    from pyspark.sql.functions import col

    valid   = silver_laps.filter(col("is_valid_lap") == True).count()   # noqa: E712
    flagged = silver_laps.filter(col("is_valid_lap") == False).count()  # noqa: E712
    log.info(f"  ✅ Valid laps                        : {valid:,}")
    log.info(f"  ⚠️  Flagged laps (excluded from Gold) : {flagged:,}")