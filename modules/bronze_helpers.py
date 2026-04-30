# =============================================================================
# F1-Pulse | Bronze Layer — I/O Helpers
# Module:   modules/bronze_helpers.py
# Author:   Jafar891
# Updated:  2026
#
# Reusable utility for writing raw ingested data to Bronze Delta tables.
# Mirrors the interface of silver_helpers.py and gold_helpers.py.
# =============================================================================

import logging

from pyspark.sql import DataFrame

log = logging.getLogger("f1_pulse.bronze")


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def write_bronze(
    df: DataFrame,
    catalog: str,
    bronze_schema: str,
    table_name: str,
) -> None:
    """
    Persist a raw DataFrame to a Bronze Delta table (idempotent overwrite).

    Args:
        df:            Raw Spark DataFrame from pdf_to_spark.
        catalog:       Unity Catalog name   (e.g. "f1_catalog").
        bronze_schema: Bronze schema name   (e.g. "bronze").
        table_name:    Target table name    (e.g. "raw_laps_2025").

    Raises:
        RuntimeError: If the write fails.
    """
    full_table = f"{catalog}.{bronze_schema}.{table_name}"
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
        raise RuntimeError(f"Cannot write Bronze table '{full_table}'") from e