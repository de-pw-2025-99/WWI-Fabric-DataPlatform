# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "116386f7-0d6f-4d1e-9639-fb2ce3bd5ce7",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "4ff4e458-6d35-4bf0-944e-bb512481f096",
# META       "known_lakehouses": [
# META         {
# META           "id": "116386f7-0d6f-4d1e-9639-fb2ce3bd5ce7"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Cell 1: Imports and Configuration
# This is the foundational notebook - all other Silver notebooks depend on it.


# CELL ********************

#Cell 1: Imports and Configuration
# Imports
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, trim, upper, lower, initcap, coalesce,
    when, isnull, isnan, length, regexp_replace,
    current_timestamp, sha2, concat_ws, to_timestamp,
    year, month, dayofmonth, hour, minute, second,
    from_json, explode, array, struct, monotonically_increasing_id,
    current_date, datediff, floor, round, abs, avg, desc, asc,
    count, sum, max, min, row_number, to_date, dayofweek
)
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import json

# Configuration
BRONZE_LAKEHOUSE = "lh_bronze"

print("✅ Imports loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 2: Data Reading Functions

# CELL ********************

# Cell 2: Data Reading Functions
def read_bronze_table(table_name: str) -> DataFrame:
    """Read a table from Bronze lakehouse using 3-part naming."""
    full_table_name = f"{BRONZE_LAKEHOUSE}.dbo.{table_name}"
    return spark.read.table(full_table_name)


def read_bronze_incremental(
    table_name: str,
    watermark_column: str,
    last_watermark: str,
    current_watermark: str = None
) -> DataFrame:
    """Read incremental data from Bronze based on watermark."""
    if current_watermark is None:
        current_watermark = datetime.utcnow().isoformat()
    
    df = read_bronze_table(table_name)
    return df.filter(
        (col(watermark_column) >= lit(last_watermark)) &
        (col(watermark_column) < lit(current_watermark))
    )

print("✅ Data reading functions loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 3: String Cleaning Functions

# CELL ********************

def clean_string_column(df: DataFrame, column_name: str) -> DataFrame:
    """Clean a string column: trim whitespace, handle empty strings as null."""
    return df.withColumn(
        column_name,
        when(trim(col(column_name)) == "", None).otherwise(trim(col(column_name)))
    )


def clean_all_string_columns(df: DataFrame) -> DataFrame:
    """Clean all string columns in a DataFrame."""
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col_name in string_cols:
        df = clean_string_column(df, col_name)
    return df


def standardize_name_column(df: DataFrame, column_name: str) -> DataFrame:
    """Standardize name columns: trim, proper case."""
    return df.withColumn(column_name, initcap(trim(col(column_name))))


def standardize_code_column(df: DataFrame, column_name: str) -> DataFrame:
    """Standardize code columns: trim, uppercase."""
    return df.withColumn(column_name, upper(trim(col(column_name))))

print("✅ String cleaning functions loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 4: Deduplication Functions

# CELL ********************

def deduplicate_by_key(
    df: DataFrame,
    key_columns: List[str],
    order_column: str = "_bronze_loaded_at",
    keep: str = "last"
) -> DataFrame:
    """Deduplicate DataFrame keeping first or last record per key."""
    order_func = desc if keep == "last" else asc
    window = Window.partitionBy(key_columns).orderBy(order_func(order_column))
    return df.withColumn("_rank", row_number().over(window)) \
             .filter(col("_rank") == 1) \
             .drop("_rank")


def get_duplicate_stats(df: DataFrame, key_columns: List[str]) -> Dict:
    """Get statistics about duplicates in the DataFrame."""
    total = df.count()
    unique = df.dropDuplicates(key_columns).count()
    duplicates = total - unique
    return {
        "total_records": total,
        "unique_records": unique,
        "duplicate_records": duplicates,
        "duplicate_rate": round(duplicates / total * 100, 2) if total > 0 else 0
    }

print("✅ Deduplication functions loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 5: Metadata and Hash Functions

# CELL ********************

# Cell 5: Metadata and Hash Functions
def add_row_hash(
    df: DataFrame,
    columns_to_hash: List[str],
    hash_column_name: str = "_silver_row_hash"
) -> DataFrame:
    """Add SHA256 hash of specified columns for change detection."""
    hash_expr = sha2(
        concat_ws("|", *[coalesce(col(c).cast("string"), lit("NULL")) for c in columns_to_hash]),
        256
    )
    return df.withColumn(hash_column_name, hash_expr)


def add_silver_metadata(
    df: DataFrame,
    source_table: str,
    pipeline_run_id: str = None,
    columns_to_hash: List[str] = None
) -> DataFrame:
    """Add standard Silver metadata columns."""
    if columns_to_hash is None:
        exclude_cols = ["_bronze_loaded_at", "_bronze_pipeline_run_id", "_silver_loaded_at", 
                        "_silver_pipeline_run_id", "_silver_source_table", "_silver_row_hash"]
        columns_to_hash = [c for c in df.columns if c not in exclude_cols]
    
    df = df.withColumn("_silver_loaded_at", current_timestamp())
    df = df.withColumn("_silver_pipeline_run_id", lit(pipeline_run_id))
    df = df.withColumn("_silver_source_table", lit(source_table))
    df = add_row_hash(df, columns_to_hash)
    return df

print("✅ Metadata and hash functions loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 6: Data Quality Functions

# CELL ********************

# Cell 6: Data Quality Functions
def check_not_null(df: DataFrame, column_name: str) -> Tuple[int, int]:
    """Check for null values. Returns (total_records, null_records)."""
    total = df.count()
    nulls = df.filter(col(column_name).isNull()).count()
    return (total, nulls)


def check_unique(df: DataFrame, key_columns: List[str]) -> Tuple[int, int]:
    """Check for duplicates. Returns (total_records, duplicate_records)."""
    total = df.count()
    unique = df.dropDuplicates(key_columns).count()
    return (total, total - unique)


def check_referential_integrity(
    df: DataFrame,
    fk_column: str,
    reference_df: DataFrame,
    pk_column: str
) -> Tuple[int, int]:
    """Check referential integrity. Returns (total_records, orphan_records)."""
    total = df.count()
    orphans = df.join(
        reference_df.select(col(pk_column).alias("_ref_pk")),
        df[fk_column] == col("_ref_pk"),
        "left_anti"
    ).count()
    return (total, orphans)


def run_quality_check(
    df: DataFrame,
    check_name: str,
    check_dimension: str,
    check_func,
    threshold_warn: float = 0.99,
    threshold_fail: float = 0.95
) -> Dict:
    """Run a quality check and return results."""
    total, failed = check_func()
    passed = total - failed
    pass_rate = passed / total if total > 0 else 1.0
    
    if pass_rate >= threshold_warn:
        status = "PASS"
    elif pass_rate >= threshold_fail:
        status = "WARN"
    else:
        status = "FAIL"
    
    return {
        "check_name": check_name,
        "check_dimension": check_dimension,
        "records_checked": total,
        "records_passed": passed,
        "records_failed": failed,
        "pass_rate": round(pass_rate, 4),
        "threshold_warn": threshold_warn,
        "threshold_fail": threshold_fail,
        "status": status
    }

print("✅ Data quality functions loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 7: Write Functions

# CELL ********************

# Cell 7: Write Functions
def write_silver_table(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    partition_by: List[str] = None
) -> int:
    """Write DataFrame to Silver table with standard options."""
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.saveAsTable(table_name)
    return df.count()


def merge_silver_table(
    source_df: DataFrame,
    target_table: str,
    key_columns: List[str]
) -> Dict:
    """Merge (upsert) data into Silver table."""
    # Check if target exists
    if not DeltaTable.isDeltaTable(spark, f"spark_catalog.default.{target_table}"):
        count = write_silver_table(source_df, target_table, "overwrite")
        return {"inserted": count, "updated": 0}
    
    target = DeltaTable.forName(spark, target_table)
    merge_condition = " AND ".join([f"target.{c} = source.{c}" for c in key_columns])
    
    target.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    return {"status": "completed"}

print("✅ Write functions loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 8: Configuration Helper Functions

# CELL ********************

# Cell 8: Configuration Helper Functions
def get_config_by_table(bronze_table: str) -> Dict:
    """Get configuration for a Bronze table from _silver_load_config."""
    row = spark.table("_silver_load_config") \
               .filter(col("bronze_table") == bronze_table) \
               .first()
    if row:
        return row.asDict()
    return None


def get_configs_by_category(category: str) -> List[Dict]:
    """Get all configurations for a category."""
    rows = spark.table("_silver_load_config") \
                .filter((col("category") == category) & (col("is_active") == True)) \
                .orderBy("load_priority", "config_id") \
                .collect()
    return [row.asDict() for row in rows]


def update_watermark(config_id: int, new_watermark: str):
    """Update the last_watermark for a configuration."""
    target = DeltaTable.forName(spark, "_silver_load_config")
    target.update(
        condition=f"config_id = {config_id}",
        set={
            "last_watermark": lit(new_watermark),
            "updated_at": current_timestamp()
        }
    )

print("✅ Configuration helper functions loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 9: Summary

# CELL ********************

# Cell 9: Summary
print("=" * 60)
print("SILVER LAYER UTILITIES LOADED SUCCESSFULLY")
print("=" * 60)
print("""
Available Functions:
--------------------
Data Reading:
  - read_bronze_table(table_name)
  - read_bronze_incremental(table_name, watermark_column, last_watermark)

String Cleaning:
  - clean_string_column(df, column_name)
  - clean_all_string_columns(df)
  - standardize_name_column(df, column_name)
  - standardize_code_column(df, column_name)

Deduplication:
  - deduplicate_by_key(df, key_columns, order_column, keep)
  - get_duplicate_stats(df, key_columns)

Metadata:
  - add_row_hash(df, columns_to_hash)
  - add_silver_metadata(df, source_table, pipeline_run_id)

Data Quality:
  - check_not_null(df, column_name)
  - check_unique(df, key_columns)
  - check_referential_integrity(df, fk_column, reference_df, pk_column)
  - run_quality_check(df, check_name, check_dimension, check_func)

Write Operations:
  - write_silver_table(df, table_name, mode, partition_by)
  - merge_silver_table(source_df, target_table, key_columns)

Configuration:
  - get_config_by_table(bronze_table)
  - get_configs_by_category(category)
  - update_watermark(config_id, new_watermark)
""")
print("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
