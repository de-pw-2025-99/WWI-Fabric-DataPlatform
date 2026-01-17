# Databricks notebook source
# MAGIC %md
# MAGIC # nb_silver_utilities
# MAGIC 
# MAGIC Common transformation functions for Silver layer processing.
# MAGIC 
# MAGIC ## Usage
# MAGIC ```python
# MAGIC %run ./nb_silver_utilities
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and Configuration

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, trim, upper, lower, initcap, coalesce,
    when, isnull, isnan, length, regexp_replace,
    current_timestamp, sha2, concat_ws, to_timestamp,
    year, month, dayofmonth, hour, minute, second,
    from_json, explode, array, struct, monotonically_increasing_id
)
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Constants

# COMMAND ----------

# Bronze lakehouse reference (3-part naming)
BRONZE_LAKEHOUSE = "lh_bronze"

# Silver metadata columns
SILVER_METADATA_COLS = [
    "_silver_loaded_at",
    "_silver_pipeline_run_id", 
    "_silver_source_table",
    "_silver_row_hash"
]

# SCD2 specific columns
SCD2_COLS = [
    "_silver_is_current",
    "_silver_valid_from",
    "_silver_valid_to"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading Functions

# COMMAND ----------

def read_bronze_table(table_name: str, spark: SparkSession = None) -> DataFrame:
    """
    Read a table from Bronze lakehouse using 3-part naming.
    
    Args:
        table_name: Name of the Bronze table (e.g., 'wwi_sales_orders')
        spark: SparkSession (uses global if not provided)
    
    Returns:
        DataFrame with Bronze data
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    
    full_table_name = f"{BRONZE_LAKEHOUSE}.{table_name}"
    return spark.read.table(full_table_name)


def read_bronze_incremental(
    table_name: str,
    watermark_column: str,
    last_watermark: str,
    current_watermark: str = None,
    spark: SparkSession = None
) -> DataFrame:
    """
    Read incremental data from Bronze based on watermark.
    
    Args:
        table_name: Name of the Bronze table
        watermark_column: Column to filter on (typically _bronze_loaded_at)
        last_watermark: Last successful watermark value
        current_watermark: Current cutoff (defaults to now)
        spark: SparkSession
    
    Returns:
        DataFrame with incremental records
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    
    if current_watermark is None:
        current_watermark = datetime.utcnow().isoformat()
    
    df = read_bronze_table(table_name, spark)
    
    return df.filter(
        (col(watermark_column) >= lit(last_watermark)) &
        (col(watermark_column) < lit(current_watermark))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## String Cleaning Functions

# COMMAND ----------

def clean_string_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Clean a string column: trim whitespace, handle empty strings as null.
    """
    return df.withColumn(
        column_name,
        when(
            trim(col(column_name)) == "", None
        ).otherwise(trim(col(column_name)))
    )


def clean_all_string_columns(df: DataFrame) -> DataFrame:
    """
    Clean all string columns in a DataFrame.
    """
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    
    for col_name in string_cols:
        df = clean_string_column(df, col_name)
    
    return df


def standardize_name_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Standardize name columns: trim, proper case.
    """
    return df.withColumn(
        column_name,
        initcap(trim(col(column_name)))
    )


def standardize_code_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Standardize code columns: trim, uppercase.
    """
    return df.withColumn(
        column_name,
        upper(trim(col(column_name)))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null Handling Functions

# COMMAND ----------

def replace_null_string(df: DataFrame, column_name: str, default_value: str = "Unknown") -> DataFrame:
    """
    Replace null string values with a default.
    """
    return df.withColumn(
        column_name,
        coalesce(col(column_name), lit(default_value))
    )


def replace_null_numeric(df: DataFrame, column_name: str, default_value: float = 0.0) -> DataFrame:
    """
    Replace null numeric values with a default.
    """
    return df.withColumn(
        column_name,
        coalesce(col(column_name), lit(default_value))
    )


def replace_null_date(df: DataFrame, column_name: str, default_value: str = "9999-12-31") -> DataFrame:
    """
    Replace null date values with a default (typically end of time for SCD).
    """
    return df.withColumn(
        column_name,
        coalesce(col(column_name), lit(default_value).cast("date"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication Functions

# COMMAND ----------

def deduplicate_by_key(
    df: DataFrame,
    key_columns: List[str],
    order_column: str = "_bronze_loaded_at",
    keep: str = "last"
) -> DataFrame:
    """
    Deduplicate DataFrame keeping first or last record per key.
    
    Args:
        df: Input DataFrame
        key_columns: List of columns forming the unique key
        order_column: Column to order by for determining first/last
        keep: 'first' or 'last'
    
    Returns:
        Deduplicated DataFrame
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc, asc
    
    order_func = desc if keep == "last" else asc
    
    window = Window.partitionBy(key_columns).orderBy(order_func(order_column))
    
    return df.withColumn("_rank", row_number().over(window)) \
             .filter(col("_rank") == 1) \
             .drop("_rank")


def get_duplicate_stats(df: DataFrame, key_columns: List[str]) -> Dict:
    """
    Get statistics about duplicates in the DataFrame.
    
    Returns:
        Dict with total_records, unique_records, duplicate_records, duplicate_rate
    """
    total = df.count()
    unique = df.dropDuplicates(key_columns).count()
    duplicates = total - unique
    
    return {
        "total_records": total,
        "unique_records": unique,
        "duplicate_records": duplicates,
        "duplicate_rate": round(duplicates / total * 100, 2) if total > 0 else 0
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hash and Row Versioning Functions

# COMMAND ----------

def add_row_hash(
    df: DataFrame,
    columns_to_hash: List[str],
    hash_column_name: str = "_silver_row_hash"
) -> DataFrame:
    """
    Add SHA256 hash of specified columns for change detection.
    
    Args:
        df: Input DataFrame
        columns_to_hash: Columns to include in hash (business columns)
        hash_column_name: Name for the hash column
    
    Returns:
        DataFrame with hash column added
    """
    # Convert all columns to string and concatenate
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
    """
    Add standard Silver metadata columns.
    
    Args:
        df: Input DataFrame
        source_table: Bronze source table name
        pipeline_run_id: Pipeline run ID (if available)
        columns_to_hash: Columns for row hash (uses all non-system columns if None)
    
    Returns:
        DataFrame with Silver metadata columns
    """
    # Determine columns to hash (exclude bronze metadata)
    if columns_to_hash is None:
        exclude_cols = ["_bronze_loaded_at", "_bronze_pipeline_run_id"] + SILVER_METADATA_COLS
        columns_to_hash = [c for c in df.columns if c not in exclude_cols]
    
    df = df.withColumn("_silver_loaded_at", current_timestamp())
    df = df.withColumn("_silver_pipeline_run_id", lit(pipeline_run_id))
    df = df.withColumn("_silver_source_table", lit(source_table))
    df = add_row_hash(df, columns_to_hash)
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Functions

# COMMAND ----------

def prepare_scd2_columns(df: DataFrame, valid_from_value: datetime = None) -> DataFrame:
    """
    Add SCD Type 2 tracking columns for new records.
    """
    if valid_from_value is None:
        valid_from_value = datetime.utcnow()
    
    return df.withColumn("_silver_is_current", lit(True)) \
             .withColumn("_silver_valid_from", lit(valid_from_value)) \
             .withColumn("_silver_valid_to", lit(datetime(9999, 12, 31)))


def apply_scd2_merge(
    target_table: str,
    source_df: DataFrame,
    key_columns: List[str],
    spark: SparkSession = None
) -> Dict:
    """
    Apply SCD Type 2 merge logic.
    
    Args:
        target_table: Name of the Silver table
        source_df: New/updated records from Bronze
        key_columns: Business key columns
        spark: SparkSession
    
    Returns:
        Dict with merge statistics
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    
    # Check if target exists
    if not DeltaTable.isDeltaTable(spark, f"spark_catalog.default.{target_table}"):
        # First load - just write with SCD2 columns
        source_df = prepare_scd2_columns(source_df)
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return {"inserted": source_df.count(), "updated": 0, "unchanged": 0}
    
    target = DeltaTable.forName(spark, target_table)
    
    # Build merge condition
    merge_condition = " AND ".join([f"target.{c} = source.{c}" for c in key_columns])
    merge_condition += " AND target._silver_is_current = true"
    
    # Detect changes using row hash
    update_condition = "target._silver_row_hash != source._silver_row_hash"
    
    # Prepare source with SCD2 columns
    source_prepared = prepare_scd2_columns(source_df)
    
    # Perform merge
    target.alias("target").merge(
        source_prepared.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        condition=update_condition,
        set={
            "_silver_is_current": lit(False),
            "_silver_valid_to": current_timestamp()
        }
    ).whenNotMatchedInsertAll().execute()
    
    # Insert new versions for updated records
    # (This is simplified - production would need more sophisticated logic)
    
    return {"status": "completed"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Functions

# COMMAND ----------

def check_not_null(df: DataFrame, column_name: str) -> Tuple[int, int]:
    """
    Check for null values in a column.
    
    Returns:
        Tuple of (total_records, null_records)
    """
    total = df.count()
    nulls = df.filter(col(column_name).isNull()).count()
    return (total, nulls)


def check_unique(df: DataFrame, key_columns: List[str]) -> Tuple[int, int]:
    """
    Check for duplicate records.
    
    Returns:
        Tuple of (total_records, duplicate_records)
    """
    total = df.count()
    unique = df.dropDuplicates(key_columns).count()
    return (total, total - unique)


def check_referential_integrity(
    df: DataFrame,
    fk_column: str,
    reference_df: DataFrame,
    pk_column: str
) -> Tuple[int, int]:
    """
    Check referential integrity between tables.
    
    Returns:
        Tuple of (total_records, orphan_records)
    """
    total = df.count()
    
    # Left anti join to find orphans
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
    """
    Run a quality check and return results.
    """
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Functions

# COMMAND ----------

def write_silver_table(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    partition_by: List[str] = None
) -> int:
    """
    Write DataFrame to Silver table with standard options.
    
    Args:
        df: DataFrame to write
        table_name: Target table name
        mode: Write mode ('overwrite', 'append', 'merge')
        partition_by: Optional partition columns
    
    Returns:
        Number of records written
    """
    writer = df.write.format("delta").mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(partition_by)
    
    writer.saveAsTable(table_name)
    
    return df.count()


def merge_silver_table(
    source_df: DataFrame,
    target_table: str,
    key_columns: List[str],
    spark: SparkSession = None
) -> Dict:
    """
    Merge (upsert) data into Silver table.
    
    Args:
        source_df: New/updated records
        target_table: Target table name
        key_columns: Columns forming unique key
        spark: SparkSession
    
    Returns:
        Dict with merge statistics
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    
    # Check if target exists
    if not DeltaTable.isDeltaTable(spark, f"spark_catalog.default.{target_table}"):
        # First load
        count = write_silver_table(source_df, target_table, "overwrite")
        return {"inserted": count, "updated": 0}
    
    target = DeltaTable.forName(spark, target_table)
    
    # Build merge condition
    merge_condition = " AND ".join([f"target.{c} = source.{c}" for c in key_columns])
    
    # Perform merge
    merge_result = target.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    return {"status": "completed", "merge_result": str(merge_result)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load History Functions

# COMMAND ----------

def start_load_history(
    config_id: int,
    bronze_table: str,
    silver_table: str,
    pipeline_run_id: str = None,
    notebook_run_id: str = None,
    watermark_start: str = None,
    watermark_end: str = None,
    spark: SparkSession = None
) -> int:
    """
    Create a load history record at the start of processing.
    
    Returns:
        load_id for the new record
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    
    from pyspark.sql.functions import max as spark_max
    
    # Get next load_id
    max_id = spark.table("_silver_load_history") \
                  .select(spark_max("load_id")) \
                  .collect()[0][0]
    load_id = (max_id or 0) + 1
    
    # Insert record
    record = [(
        load_id, config_id, bronze_table, silver_table,
        pipeline_run_id, notebook_run_id,
        datetime.utcnow(), None, None,
        None, None, None,
        watermark_start, watermark_end,
        "Running", None
    )]
    
    schema = spark.table("_silver_load_history").schema
    spark.createDataFrame(record, schema) \
         .write.format("delta") \
         .mode("append") \
         .saveAsTable("_silver_load_history")
    
    return load_id


def complete_load_history(
    load_id: int,
    status: str,
    records_read: int = None,
    records_written: int = None,
    records_rejected: int = None,
    error_message: str = None,
    spark: SparkSession = None
):
    """
    Update load history record on completion.
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    
    end_time = datetime.utcnow()
    
    # Get start time to calculate duration
    start_time = spark.table("_silver_load_history") \
                      .filter(col("load_id") == load_id) \
                      .select("start_time") \
                      .collect()[0][0]
    
    duration = int((end_time - start_time).total_seconds()) if start_time else None
    
    # Update using Delta merge
    target = DeltaTable.forName(spark, "_silver_load_history")
    
    target.update(
        condition=f"load_id = {load_id}",
        set={
            "end_time": lit(end_time),
            "duration_seconds": lit(duration),
            "records_read": lit(records_read),
            "records_written": lit(records_written),
            "records_rejected": lit(records_rejected),
            "status": lit(status),
            "error_message": lit(error_message)
        }
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def get_config_by_table(bronze_table: str, spark: SparkSession = None) -> Dict:
    """
    Get configuration for a Bronze table from _silver_load_config.
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    
    row = spark.table("_silver_load_config") \
               .filter(col("bronze_table") == bronze_table) \
               .first()
    
    if row:
        return row.asDict()
    return None


def get_configs_by_category(category: str, spark: SparkSession = None) -> List[Dict]:
    """
    Get all configurations for a category.
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    
    rows = spark.table("_silver_load_config") \
                .filter((col("category") == category) & (col("is_active") == True)) \
                .orderBy("load_priority", "config_id") \
                .collect()
    
    return [row.asDict() for row in rows]


def update_watermark(config_id: int, new_watermark: str, spark: SparkSession = None):
    """
    Update the last_watermark for a configuration.
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    
    target = DeltaTable.forName(spark, "_silver_load_config")
    
    target.update(
        condition=f"config_id = {config_id}",
        set={
            "last_watermark": lit(new_watermark),
            "updated_at": current_timestamp()
        }
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print Summary

# COMMAND ----------

print("=" * 60)
print("Silver Layer Utilities Loaded Successfully")
print("=" * 60)
print("\nAvailable Functions:")
print("-" * 60)
print("Data Reading:")
print("  - read_bronze_table(table_name)")
print("  - read_bronze_incremental(table_name, watermark_column, last_watermark)")
print("\nString Cleaning:")
print("  - clean_string_column(df, column_name)")
print("  - clean_all_string_columns(df)")
print("  - standardize_name_column(df, column_name)")
print("  - standardize_code_column(df, column_name)")
print("\nNull Handling:")
print("  - replace_null_string(df, column_name, default_value)")
print("  - replace_null_numeric(df, column_name, default_value)")
print("  - replace_null_date(df, column_name, default_value)")
print("\nDeduplication:")
print("  - deduplicate_by_key(df, key_columns, order_column, keep)")
print("  - get_duplicate_stats(df, key_columns)")
print("\nMetadata & Hashing:")
print("  - add_row_hash(df, columns_to_hash)")
print("  - add_silver_metadata(df, source_table, pipeline_run_id)")
print("\nSCD Type 2:")
print("  - prepare_scd2_columns(df)")
print("  - apply_scd2_merge(target_table, source_df, key_columns)")
print("\nData Quality:")
print("  - check_not_null(df, column_name)")
print("  - check_unique(df, key_columns)")
print("  - check_referential_integrity(df, fk_column, reference_df, pk_column)")
print("  - run_quality_check(df, check_name, check_dimension, check_func)")
print("\nWrite Operations:")
print("  - write_silver_table(df, table_name, mode, partition_by)")
print("  - merge_silver_table(source_df, target_table, key_columns)")
print("\nLoad History:")
print("  - start_load_history(...)")
print("  - complete_load_history(load_id, status, ...)")
print("\nConfiguration:")
print("  - get_config_by_table(bronze_table)")
print("  - get_configs_by_category(category)")
print("  - update_watermark(config_id, new_watermark)")
print("=" * 60)
