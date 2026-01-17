# Databricks notebook source
# MAGIC %md
# MAGIC # nb_silver_iot_temperatures
# MAGIC 
# MAGIC Transforms IoT Temperature data from Bronze to Silver layer.
# MAGIC 
# MAGIC ## Source Tables
# MAGIC - **wwi_warehouse_vehicletemperatures** (~65,998 readings)
# MAGIC - **wwi_warehouse_coldroomtemperatures** (~3,651,193 readings)
# MAGIC 
# MAGIC ## Key Transformations
# MAGIC - Time-series standardization
# MAGIC - Outlier detection and flagging
# MAGIC - Temperature unit validation
# MAGIC - Aggregation preparation
# MAGIC 
# MAGIC ## Output Tables
# MAGIC - **slv_vehicle_temperature**
# MAGIC - **slv_cold_room_temperature**

# COMMAND ----------

# MAGIC %run ./nb_silver_utilities

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime

pipeline_run_id = dbutils.widgets.get("pipeline_run_id") if "pipeline_run_id" in [w.name for w in dbutils.widgets.getAll()] else None
is_full_load = dbutils.widgets.get("is_full_load") if "is_full_load" in [w.name for w in dbutils.widgets.getAll()] else "true"
is_full_load = is_full_load.lower() == "true"

results = []

# Temperature thresholds for quality checks
VEHICLE_TEMP_MIN = -30.0  # Celsius
VEHICLE_TEMP_MAX = 50.0   # Celsius
COLD_ROOM_TEMP_MIN = -25.0
COLD_ROOM_TEMP_MAX = 10.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_vehicle_temperature

# COMMAND ----------

def transform_vehicle_temperatures(is_full: bool = True):
    bronze_table = "wwi_warehouse_vehicletemperatures"
    silver_table = "slv_vehicle_temperature"
    
    config = get_config_by_table(bronze_table) if not is_full else None
    last_watermark = config.get("last_watermark", "1900-01-01T00:00:00") if config else "1900-01-01T00:00:00"
    current_watermark = datetime.utcnow().isoformat()
    
    if is_full:
        df = read_bronze_table(bronze_table)
    else:
        df = read_bronze_incremental(bronze_table, "_bronze_loaded_at", last_watermark, current_watermark)
    
    record_count = df.count()
    print(f"Records to process: {record_count:,}")
    
    if record_count == 0:
        return {"table": silver_table, "records": 0, "status": "no_data"}
    
    df = deduplicate_by_key(df, ["VehicleTemperatureID"], "_bronze_loaded_at", "last")
    
    df_transformed = df.select(
        col("VehicleTemperatureID").alias("vehicle_temperature_id"),
        col("VehicleRegistration").alias("vehicle_registration"),
        col("ChillerSensorNumber").cast("int").alias("chiller_sensor_number"),
        col("RecordedWhen").alias("recorded_when"),
        col("Temperature").cast("decimal(10,2)").alias("temperature"),
        col("FullSensorData").alias("full_sensor_data_json"),
        col("IsCompressed").alias("is_compressed"),
        col("CompressedSensorData").alias("compressed_sensor_data"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df_cleaned = clean_all_string_columns(df_transformed)
    
    # Add time-based columns
    df_enriched = df_cleaned \
        .withColumn("recorded_date", to_date(col("recorded_when"))) \
        .withColumn("recorded_year", year(col("recorded_when"))) \
        .withColumn("recorded_month", month(col("recorded_when"))) \
        .withColumn("recorded_hour", hour(col("recorded_when"))) \
        .withColumn("recorded_day_of_week", dayofweek(col("recorded_when")))
    
    # Add quality flags
    df_with_quality = df_enriched \
        .withColumn("is_temperature_valid",
                   (col("temperature") >= VEHICLE_TEMP_MIN) & 
                   (col("temperature") <= VEHICLE_TEMP_MAX)) \
        .withColumn("temperature_status",
                   when(col("temperature") < -5, "freezing")
                   .when(col("temperature") < 5, "chilled")
                   .when(col("temperature") < 15, "cool")
                   .when(col("temperature") < 25, "ambient")
                   .otherwise("warm")) \
        .withColumn("is_outlier",
                   (col("temperature") < VEHICLE_TEMP_MIN) | 
                   (col("temperature") > VEHICLE_TEMP_MAX))
    
    # Calculate rolling stats per vehicle/sensor (for anomaly detection)
    window_spec = Window.partitionBy("vehicle_registration", "chiller_sensor_number") \
                        .orderBy("recorded_when") \
                        .rowsBetween(-10, 0)
    
    df_with_stats = df_with_quality \
        .withColumn("rolling_avg_temp", round(avg("temperature").over(window_spec), 2)) \
        .withColumn("temp_deviation", round(col("temperature") - col("rolling_avg_temp"), 2))
    
    hash_columns = ["vehicle_temperature_id", "vehicle_registration", "chiller_sensor_number",
                    "recorded_when", "temperature"]
    df_final = add_silver_metadata(df_with_stats, bronze_table, pipeline_run_id, hash_columns)
    
    write_mode = "overwrite" if is_full else "append"
    if not is_full:
        records_written = merge_silver_table(df_final, silver_table, ["vehicle_temperature_id"])
    else:
        records_written = write_silver_table(df_final, silver_table, write_mode,
                                            partition_by=["recorded_year", "recorded_month"])
    
    # Log quality metrics
    invalid_count = df_final.filter(~col("is_temperature_valid")).count()
    if invalid_count > 0:
        print(f"⚠️ Found {invalid_count:,} readings outside valid range")
    
    if config and not is_full:
        update_watermark(config["config_id"], current_watermark)
    
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_vehicle_temperatures(is_full_load))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_cold_room_temperature

# COMMAND ----------

def transform_cold_room_temperatures(is_full: bool = True):
    bronze_table = "wwi_warehouse_coldroomtemperatures"
    silver_table = "slv_cold_room_temperature"
    
    config = get_config_by_table(bronze_table) if not is_full else None
    last_watermark = config.get("last_watermark", "1900-01-01T00:00:00") if config else "1900-01-01T00:00:00"
    current_watermark = datetime.utcnow().isoformat()
    
    if is_full:
        df = read_bronze_table(bronze_table)
        print("Full load - this table has ~3.6M records, may take a few minutes")
    else:
        df = read_bronze_incremental(bronze_table, "_bronze_loaded_at", last_watermark, current_watermark)
    
    record_count = df.count()
    print(f"Records to process: {record_count:,}")
    
    if record_count == 0:
        return {"table": silver_table, "records": 0, "status": "no_data"}
    
    df = deduplicate_by_key(df, ["ColdRoomTemperatureID"], "_bronze_loaded_at", "last")
    
    df_transformed = df.select(
        col("ColdRoomTemperatureID").alias("cold_room_temperature_id"),
        col("ColdRoomSensorNumber").cast("int").alias("cold_room_sensor_number"),
        col("RecordedWhen").alias("recorded_when"),
        col("Temperature").cast("decimal(10,2)").alias("temperature"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    # Add time-based columns
    df_enriched = df_transformed \
        .withColumn("recorded_date", to_date(col("recorded_when"))) \
        .withColumn("recorded_year", year(col("recorded_when"))) \
        .withColumn("recorded_month", month(col("recorded_when"))) \
        .withColumn("recorded_hour", hour(col("recorded_when"))) \
        .withColumn("recorded_day_of_week", dayofweek(col("recorded_when")))
    
    # Add quality flags
    df_with_quality = df_enriched \
        .withColumn("is_temperature_valid",
                   (col("temperature") >= COLD_ROOM_TEMP_MIN) & 
                   (col("temperature") <= COLD_ROOM_TEMP_MAX)) \
        .withColumn("temperature_status",
                   when(col("temperature") < -10, "deep_freeze")
                   .when(col("temperature") < 0, "frozen")
                   .when(col("temperature") < 5, "chilled")
                   .otherwise("too_warm")) \
        .withColumn("is_outlier",
                   (col("temperature") < COLD_ROOM_TEMP_MIN) | 
                   (col("temperature") > COLD_ROOM_TEMP_MAX)) \
        .withColumn("requires_attention",
                   col("temperature") > 5)  # Cold room getting too warm
    
    hash_columns = ["cold_room_temperature_id", "cold_room_sensor_number", 
                    "recorded_when", "temperature"]
    df_final = add_silver_metadata(df_with_quality, bronze_table, pipeline_run_id, hash_columns)
    
    write_mode = "overwrite" if is_full else "append"
    if not is_full:
        records_written = merge_silver_table(df_final, silver_table, ["cold_room_temperature_id"])
    else:
        records_written = write_silver_table(df_final, silver_table, write_mode,
                                            partition_by=["recorded_year", "recorded_month"])
    
    # Log quality metrics
    invalid_count = df_final.filter(~col("is_temperature_valid")).count()
    attention_count = df_final.filter(col("requires_attention")).count()
    
    if invalid_count > 0:
        print(f"⚠️ Found {invalid_count:,} readings outside valid range")
    if attention_count > 0:
        print(f"🚨 Found {attention_count:,} readings requiring attention (temp > 5°C)")
    
    if config and not is_full:
        update_watermark(config["config_id"], current_watermark)
    
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_cold_room_temperatures(is_full_load))

# COMMAND ----------

# Summary
print("=" * 70)
print("IOT TEMPERATURE TRANSFORMATIONS COMPLETE")
print("=" * 70)
for r in results:
    status_emoji = "✅" if r["status"] == "success" else "⚠️"
    print(f"{status_emoji} {r['table']}: {r['records']:,} records")

dbutils.notebook.exit(str(results))
