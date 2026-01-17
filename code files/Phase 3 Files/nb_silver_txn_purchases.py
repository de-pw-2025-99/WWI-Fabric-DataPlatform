# Databricks notebook source
# MAGIC %md
# MAGIC # nb_silver_txn_purchases
# MAGIC 
# MAGIC Transforms Purchase Order data from Bronze to Silver layer.
# MAGIC 
# MAGIC ## Source Tables
# MAGIC - **wwi_purchasing_purchaseorders** (~2,074 orders)
# MAGIC - **wwi_purchasing_purchaseorderlines** (~8,367 lines)
# MAGIC 
# MAGIC ## Output Tables
# MAGIC - **slv_purchase_order**
# MAGIC - **slv_purchase_order_line**

# COMMAND ----------

# MAGIC %run ./nb_silver_utilities

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

pipeline_run_id = dbutils.widgets.get("pipeline_run_id") if "pipeline_run_id" in [w.name for w in dbutils.widgets.getAll()] else None
is_full_load = dbutils.widgets.get("is_full_load") if "is_full_load" in [w.name for w in dbutils.widgets.getAll()] else "true"
is_full_load = is_full_load.lower() == "true"

results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_purchase_order

# COMMAND ----------

def transform_purchase_orders(is_full: bool = True):
    bronze_table = "wwi_purchasing_purchaseorders"
    silver_table = "slv_purchase_order"
    
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
    
    df = deduplicate_by_key(df, ["PurchaseOrderID"], "_bronze_loaded_at", "last")
    
    df_transformed = df.select(
        col("PurchaseOrderID").alias("purchase_order_id"),
        col("SupplierID").alias("supplier_id"),
        col("OrderDate").cast("date").alias("order_date"),
        col("DeliveryMethodID").alias("delivery_method_id"),
        col("ContactPersonID").alias("contact_person_id"),
        col("ExpectedDeliveryDate").cast("date").alias("expected_delivery_date"),
        col("SupplierReference").alias("supplier_reference"),
        col("IsOrderFinalized").alias("is_order_finalized"),
        col("Comments").alias("comments"),
        col("InternalComments").alias("internal_comments"),
        col("LastEditedBy").alias("last_edited_by"),
        col("LastEditedWhen").alias("last_edited_when"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df_cleaned = clean_all_string_columns(df_transformed)
    
    df_enriched = df_cleaned \
        .withColumn("order_year", year(col("order_date"))) \
        .withColumn("order_month", month(col("order_date"))) \
        .withColumn("days_to_delivery", datediff(col("expected_delivery_date"), col("order_date"))) \
        .withColumn("is_order_finalized", coalesce(col("is_order_finalized"), lit(False)))
    
    hash_columns = ["purchase_order_id", "supplier_id", "order_date", "is_order_finalized"]
    df_final = add_silver_metadata(df_enriched, bronze_table, pipeline_run_id, hash_columns)
    
    write_mode = "overwrite" if is_full else "append"
    if not is_full:
        records_written = merge_silver_table(df_final, silver_table, ["purchase_order_id"])
    else:
        records_written = write_silver_table(df_final, silver_table, write_mode,
                                            partition_by=["order_year"])
    
    if config and not is_full:
        update_watermark(config["config_id"], current_watermark)
    
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_purchase_orders(is_full_load))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_purchase_order_line

# COMMAND ----------

def transform_purchase_order_lines(is_full: bool = True):
    bronze_table = "wwi_purchasing_purchaseorderlines"
    silver_table = "slv_purchase_order_line"
    
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
    
    df = deduplicate_by_key(df, ["PurchaseOrderLineID"], "_bronze_loaded_at", "last")
    
    df_transformed = df.select(
        col("PurchaseOrderLineID").alias("purchase_order_line_id"),
        col("PurchaseOrderID").alias("purchase_order_id"),
        col("StockItemID").alias("stock_item_id"),
        col("OrderedOuters").cast("int").alias("ordered_outers"),
        col("Description").alias("description"),
        col("ReceivedOuters").cast("int").alias("received_outers"),
        col("PackageTypeID").alias("package_type_id"),
        col("ExpectedUnitPricePerOuter").cast("decimal(18,2)").alias("expected_unit_price_per_outer"),
        col("LastReceiptDate").cast("date").alias("last_receipt_date"),
        col("IsOrderLineFinalized").alias("is_order_line_finalized"),
        col("LastEditedBy").alias("last_edited_by"),
        col("LastEditedWhen").alias("last_edited_when"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df_cleaned = clean_all_string_columns(df_transformed)
    
    df_enriched = df_cleaned \
        .withColumn("line_total", round(col("ordered_outers") * col("expected_unit_price_per_outer"), 2)) \
        .withColumn("received_outers", coalesce(col("received_outers"), lit(0))) \
        .withColumn("outstanding_outers", col("ordered_outers") - col("received_outers")) \
        .withColumn("is_fully_received", col("received_outers") >= col("ordered_outers")) \
        .withColumn("receipt_variance", col("received_outers") - col("ordered_outers")) \
        .withColumn("is_order_line_finalized", coalesce(col("is_order_line_finalized"), lit(False)))
    
    hash_columns = ["purchase_order_line_id", "purchase_order_id", "stock_item_id", 
                    "ordered_outers", "received_outers", "expected_unit_price_per_outer"]
    df_final = add_silver_metadata(df_enriched, bronze_table, pipeline_run_id, hash_columns)
    
    write_mode = "overwrite" if is_full else "append"
    if not is_full:
        records_written = merge_silver_table(df_final, silver_table, ["purchase_order_line_id"])
    else:
        records_written = write_silver_table(df_final, silver_table, write_mode)
    
    if config and not is_full:
        update_watermark(config["config_id"], current_watermark)
    
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_purchase_order_lines(is_full_load))

# COMMAND ----------

# Summary
print("=" * 70)
print("PURCHASE ORDER TRANSFORMATIONS COMPLETE")
print("=" * 70)
for r in results:
    status_emoji = "✅" if r["status"] == "success" else "⚠️"
    print(f"{status_emoji} {r['table']}: {r['records']:,} records")

dbutils.notebook.exit(str(results))
