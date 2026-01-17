# Databricks notebook source
# MAGIC %md
# MAGIC # nb_silver_inventory
# MAGIC 
# MAGIC Transforms Inventory data from Bronze to Silver layer.
# MAGIC 
# MAGIC ## Source Tables
# MAGIC - **wwi_warehouse_stockitemholdings** (~227 records - current state)
# MAGIC - **wwi_warehouse_stockitemstockgroups** (~443 records - bridge table)
# MAGIC - **wwi_warehouse_stockitemtransactions** (~236,667 transactions)
# MAGIC 
# MAGIC ## Output Tables
# MAGIC - **slv_stock_item_holding**
# MAGIC - **slv_stock_item_stock_group**
# MAGIC - **slv_stock_item_transaction**

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
# MAGIC ## Transform: slv_stock_item_holding

# COMMAND ----------

def transform_stock_item_holdings():
    """Stock holdings is always full load - point-in-time snapshot."""
    bronze_table = "wwi_warehouse_stockitemholdings"
    silver_table = "slv_stock_item_holding"
    
    df = read_bronze_table(bronze_table)
    record_count = df.count()
    print(f"Records to process: {record_count:,}")
    
    df = deduplicate_by_key(df, ["StockItemID"], "_bronze_loaded_at", "last")
    
    df_transformed = df.select(
        col("StockItemID").alias("stock_item_id"),
        col("QuantityOnHand").cast("int").alias("quantity_on_hand"),
        col("BinLocation").alias("bin_location"),
        col("LastStocktakeQuantity").cast("int").alias("last_stocktake_quantity"),
        col("LastCostPrice").cast("decimal(18,2)").alias("last_cost_price"),
        col("ReorderLevel").cast("int").alias("reorder_level"),
        col("TargetStockLevel").cast("int").alias("target_stock_level"),
        col("LastEditedBy").alias("last_edited_by"),
        col("LastEditedWhen").alias("last_edited_when"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df_cleaned = clean_all_string_columns(df_transformed)
    
    # Add inventory metrics
    df_enriched = df_cleaned \
        .withColumn("inventory_value", round(col("quantity_on_hand") * col("last_cost_price"), 2)) \
        .withColumn("stocktake_variance", col("quantity_on_hand") - col("last_stocktake_quantity")) \
        .withColumn("needs_reorder", col("quantity_on_hand") <= col("reorder_level")) \
        .withColumn("stock_level_pct",
                   when(col("target_stock_level") > 0,
                        round(col("quantity_on_hand") / col("target_stock_level") * 100, 2))
                   .otherwise(lit(0))) \
        .withColumn("stock_status",
                   when(col("quantity_on_hand") <= 0, "out_of_stock")
                   .when(col("quantity_on_hand") <= col("reorder_level"), "low_stock")
                   .when(col("quantity_on_hand") >= col("target_stock_level"), "overstocked")
                   .otherwise("normal"))
    
    hash_columns = ["stock_item_id", "quantity_on_hand", "last_cost_price", "reorder_level"]
    df_final = add_silver_metadata(df_enriched, bronze_table, pipeline_run_id, hash_columns)
    
    records_written = write_silver_table(df_final, silver_table, "overwrite")
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_stock_item_holdings())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_stock_item_stock_group

# COMMAND ----------

def transform_stock_item_stock_groups():
    """Bridge table - always full load."""
    bronze_table = "wwi_warehouse_stockitemstockgroups"
    silver_table = "slv_stock_item_stock_group"
    
    df = read_bronze_table(bronze_table)
    record_count = df.count()
    print(f"Records to process: {record_count:,}")
    
    df = deduplicate_by_key(df, ["StockItemStockGroupID"], "_bronze_loaded_at", "last")
    
    df_transformed = df.select(
        col("StockItemStockGroupID").alias("stock_item_stock_group_id"),
        col("StockItemID").alias("stock_item_id"),
        col("StockGroupID").alias("stock_group_id"),
        col("LastEditedBy").alias("last_edited_by"),
        col("LastEditedWhen").alias("last_edited_when"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    hash_columns = ["stock_item_stock_group_id", "stock_item_id", "stock_group_id"]
    df_final = add_silver_metadata(df_transformed, bronze_table, pipeline_run_id, hash_columns)
    
    records_written = write_silver_table(df_final, silver_table, "overwrite")
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_stock_item_stock_groups())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_stock_item_transaction

# COMMAND ----------

def transform_stock_item_transactions(is_full: bool = True):
    bronze_table = "wwi_warehouse_stockitemtransactions"
    silver_table = "slv_stock_item_transaction"
    
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
    
    df = deduplicate_by_key(df, ["StockItemTransactionID"], "_bronze_loaded_at", "last")
    
    df_transformed = df.select(
        col("StockItemTransactionID").alias("stock_item_transaction_id"),
        col("StockItemID").alias("stock_item_id"),
        col("TransactionTypeID").alias("transaction_type_id"),
        col("CustomerID").alias("customer_id"),
        col("InvoiceID").alias("invoice_id"),
        col("SupplierID").alias("supplier_id"),
        col("PurchaseOrderID").alias("purchase_order_id"),
        col("TransactionOccurredWhen").alias("transaction_occurred_when"),
        col("Quantity").cast("decimal(18,3)").alias("quantity"),
        col("LastEditedBy").alias("last_edited_by"),
        col("LastEditedWhen").alias("last_edited_when"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df_cleaned = clean_all_string_columns(df_transformed)
    
    df_enriched = df_cleaned \
        .withColumn("transaction_date", to_date(col("transaction_occurred_when"))) \
        .withColumn("transaction_year", year(col("transaction_date"))) \
        .withColumn("transaction_month", month(col("transaction_date"))) \
        .withColumn("is_inbound", col("quantity") > 0) \
        .withColumn("is_outbound", col("quantity") < 0) \
        .withColumn("absolute_quantity", abs(col("quantity"))) \
        .withColumn("transaction_source",
                   when(col("invoice_id").isNotNull(), "sale")
                   .when(col("purchase_order_id").isNotNull(), "purchase")
                   .otherwise("adjustment"))
    
    hash_columns = ["stock_item_transaction_id", "stock_item_id", "transaction_type_id", 
                    "quantity", "transaction_occurred_when"]
    df_final = add_silver_metadata(df_enriched, bronze_table, pipeline_run_id, hash_columns)
    
    write_mode = "overwrite" if is_full else "append"
    if not is_full:
        records_written = merge_silver_table(df_final, silver_table, ["stock_item_transaction_id"])
    else:
        records_written = write_silver_table(df_final, silver_table, write_mode,
                                            partition_by=["transaction_year", "transaction_month"])
    
    if config and not is_full:
        update_watermark(config["config_id"], current_watermark)
    
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_stock_item_transactions(is_full_load))

# COMMAND ----------

# Summary
print("=" * 70)
print("INVENTORY TRANSFORMATIONS COMPLETE")
print("=" * 70)
for r in results:
    status_emoji = "✅" if r["status"] == "success" else "⚠️"
    print(f"{status_emoji} {r['table']}: {r['records']:,} records")

dbutils.notebook.exit(str(results))
