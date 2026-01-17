# Databricks notebook source
# MAGIC %md
# MAGIC # nb_silver_txn_orders
# MAGIC 
# MAGIC Transforms Sales Orders and Order Lines from Bronze to Silver layer.
# MAGIC 
# MAGIC ## Source Tables
# MAGIC - **wwi_sales_orders** (~73,595 orders)
# MAGIC - **wwi_sales_orderlines** (~231,412 order lines)
# MAGIC - **wwi_sales_specialdeals** (~3 deals)
# MAGIC 
# MAGIC ## Key Transformations
# MAGIC 1. Incremental processing using watermark
# MAGIC 2. Deduplicate on primary keys
# MAGIC 3. Data type standardization
# MAGIC 4. Referential integrity validation
# MAGIC 5. Calculate derived metrics (line totals, etc.)
# MAGIC 
# MAGIC ## Output Tables
# MAGIC - **slv_order**
# MAGIC - **slv_order_line**
# MAGIC - **slv_special_deal**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ./nb_silver_utilities

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Pipeline parameters
pipeline_run_id = dbutils.widgets.get("pipeline_run_id") if "pipeline_run_id" in [w.name for w in dbutils.widgets.getAll()] else None
is_full_load = dbutils.widgets.get("is_full_load") if "is_full_load" in [w.name for w in dbutils.widgets.getAll()] else "true"
is_full_load = is_full_load.lower() == "true"

# Track results
results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_order

# COMMAND ----------

def transform_orders(is_full: bool = True):
    """Transform orders from Bronze to Silver."""
    
    bronze_table = "wwi_sales_orders"
    silver_table = "slv_order"
    
    # Get configuration for watermark
    config = get_config_by_table(bronze_table) if not is_full else None
    last_watermark = config.get("last_watermark", "1900-01-01T00:00:00") if config else "1900-01-01T00:00:00"
    current_watermark = datetime.utcnow().isoformat()
    
    # Read from Bronze (full or incremental)
    if is_full:
        df = read_bronze_table(bronze_table)
        print(f"Full load - reading all records")
    else:
        df = read_bronze_incremental(bronze_table, "_bronze_loaded_at", last_watermark, current_watermark)
        print(f"Incremental load - watermark from {last_watermark} to {current_watermark}")
    
    record_count = df.count()
    print(f"Records to process: {record_count:,}")
    
    if record_count == 0:
        return {"table": silver_table, "records": 0, "status": "no_data"}
    
    # Deduplicate
    df = deduplicate_by_key(df, ["OrderID"], "_bronze_loaded_at", "last")
    
    # Transform
    df_transformed = df.select(
        # Primary Key
        col("OrderID").alias("order_id"),
        
        # Foreign Keys
        col("CustomerID").alias("customer_id"),
        col("SalespersonPersonID").alias("salesperson_person_id"),
        col("PickedByPersonID").alias("picked_by_person_id"),
        col("ContactPersonID").alias("contact_person_id"),
        col("BackorderOrderID").alias("backorder_order_id"),
        
        # Dates
        col("OrderDate").cast("date").alias("order_date"),
        col("ExpectedDeliveryDate").cast("date").alias("expected_delivery_date"),
        
        # Business Data
        col("CustomerPurchaseOrderNumber").alias("customer_purchase_order_number"),
        col("IsUndersupplyBackordered").alias("is_undersupply_backordered"),
        col("Comments").alias("comments"),
        col("DeliveryInstructions").alias("delivery_instructions"),
        col("InternalComments").alias("internal_comments"),
        
        # Audit from source
        col("LastEditedBy").alias("last_edited_by"),
        col("LastEditedWhen").alias("last_edited_when"),
        
        # Bronze metadata
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    # Clean strings
    df_transformed = clean_all_string_columns(df_transformed)
    
    # Add derived columns
    df_enriched = df_transformed \
        .withColumn("order_year", year(col("order_date"))) \
        .withColumn("order_month", month(col("order_date"))) \
        .withColumn("order_day_of_week", dayofweek(col("order_date"))) \
        .withColumn("days_until_delivery", datediff(col("expected_delivery_date"), col("order_date"))) \
        .withColumn("is_backorder", col("backorder_order_id").isNotNull())
    
    # Add Silver metadata
    hash_columns = ["order_id", "customer_id", "order_date", "expected_delivery_date", 
                    "is_undersupply_backordered"]
    df_final = add_silver_metadata(df_enriched, bronze_table, pipeline_run_id, hash_columns)
    
    # Write to Silver
    write_mode = "overwrite" if is_full else "append"
    
    if not is_full:
        # For incremental, use merge to handle updates
        records_written = merge_silver_table(df_final, silver_table, ["order_id"])
        print(f"Merged {df_final.count():,} records")
    else:
        records_written = write_silver_table(df_final, silver_table, write_mode, 
                                            partition_by=["order_year", "order_month"])
    
    # Update watermark on success
    if config and not is_full:
        update_watermark(config["config_id"], current_watermark)
    
    return {
        "table": silver_table, 
        "records": records_written, 
        "status": "success",
        "watermark": current_watermark
    }

results.append(transform_orders(is_full_load))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_order_line

# COMMAND ----------

def transform_order_lines(is_full: bool = True):
    """Transform order lines from Bronze to Silver."""
    
    bronze_table = "wwi_sales_orderlines"
    silver_table = "slv_order_line"
    
    # Get configuration for watermark
    config = get_config_by_table(bronze_table) if not is_full else None
    last_watermark = config.get("last_watermark", "1900-01-01T00:00:00") if config else "1900-01-01T00:00:00"
    current_watermark = datetime.utcnow().isoformat()
    
    # Read from Bronze
    if is_full:
        df = read_bronze_table(bronze_table)
        print(f"Full load - reading all records")
    else:
        df = read_bronze_incremental(bronze_table, "_bronze_loaded_at", last_watermark, current_watermark)
        print(f"Incremental load - watermark from {last_watermark} to {current_watermark}")
    
    record_count = df.count()
    print(f"Records to process: {record_count:,}")
    
    if record_count == 0:
        return {"table": silver_table, "records": 0, "status": "no_data"}
    
    # Deduplicate
    df = deduplicate_by_key(df, ["OrderLineID"], "_bronze_loaded_at", "last")
    
    # Transform
    df_transformed = df.select(
        # Primary Key
        col("OrderLineID").alias("order_line_id"),
        
        # Foreign Keys
        col("OrderID").alias("order_id"),
        col("StockItemID").alias("stock_item_id"),
        col("PackageTypeID").alias("package_type_id"),
        
        # Line Details
        col("Description").alias("description"),
        col("Quantity").cast("int").alias("quantity"),
        col("UnitPrice").cast("decimal(18,2)").alias("unit_price"),
        col("TaxRate").cast("decimal(18,3)").alias("tax_rate"),
        col("PickedQuantity").cast("int").alias("picked_quantity"),
        col("PickingCompletedWhen").alias("picking_completed_when"),
        
        # Audit from source
        col("LastEditedBy").alias("last_edited_by"),
        col("LastEditedWhen").alias("last_edited_when"),
        
        # Bronze metadata
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    # Clean strings
    df_transformed = clean_all_string_columns(df_transformed)
    
    # Add calculated columns
    df_enriched = df_transformed \
        .withColumn("line_total", round(col("quantity") * col("unit_price"), 2)) \
        .withColumn("tax_amount", round(col("quantity") * col("unit_price") * col("tax_rate") / 100, 2)) \
        .withColumn("line_total_with_tax", round(col("line_total") + col("tax_amount"), 2)) \
        .withColumn("is_fully_picked", col("picked_quantity") >= col("quantity")) \
        .withColumn("pick_variance", col("picked_quantity") - col("quantity"))
    
    # Handle nulls
    df_enriched = df_enriched \
        .withColumn("picked_quantity", coalesce(col("picked_quantity"), lit(0))) \
        .withColumn("pick_variance", coalesce(col("pick_variance"), lit(0)))
    
    # Add Silver metadata
    hash_columns = ["order_line_id", "order_id", "stock_item_id", "quantity", "unit_price", "tax_rate"]
    df_final = add_silver_metadata(df_enriched, bronze_table, pipeline_run_id, hash_columns)
    
    # Write to Silver
    write_mode = "overwrite" if is_full else "append"
    
    if not is_full:
        records_written = merge_silver_table(df_final, silver_table, ["order_line_id"])
    else:
        records_written = write_silver_table(df_final, silver_table, write_mode)
    
    # Update watermark on success
    if config and not is_full:
        update_watermark(config["config_id"], current_watermark)
    
    return {
        "table": silver_table, 
        "records": records_written, 
        "status": "success",
        "watermark": current_watermark
    }

results.append(transform_order_lines(is_full_load))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_special_deal

# COMMAND ----------

def transform_special_deals():
    """Transform special deals from Bronze to Silver (always full load - small table)."""
    
    bronze_table = "wwi_sales_specialdeals"
    silver_table = "slv_special_deal"
    
    # Read from Bronze (always full for this small table)
    df = read_bronze_table(bronze_table)
    record_count = df.count()
    print(f"Records to process: {record_count:,}")
    
    if record_count == 0:
        return {"table": silver_table, "records": 0, "status": "no_data"}
    
    # Deduplicate
    df = deduplicate_by_key(df, ["SpecialDealID"], "_bronze_loaded_at", "last")
    
    # Transform
    df_transformed = df.select(
        # Primary Key
        col("SpecialDealID").alias("special_deal_id"),
        
        # Foreign Keys
        col("StockItemID").alias("stock_item_id"),
        col("CustomerID").alias("customer_id"),
        col("BuyingGroupID").alias("buying_group_id"),
        col("CustomerCategoryID").alias("customer_category_id"),
        col("StockGroupID").alias("stock_group_id"),
        
        # Deal Details
        col("DealDescription").alias("deal_description"),
        col("StartDate").cast("date").alias("start_date"),
        col("EndDate").cast("date").alias("end_date"),
        col("DiscountAmount").cast("decimal(18,2)").alias("discount_amount"),
        col("DiscountPercentage").cast("decimal(18,3)").alias("discount_percentage"),
        col("UnitPrice").cast("decimal(18,2)").alias("unit_price"),
        
        # Audit from source
        col("LastEditedBy").alias("last_edited_by"),
        col("LastEditedWhen").alias("last_edited_when"),
        
        # Bronze metadata
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    # Clean strings
    df_transformed = clean_all_string_columns(df_transformed)
    
    # Add derived columns
    df_enriched = df_transformed \
        .withColumn("deal_duration_days", datediff(col("end_date"), col("start_date"))) \
        .withColumn("is_active", 
                   (col("start_date") <= current_date()) & 
                   ((col("end_date").isNull()) | (col("end_date") >= current_date()))) \
        .withColumn("deal_type", 
                   when(col("discount_percentage").isNotNull() & (col("discount_percentage") > 0), "percentage")
                   .when(col("discount_amount").isNotNull() & (col("discount_amount") > 0), "amount")
                   .when(col("unit_price").isNotNull(), "fixed_price")
                   .otherwise("unknown"))
    
    # Add Silver metadata
    hash_columns = ["special_deal_id", "deal_description", "start_date", "end_date", 
                    "discount_amount", "discount_percentage", "unit_price"]
    df_final = add_silver_metadata(df_enriched, bronze_table, pipeline_run_id, hash_columns)
    
    # Write to Silver
    records_written = write_silver_table(df_final, silver_table, "overwrite")
    
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_special_deals())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation

# COMMAND ----------

# Validate referential integrity between orders and order lines
df_orders = spark.table("slv_order")
df_lines = spark.table("slv_order_line")

# Check: All order lines should have a valid order
orphan_lines = df_lines.join(
    df_orders.select("order_id"),
    df_lines["order_id"] == df_orders["order_id"],
    "left_anti"
)

orphan_count = orphan_lines.count()
if orphan_count > 0:
    print(f"⚠️ WARNING: Found {orphan_count:,} order lines without matching orders")
    display(orphan_lines.select("order_line_id", "order_id").limit(10))
else:
    print("✅ All order lines have valid orders")

# COMMAND ----------

# Validate order totals
order_totals = df_lines.groupBy("order_id").agg(
    count("*").alias("line_count"),
    sum("quantity").alias("total_quantity"),
    sum("line_total").alias("total_amount"),
    sum("line_total_with_tax").alias("total_with_tax")
)

print("Order Statistics:")
display(order_totals.select(
    mean("line_count").alias("avg_lines_per_order"),
    mean("total_quantity").alias("avg_items_per_order"),
    mean("total_amount").alias("avg_order_amount"),
    max("total_amount").alias("max_order_amount")
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("ORDER TRANSFORMATIONS COMPLETE")
print("=" * 70)
print(f"Load Type: {'Full' if is_full_load else 'Incremental'}")
print("-" * 70)

total_records = 0
for r in results:
    status_emoji = "✅" if r["status"] == "success" else "⚠️" if r["status"] == "no_data" else "❌"
    print(f"{status_emoji} {r['table']}: {r['records']:,} records")
    total_records += r['records']

print("-" * 70)
print(f"TOTAL: {len(results)} tables, {total_records:,} records")
print("=" * 70)

# Return results
dbutils.notebook.exit(str(results))
