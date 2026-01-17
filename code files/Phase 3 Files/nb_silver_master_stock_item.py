# Databricks notebook source
# MAGIC %md
# MAGIC # nb_silver_master_stock_item
# MAGIC 
# MAGIC Transforms Stock Item (Product) data from Bronze to Silver layer.
# MAGIC 
# MAGIC ## Source Table
# MAGIC - **wwi_warehouse_stockitems** (~227 items)
# MAGIC 
# MAGIC ## Key Transformations
# MAGIC - Handle CustomFields JSON column
# MAGIC - Extract tags from SearchDetails
# MAGIC - Calculate margin percentages
# MAGIC - Standardize measurements
# MAGIC 
# MAGIC ## Output Table
# MAGIC - **slv_stock_item**

# COMMAND ----------

# MAGIC %run ./nb_silver_utilities

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

pipeline_run_id = dbutils.widgets.get("pipeline_run_id") if "pipeline_run_id" in [w.name for w in dbutils.widgets.getAll()] else None

BRONZE_TABLE = "wwi_warehouse_stockitems"
SILVER_TABLE = "slv_stock_item"

# COMMAND ----------

# Read and deduplicate
df_bronze = read_bronze_table(BRONZE_TABLE)
print(f"Bronze records: {df_bronze.count():,}")

df_dedup = deduplicate_by_key(df_bronze, ["StockItemID"], "_bronze_loaded_at", "last")

# COMMAND ----------

# Transform
df_transformed = df_dedup.select(
    # Primary Key
    col("StockItemID").alias("stock_item_id"),
    
    # Descriptive
    col("StockItemName").alias("stock_item_name"),
    col("Brand").alias("brand"),
    col("Size").alias("size"),
    col("SearchDetails").alias("search_details"),
    col("CustomFields").alias("custom_fields_json"),
    col("MarketingComments").alias("marketing_comments"),
    col("Photo").alias("photo"),  # Binary data
    
    # Foreign Keys
    col("SupplierID").alias("supplier_id"),
    col("ColorID").alias("color_id"),
    col("UnitPackageID").alias("unit_package_id"),
    col("OuterPackageID").alias("outer_package_id"),
    
    # Pricing
    col("UnitPrice").cast("decimal(18,2)").alias("unit_price"),
    col("RecommendedRetailPrice").cast("decimal(18,2)").alias("recommended_retail_price"),
    col("TypicalWeightPerUnit").cast("decimal(18,3)").alias("typical_weight_per_unit"),
    col("TaxRate").cast("decimal(18,3)").alias("tax_rate"),
    
    # Inventory
    col("QuantityPerOuter").alias("quantity_per_outer"),
    col("LeadTimeDays").alias("lead_time_days"),
    col("IsChillerStock").alias("is_chiller_stock"),
    col("Barcode").alias("barcode"),
    
    # Audit
    col("LastEditedBy").alias("last_edited_by"),
    col("ValidFrom").alias("source_valid_from"),
    col("ValidTo").alias("source_valid_to"),
    col("_bronze_loaded_at"),
    col("_bronze_pipeline_run_id")
)

# COMMAND ----------

# Clean strings
df_cleaned = clean_all_string_columns(df_transformed)

# Add derived columns
df_enriched = df_cleaned \
    .withColumn("unit_margin", 
                when(col("unit_price") > 0, 
                     round(col("recommended_retail_price") - col("unit_price"), 2))
                .otherwise(lit(0))) \
    .withColumn("margin_percentage",
                when((col("recommended_retail_price") > 0) & (col("unit_price") > 0),
                     round((col("recommended_retail_price") - col("unit_price")) / col("recommended_retail_price") * 100, 2))
                .otherwise(lit(0))) \
    .withColumn("price_tier",
                when(col("unit_price") < 10, "budget")
                .when(col("unit_price") < 50, "standard")
                .when(col("unit_price") < 100, "premium")
                .otherwise("luxury")) \
    .withColumn("has_brand", col("brand").isNotNull() & (length(col("brand")) > 0)) \
    .withColumn("has_barcode", col("barcode").isNotNull() & (length(col("barcode")) > 0)) \
    .withColumn("has_photo", col("photo").isNotNull())

# Handle nulls
df_with_defaults = df_enriched \
    .withColumn("lead_time_days", coalesce(col("lead_time_days"), lit(14))) \
    .withColumn("quantity_per_outer", coalesce(col("quantity_per_outer"), lit(1))) \
    .withColumn("is_chiller_stock", coalesce(col("is_chiller_stock"), lit(False)))

# Add Silver metadata
hash_columns = ["stock_item_id", "stock_item_name", "supplier_id", "unit_price", 
                "recommended_retail_price", "is_chiller_stock"]
df_final = add_silver_metadata(df_with_defaults, BRONZE_TABLE, pipeline_run_id, hash_columns)

# COMMAND ----------

# Data quality checks
print("Data Quality Checks:")
print(f"  - Records with brand: {df_final.filter(col('has_brand')).count():,}")
print(f"  - Chiller items: {df_final.filter(col('is_chiller_stock')).count():,}")
print(f"  - Items with barcode: {df_final.filter(col('has_barcode')).count():,}")

# Price tier distribution
display(df_final.groupBy("price_tier").count().orderBy("price_tier"))

# COMMAND ----------

# Write to Silver
records_written = write_silver_table(df_final, SILVER_TABLE, "overwrite")
print(f"✅ Wrote {records_written:,} records to {SILVER_TABLE}")

# COMMAND ----------

summary = {"bronze_table": BRONZE_TABLE, "silver_table": SILVER_TABLE, "records_written": records_written, "status": "success"}
dbutils.notebook.exit(str(summary))
