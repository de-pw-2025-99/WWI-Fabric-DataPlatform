# Databricks notebook source
# MAGIC %md
# MAGIC # nb_silver_reference_tables
# MAGIC 
# MAGIC Transforms reference/lookup tables from Bronze to Silver.
# MAGIC These are simple tables with minimal transformation needs.
# MAGIC 
# MAGIC ## Tables Processed
# MAGIC | Bronze | Silver | Records (approx) |
# MAGIC |--------|--------|------------------|
# MAGIC | wwi_application_cities | slv_city | 37,940 |
# MAGIC | wwi_application_countries | slv_country | 190 |
# MAGIC | wwi_application_deliverymethods | slv_delivery_method | 10 |
# MAGIC | wwi_application_paymentmethods | slv_payment_method | 4 |
# MAGIC | wwi_application_transactiontypes | slv_transaction_type | 13 |
# MAGIC | wwi_application_stateprovinces | slv_state_province | 53 |
# MAGIC | wwi_sales_buyinggroups | slv_buying_group | 2 |
# MAGIC | wwi_sales_customercategories | slv_customer_category | 8 |
# MAGIC | wwi_purchasing_suppliercategories | slv_supplier_category | 9 |
# MAGIC | wwi_warehouse_colors | slv_color | 36 |
# MAGIC | wwi_warehouse_packagetypes | slv_package_type | 14 |
# MAGIC | wwi_warehouse_stockgroups | slv_stock_group | 10 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ./nb_silver_utilities

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

# Pipeline parameters (would be passed from orchestration)
pipeline_run_id = dbutils.widgets.get("pipeline_run_id") if "pipeline_run_id" in [w.name for w in dbutils.widgets.getAll()] else None

# Processing timestamp for this run
processing_timestamp = datetime.utcnow().isoformat()

# Track results
results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_city
# MAGIC 
# MAGIC Source: wwi_application_cities
# MAGIC - Handles Geography column (already converted to WKT in Bronze)
# MAGIC - Standardizes column names
# MAGIC - Adds Silver metadata

# COMMAND ----------

def transform_cities():
    """Transform cities reference table."""
    
    bronze_table = "wwi_application_cities"
    silver_table = "slv_city"
    
    # Read from Bronze
    df = read_bronze_table(bronze_table)
    
    # Check for duplicates
    dup_stats = get_duplicate_stats(df, ["CityID"])
    print(f"Duplicate stats: {dup_stats}")
    
    # Deduplicate (keep latest)
    df = deduplicate_by_key(df, ["CityID"], "_bronze_loaded_at", "last")
    
    # Select and rename columns (clean names)
    df = df.select(
        col("CityID").alias("city_id"),
        col("CityName").alias("city_name"),
        col("StateProvinceID").alias("state_province_id"),
        col("LatestRecordedPopulation").alias("latest_recorded_population"),
        col("Location_WKT").alias("location_wkt"),  # Geography as WKT from Bronze
        col("LastEditedBy").alias("last_edited_by"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    # Clean string columns
    df = clean_string_column(df, "city_name")
    
    # Add Silver metadata
    df = add_silver_metadata(df, bronze_table, pipeline_run_id, 
                             ["city_id", "city_name", "state_province_id", "latest_recorded_population"])
    
    # Write to Silver
    records_written = write_silver_table(df, silver_table, "overwrite")
    
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_cities())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_country

# COMMAND ----------

def transform_countries():
    """Transform countries reference table."""
    
    bronze_table = "wwi_application_countries"
    silver_table = "slv_country"
    
    df = read_bronze_table(bronze_table)
    df = deduplicate_by_key(df, ["CountryID"], "_bronze_loaded_at", "last")
    
    df = df.select(
        col("CountryID").alias("country_id"),
        col("CountryName").alias("country_name"),
        col("FormalName").alias("formal_name"),
        col("IsoAlpha3Code").alias("iso_alpha3_code"),
        col("IsoNumericCode").alias("iso_numeric_code"),
        col("CountryType").alias("country_type"),
        col("LatestRecordedPopulation").alias("latest_recorded_population"),
        col("Continent").alias("continent"),
        col("Region").alias("region"),
        col("Subregion").alias("subregion"),
        col("LastEditedBy").alias("last_edited_by"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df = clean_all_string_columns(df)
    df = add_silver_metadata(df, bronze_table, pipeline_run_id,
                             ["country_id", "country_name", "iso_alpha3_code"])
    
    records_written = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_countries())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_state_province

# COMMAND ----------

def transform_state_provinces():
    """Transform state provinces reference table."""
    
    bronze_table = "wwi_application_stateprovinces"
    silver_table = "slv_state_province"
    
    df = read_bronze_table(bronze_table)
    df = deduplicate_by_key(df, ["StateProvinceID"], "_bronze_loaded_at", "last")
    
    df = df.select(
        col("StateProvinceID").alias("state_province_id"),
        col("StateProvinceCode").alias("state_province_code"),
        col("StateProvinceName").alias("state_province_name"),
        col("CountryID").alias("country_id"),
        col("SalesTerritory").alias("sales_territory"),
        col("LatestRecordedPopulation").alias("latest_recorded_population"),
        col("LastEditedBy").alias("last_edited_by"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df = clean_all_string_columns(df)
    df = standardize_code_column(df, "state_province_code")
    df = add_silver_metadata(df, bronze_table, pipeline_run_id,
                             ["state_province_id", "state_province_code", "state_province_name", "country_id"])
    
    records_written = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_state_provinces())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_delivery_method

# COMMAND ----------

def transform_delivery_methods():
    """Transform delivery methods reference table."""
    
    bronze_table = "wwi_application_deliverymethods"
    silver_table = "slv_delivery_method"
    
    df = read_bronze_table(bronze_table)
    df = deduplicate_by_key(df, ["DeliveryMethodID"], "_bronze_loaded_at", "last")
    
    df = df.select(
        col("DeliveryMethodID").alias("delivery_method_id"),
        col("DeliveryMethodName").alias("delivery_method_name"),
        col("LastEditedBy").alias("last_edited_by"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df = clean_all_string_columns(df)
    df = add_silver_metadata(df, bronze_table, pipeline_run_id,
                             ["delivery_method_id", "delivery_method_name"])
    
    records_written = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_delivery_methods())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_payment_method

# COMMAND ----------

def transform_payment_methods():
    """Transform payment methods reference table."""
    
    bronze_table = "wwi_application_paymentmethods"
    silver_table = "slv_payment_method"
    
    df = read_bronze_table(bronze_table)
    df = deduplicate_by_key(df, ["PaymentMethodID"], "_bronze_loaded_at", "last")
    
    df = df.select(
        col("PaymentMethodID").alias("payment_method_id"),
        col("PaymentMethodName").alias("payment_method_name"),
        col("LastEditedBy").alias("last_edited_by"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df = clean_all_string_columns(df)
    df = add_silver_metadata(df, bronze_table, pipeline_run_id,
                             ["payment_method_id", "payment_method_name"])
    
    records_written = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_payment_methods())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_transaction_type

# COMMAND ----------

def transform_transaction_types():
    """Transform transaction types reference table."""
    
    bronze_table = "wwi_application_transactiontypes"
    silver_table = "slv_transaction_type"
    
    df = read_bronze_table(bronze_table)
    df = deduplicate_by_key(df, ["TransactionTypeID"], "_bronze_loaded_at", "last")
    
    df = df.select(
        col("TransactionTypeID").alias("transaction_type_id"),
        col("TransactionTypeName").alias("transaction_type_name"),
        col("LastEditedBy").alias("last_edited_by"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df = clean_all_string_columns(df)
    df = add_silver_metadata(df, bronze_table, pipeline_run_id,
                             ["transaction_type_id", "transaction_type_name"])
    
    records_written = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_transaction_types())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_buying_group

# COMMAND ----------

def transform_buying_groups():
    """Transform buying groups reference table."""
    
    bronze_table = "wwi_sales_buyinggroups"
    silver_table = "slv_buying_group"
    
    df = read_bronze_table(bronze_table)
    df = deduplicate_by_key(df, ["BuyingGroupID"], "_bronze_loaded_at", "last")
    
    df = df.select(
        col("BuyingGroupID").alias("buying_group_id"),
        col("BuyingGroupName").alias("buying_group_name"),
        col("LastEditedBy").alias("last_edited_by"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df = clean_all_string_columns(df)
    df = add_silver_metadata(df, bronze_table, pipeline_run_id,
                             ["buying_group_id", "buying_group_name"])
    
    records_written = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_buying_groups())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_customer_category

# COMMAND ----------

def transform_customer_categories():
    """Transform customer categories reference table."""
    
    bronze_table = "wwi_sales_customercategories"
    silver_table = "slv_customer_category"
    
    df = read_bronze_table(bronze_table)
    df = deduplicate_by_key(df, ["CustomerCategoryID"], "_bronze_loaded_at", "last")
    
    df = df.select(
        col("CustomerCategoryID").alias("customer_category_id"),
        col("CustomerCategoryName").alias("customer_category_name"),
        col("LastEditedBy").alias("last_edited_by"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df = clean_all_string_columns(df)
    df = add_silver_metadata(df, bronze_table, pipeline_run_id,
                             ["customer_category_id", "customer_category_name"])
    
    records_written = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_customer_categories())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_supplier_category

# COMMAND ----------

def transform_supplier_categories():
    """Transform supplier categories reference table."""
    
    bronze_table = "wwi_purchasing_suppliercategories"
    silver_table = "slv_supplier_category"
    
    df = read_bronze_table(bronze_table)
    df = deduplicate_by_key(df, ["SupplierCategoryID"], "_bronze_loaded_at", "last")
    
    df = df.select(
        col("SupplierCategoryID").alias("supplier_category_id"),
        col("SupplierCategoryName").alias("supplier_category_name"),
        col("LastEditedBy").alias("last_edited_by"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df = clean_all_string_columns(df)
    df = add_silver_metadata(df, bronze_table, pipeline_run_id,
                             ["supplier_category_id", "supplier_category_name"])
    
    records_written = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_supplier_categories())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_color

# COMMAND ----------

def transform_colors():
    """Transform colors reference table."""
    
    bronze_table = "wwi_warehouse_colors"
    silver_table = "slv_color"
    
    df = read_bronze_table(bronze_table)
    df = deduplicate_by_key(df, ["ColorID"], "_bronze_loaded_at", "last")
    
    df = df.select(
        col("ColorID").alias("color_id"),
        col("ColorName").alias("color_name"),
        col("LastEditedBy").alias("last_edited_by"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df = clean_all_string_columns(df)
    df = add_silver_metadata(df, bronze_table, pipeline_run_id,
                             ["color_id", "color_name"])
    
    records_written = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_colors())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_package_type

# COMMAND ----------

def transform_package_types():
    """Transform package types reference table."""
    
    bronze_table = "wwi_warehouse_packagetypes"
    silver_table = "slv_package_type"
    
    df = read_bronze_table(bronze_table)
    df = deduplicate_by_key(df, ["PackageTypeID"], "_bronze_loaded_at", "last")
    
    df = df.select(
        col("PackageTypeID").alias("package_type_id"),
        col("PackageTypeName").alias("package_type_name"),
        col("LastEditedBy").alias("last_edited_by"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df = clean_all_string_columns(df)
    df = add_silver_metadata(df, bronze_table, pipeline_run_id,
                             ["package_type_id", "package_type_name"])
    
    records_written = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_package_types())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_stock_group

# COMMAND ----------

def transform_stock_groups():
    """Transform stock groups reference table."""
    
    bronze_table = "wwi_warehouse_stockgroups"
    silver_table = "slv_stock_group"
    
    df = read_bronze_table(bronze_table)
    df = deduplicate_by_key(df, ["StockGroupID"], "_bronze_loaded_at", "last")
    
    df = df.select(
        col("StockGroupID").alias("stock_group_id"),
        col("StockGroupName").alias("stock_group_name"),
        col("LastEditedBy").alias("last_edited_by"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df = clean_all_string_columns(df)
    df = add_silver_metadata(df, bronze_table, pipeline_run_id,
                             ["stock_group_id", "stock_group_name"])
    
    records_written = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_stock_groups())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Display results
print("=" * 70)
print("REFERENCE TABLES TRANSFORMATION COMPLETE")
print("=" * 70)
print(f"Processing Time: {processing_timestamp}")
print("-" * 70)

total_records = 0
for r in results:
    status_emoji = "✅" if r["status"] == "success" else "❌"
    print(f"{status_emoji} {r['table']}: {r['records']:,} records")
    total_records += r['records']

print("-" * 70)
print(f"TOTAL: {len(results)} tables, {total_records:,} records")
print("=" * 70)

# Return results for pipeline
dbutils.notebook.exit(str(results))
