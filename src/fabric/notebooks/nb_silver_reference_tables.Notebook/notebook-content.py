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
# META         },
# META         {
# META           "id": "f4ac55a7-ed20-4139-8a6b-6c40b9eb157d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Step 3: Create nb_silver_reference_tables

# MARKDOWN ********************

# # Cell 1: Setup - Run Utilities

# CELL ********************

%run nb_silver_utilities

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 2: Configuration

# CELL ********************

from datetime import datetime

# Track results
results = []
processing_timestamp = datetime.utcnow().isoformat()

print(f"Processing started: {processing_timestamp}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 3: Transform slv_city

# CELL ********************

def transform_cities():
    bronze_table = "wwi_application_cities"
    silver_table = "slv_city"
    
    df = read_bronze_table(bronze_table)
    df = deduplicate_by_key(df, ["CityID"], "_bronze_loaded_at", "last")
    
    df = df.select(
        col("CityID").alias("city_id"),
        col("CityName").alias("city_name"),
        col("StateProvinceID").alias("state_province_id"),
        col("LatestRecordedPopulation").alias("latest_recorded_population"),
        col("Location_WKT").alias("location_wkt"),
        col("LastEditedBy").alias("last_edited_by"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df = clean_all_string_columns(df)
    df = add_silver_metadata(df, bronze_table, None, ["city_id", "city_name", "state_province_id"])
    
    records = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records, "status": "success"}

results.append(transform_cities())
print(f"✅ slv_city: {results[-1]['records']:,} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 4: Transform slv_country

# CELL ********************

def transform_countries():
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
    df = add_silver_metadata(df, bronze_table, None, ["country_id", "country_name", "iso_alpha3_code"])
    
    records = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records, "status": "success"}

results.append(transform_countries())
print(f"✅ slv_country: {results[-1]['records']:,} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 5: Transform slv_state_province

# CELL ********************

def transform_state_provinces():
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
    df = add_silver_metadata(df, bronze_table, None, ["state_province_id", "state_province_code", "country_id"])
    
    records = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records, "status": "success"}

results.append(transform_state_provinces())
print(f"✅ slv_state_province: {results[-1]['records']:,} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 6: Transform Simple Reference Tables (8 tables)

# CELL ********************

# These tables have similar simple structures: ID, Name, audit columns

simple_tables = [
    ("wwi_application_deliverymethods", "slv_delivery_method", "DeliveryMethodID", "DeliveryMethodName", "delivery_method_id", "delivery_method_name"),
    ("wwi_application_paymentmethods", "slv_payment_method", "PaymentMethodID", "PaymentMethodName", "payment_method_id", "payment_method_name"),
    ("wwi_application_transactiontypes", "slv_transaction_type", "TransactionTypeID", "TransactionTypeName", "transaction_type_id", "transaction_type_name"),
    ("wwi_sales_buyinggroups", "slv_buying_group", "BuyingGroupID", "BuyingGroupName", "buying_group_id", "buying_group_name"),
    ("wwi_sales_customercategories", "slv_customer_category", "CustomerCategoryID", "CustomerCategoryName", "customer_category_id", "customer_category_name"),
    ("wwi_purchasing_suppliercategories", "slv_supplier_category", "SupplierCategoryID", "SupplierCategoryName", "supplier_category_id", "supplier_category_name"),
    ("wwi_warehouse_colors", "slv_color", "ColorID", "ColorName", "color_id", "color_name"),
    ("wwi_warehouse_packagetypes", "slv_package_type", "PackageTypeID", "PackageTypeName", "package_type_id", "package_type_name"),
]

for bronze_table, silver_table, pk_col, name_col, pk_alias, name_alias in simple_tables:
    df = read_bronze_table(bronze_table)
    df = deduplicate_by_key(df, [pk_col], "_bronze_loaded_at", "last")
    
    df = df.select(
        col(pk_col).alias(pk_alias),
        col(name_col).alias(name_alias),
        col("LastEditedBy").alias("last_edited_by"),
        col("ValidFrom").alias("valid_from"),
        col("ValidTo").alias("valid_to"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df = clean_all_string_columns(df)
    df = add_silver_metadata(df, bronze_table, None, [pk_alias, name_alias])
    
    records = write_silver_table(df, silver_table, "overwrite")
    results.append({"table": silver_table, "records": records, "status": "success"})
    print(f"✅ {silver_table}: {records:,} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 7: Transform slv_stock_group

# CELL ********************

def transform_stock_groups():
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
    df = add_silver_metadata(df, bronze_table, None, ["stock_group_id", "stock_group_name"])
    
    records = write_silver_table(df, silver_table, "overwrite")
    return {"table": silver_table, "records": records, "status": "success"}

results.append(transform_stock_groups())
print(f"✅ slv_stock_group: {results[-1]['records']:,} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 8: Summary

# CELL ********************

print("\n" + "=" * 70)
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
