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

# # Cell 1:  Create `_silver_load_config` table

# CELL ********************

df = spark.sql("SELECT * FROM lh_silver.dbo._silver_load_config LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

silver_load_config_schema = StructType([
    StructField("config_id", IntegerType(), False),
    StructField("bronze_table", StringType(), False),
    StructField("silver_table", StringType(), False),
    StructField("category", StringType(), False),
    StructField("load_type", StringType(), False),
    StructField("load_priority", IntegerType(), False),
    StructField("primary_key_columns", StringType(), False),
    StructField("business_key_columns", StringType(), True),
    StructField("watermark_column", StringType(), True),
    StructField("last_watermark", StringType(), True), 
    StructField("transformation_notebook", StringType(), False),
    StructField("dq_checks_enabled", BooleanType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False)
])

spark.createDataFrame([], silver_load_config_schema) \
    .write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("_silver_load_config")

print("✅ _silver_load_config table created")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  # Cell 2: Create `_silver_load_history` table

# CELL ********************

silver_load_history_schema = StructType([
    StructField("load_id", LongType(), False),
    StructField("config_id", IntegerType(), False),
    StructField("bronze_table", StringType(), False),
    StructField("silver_table", StringType(), False),
    StructField("pipeline_run_id", StringType(), True),
    StructField("notebook_run_id", StringType(), True),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("records_read", LongType(), True),
    StructField("records_written", LongType(), True),
    StructField("records_rejected", LongType(), True),
    StructField("watermark_start", StringType(), True),
    StructField("watermark_end", StringType(), True),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True)
])

spark.createDataFrame([], silver_load_history_schema) \
    .write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("_silver_load_history")

print("✅ _silver_load_history table created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 3: Create `_silver_data_quality_log` table

# CELL ********************

silver_dq_log_schema = StructType([
    StructField("check_id", LongType(), False),
    StructField("load_id", LongType(), False),
    StructField("run_timestamp", TimestampType(), False),
    StructField("table_name", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("check_dimension", StringType(), False),
    StructField("check_description", StringType(), True),
    StructField("check_expression", StringType(), True),
    StructField("records_checked", LongType(), False),
    StructField("records_passed", LongType(), False),
    StructField("records_failed", LongType(), False),
    StructField("pass_rate", DoubleType(), False),
    StructField("threshold_warn", DoubleType(), True),
    StructField("threshold_fail", DoubleType(), True),
    StructField("status", StringType(), False),
    StructField("failed_sample", StringType(), True)
])

spark.createDataFrame([], silver_dq_log_schema) \
    .write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("_silver_data_quality_log")

print("✅ _silver_data_quality_log table created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 4: Populate 30 configuration records

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime

# Define schema explicitly to match the table
config_schema = StructType([
    StructField("config_id", IntegerType(), False),
    StructField("bronze_table", StringType(), False),
    StructField("silver_table", StringType(), False),
    StructField("category", StringType(), False),
    StructField("load_type", StringType(), False),
    StructField("load_priority", IntegerType(), False),
    StructField("primary_key_columns", StringType(), False),
    StructField("business_key_columns", StringType(), True),
    StructField("watermark_column", StringType(), True),
    StructField("last_watermark", StringType(), True),
    StructField("transformation_notebook", StringType(), False),
    StructField("dq_checks_enabled", BooleanType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False)
])

now = datetime.utcnow()

# Use tuples instead of Row objects for cleaner schema matching
config_data = [
    # REFERENCE TABLES (12)
    (1, "wwi_application_cities", "slv_city", "reference", "full", 10, '["CityID"]', None, None, None, "nb_silver_reference_tables", True, True, now, now),
    (2, "wwi_application_countries", "slv_country", "reference", "full", 10, '["CountryID"]', None, None, None, "nb_silver_reference_tables", True, True, now, now),
    (3, "wwi_application_deliverymethods", "slv_delivery_method", "reference", "full", 10, '["DeliveryMethodID"]', None, None, None, "nb_silver_reference_tables", True, True, now, now),
    (4, "wwi_application_paymentmethods", "slv_payment_method", "reference", "full", 10, '["PaymentMethodID"]', None, None, None, "nb_silver_reference_tables", True, True, now, now),
    (5, "wwi_application_transactiontypes", "slv_transaction_type", "reference", "full", 10, '["TransactionTypeID"]', None, None, None, "nb_silver_reference_tables", True, True, now, now),
    (6, "wwi_sales_buyinggroups", "slv_buying_group", "reference", "full", 10, '["BuyingGroupID"]', None, None, None, "nb_silver_reference_tables", True, True, now, now),
    (7, "wwi_sales_customercategories", "slv_customer_category", "reference", "full", 10, '["CustomerCategoryID"]', None, None, None, "nb_silver_reference_tables", True, True, now, now),
    (8, "wwi_warehouse_colors", "slv_color", "reference", "full", 10, '["ColorID"]', None, None, None, "nb_silver_reference_tables", True, True, now, now),
    (9, "wwi_warehouse_packagetypes", "slv_package_type", "reference", "full", 10, '["PackageTypeID"]', None, None, None, "nb_silver_reference_tables", True, True, now, now),
    (10, "wwi_application_stateprovinces", "slv_state_province", "reference", "full", 10, '["StateProvinceID"]', None, None, None, "nb_silver_reference_tables", True, True, now, now),
    (11, "wwi_purchasing_suppliercategories", "slv_supplier_category", "reference", "full", 10, '["SupplierCategoryID"]', None, None, None, "nb_silver_reference_tables", True, True, now, now),
    (12, "wwi_warehouse_stockgroups", "slv_stock_group", "reference", "full", 10, '["StockGroupID"]', None, None, None, "nb_silver_reference_tables", True, True, now, now),
    # MASTER TABLES (4)
    (13, "wwi_application_people", "slv_person", "master", "full", 20, '["PersonID"]', '["PersonID"]', "_bronze_loaded_at", None, "nb_silver_master_person", True, True, now, now),
    (14, "wwi_sales_customers", "slv_customer", "master", "full", 20, '["CustomerID"]', '["CustomerID"]', "_bronze_loaded_at", None, "nb_silver_master_customer", True, True, now, now),
    (15, "wwi_purchasing_suppliers", "slv_supplier", "master", "full", 20, '["SupplierID"]', '["SupplierID"]', "_bronze_loaded_at", None, "nb_silver_master_supplier", True, True, now, now),
    (16, "wwi_warehouse_stockitems", "slv_stock_item", "master", "full", 20, '["StockItemID"]', '["StockItemID"]', "_bronze_loaded_at", None, "nb_silver_master_stock_item", True, True, now, now),
    # TRANSACTION TABLES (10)
    (17, "wwi_sales_orders", "slv_order", "transaction", "incremental", 30, '["OrderID"]', None, "_bronze_loaded_at", "1900-01-01T00:00:00", "nb_silver_txn_orders", True, True, now, now),
    (18, "wwi_sales_orderlines", "slv_order_line", "transaction", "incremental", 31, '["OrderLineID"]', None, "_bronze_loaded_at", "1900-01-01T00:00:00", "nb_silver_txn_orders", True, True, now, now),
    (19, "wwi_sales_specialdeals", "slv_special_deal", "transaction", "full", 30, '["SpecialDealID"]', None, None, None, "nb_silver_txn_orders", True, True, now, now),
    (20, "wwi_sales_invoices", "slv_invoice", "transaction", "incremental", 32, '["InvoiceID"]', None, "_bronze_loaded_at", "1900-01-01T00:00:00", "nb_silver_txn_invoices", True, True, now, now),
    (21, "wwi_sales_invoicelines", "slv_invoice_line", "transaction", "incremental", 33, '["InvoiceLineID"]', None, "_bronze_loaded_at", "1900-01-01T00:00:00", "nb_silver_txn_invoices", True, True, now, now),
    (22, "wwi_sales_customertransactions", "slv_customer_transaction", "transaction", "incremental", 34, '["CustomerTransactionID"]', None, "_bronze_loaded_at", "1900-01-01T00:00:00", "nb_silver_txn_financials", True, True, now, now),
    (23, "wwi_purchasing_purchaseorders", "slv_purchase_order", "transaction", "incremental", 30, '["PurchaseOrderID"]', None, "_bronze_loaded_at", "1900-01-01T00:00:00", "nb_silver_txn_purchases", True, True, now, now),
    (24, "wwi_purchasing_purchaseorderlines", "slv_purchase_order_line", "transaction", "incremental", 31, '["PurchaseOrderLineID"]', None, "_bronze_loaded_at", "1900-01-01T00:00:00", "nb_silver_txn_purchases", True, True, now, now),
    (25, "wwi_purchasing_suppliertransactions", "slv_supplier_transaction", "transaction", "incremental", 34, '["SupplierTransactionID"]', None, "_bronze_loaded_at", "1900-01-01T00:00:00", "nb_silver_txn_financials", True, True, now, now),
    # INVENTORY TABLES (5)
    (26, "wwi_warehouse_stockitemholdings", "slv_stock_item_holding", "inventory", "full", 40, '["StockItemID"]', None, None, None, "nb_silver_inventory", True, True, now, now),
    (27, "wwi_warehouse_stockitemstockgroups", "slv_stock_item_stock_group", "inventory", "full", 40, '["StockItemStockGroupID"]', None, None, None, "nb_silver_inventory", True, True, now, now),
    (28, "wwi_warehouse_stockitemtransactions", "slv_stock_item_transaction", "inventory", "incremental", 41, '["StockItemTransactionID"]', None, "_bronze_loaded_at", "1900-01-01T00:00:00", "nb_silver_inventory", True, True, now, now),
    (29, "wwi_warehouse_vehicletemperatures", "slv_vehicle_temperature", "inventory", "incremental", 42, '["VehicleTemperatureID"]', None, "_bronze_loaded_at", "1900-01-01T00:00:00", "nb_silver_iot_temperatures", True, True, now, now),
    (30, "wwi_warehouse_coldroomtemperatures", "slv_cold_room_temperature", "inventory", "incremental", 42, '["ColdRoomTemperatureID"]', None, "_bronze_loaded_at", "1900-01-01T00:00:00", "nb_silver_iot_temperatures", True, True, now, now),
]

# Create DataFrame with explicit schema
config_df = spark.createDataFrame(config_data, schema=config_schema)

# Overwrite the table
config_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("_silver_load_config")

print(f"✅ Loaded {config_df.count()} configuration records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 5: Verify setup

# CELL ********************

print("=" * 60)
print("SILVER LAYER CONTROL TABLES SETUP COMPLETE")
print("=" * 60)

for t in ["_silver_load_config", "_silver_load_history", "_silver_data_quality_log"]:
    count = spark.table(t).count()
    print(f"✅ {t}: {count} records")

print("\nConfiguration Summary:")
display(spark.sql("""
    SELECT category, load_type, COUNT(*) as table_count
    FROM _silver_load_config
    WHERE is_active = true
    GROUP BY category, load_type
    ORDER BY category, load_type
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * From _silver_load_config

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
