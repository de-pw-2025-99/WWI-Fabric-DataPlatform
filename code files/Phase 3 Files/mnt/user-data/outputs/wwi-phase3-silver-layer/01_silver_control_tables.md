# Silver Layer Control Tables

## Overview

Unlike Bronze (which used SQL Server for control tables), Silver layer control tables are **Delta tables in lh_silver**. This keeps everything within Fabric and simplifies the architecture.

---

## Control Table Definitions

### 1. _silver_load_config

Metadata-driven configuration for Silver transformations.

```python
# Run this in a notebook attached to lh_silver

from pyspark.sql.types import *
from delta.tables import DeltaTable

# Define schema
silver_load_config_schema = StructType([
    StructField("config_id", IntegerType(), False),
    StructField("bronze_table", StringType(), False),
    StructField("silver_table", StringType(), False),
    StructField("category", StringType(), False),  # reference, master, transaction, inventory
    StructField("load_type", StringType(), False),  # full, incremental, scd2
    StructField("load_priority", IntegerType(), False),
    StructField("primary_key_columns", StringType(), False),  # JSON array
    StructField("business_key_columns", StringType(), True),  # JSON array (for SCD)
    StructField("watermark_column", StringType(), True),
    StructField("last_watermark", StringType(), True),
    StructField("transformation_notebook", StringType(), False),
    StructField("dq_checks_enabled", BooleanType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False)
])

# Create empty table
spark.createDataFrame([], silver_load_config_schema) \
    .write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("_silver_load_config")
```

### 2. _silver_load_history

Audit trail for Silver layer loads.

```python
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
    StructField("status", StringType(), False),  # Running, Succeeded, Failed
    StructField("error_message", StringType(), True)
])

spark.createDataFrame([], silver_load_history_schema) \
    .write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("_silver_load_history")
```

### 3. _silver_data_quality_log

Quality check results for monitoring and alerting.

```python
silver_dq_log_schema = StructType([
    StructField("check_id", LongType(), False),
    StructField("load_id", LongType(), False),
    StructField("run_timestamp", TimestampType(), False),
    StructField("table_name", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("check_dimension", StringType(), False),  # completeness, uniqueness, validity, consistency, referential
    StructField("check_description", StringType(), True),
    StructField("check_expression", StringType(), True),
    StructField("records_checked", LongType(), False),
    StructField("records_passed", LongType(), False),
    StructField("records_failed", LongType(), False),
    StructField("pass_rate", DoubleType(), False),
    StructField("threshold_warn", DoubleType(), True),
    StructField("threshold_fail", DoubleType(), True),
    StructField("status", StringType(), False),  # PASS, WARN, FAIL
    StructField("failed_sample", StringType(), True)  # JSON sample of failed records
])

spark.createDataFrame([], silver_dq_log_schema) \
    .write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("_silver_data_quality_log")
```

---

## Populate _silver_load_config

```python
from pyspark.sql import Row
from datetime import datetime
import json

# Configuration data
config_data = [
    # ========== CATEGORY 1: REFERENCE TABLES ==========
    Row(config_id=1, bronze_table="wwi_application_cities", silver_table="slv_city", 
        category="reference", load_type="full", load_priority=10,
        primary_key_columns='["CityID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_reference_tables", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=2, bronze_table="wwi_application_countries", silver_table="slv_country",
        category="reference", load_type="full", load_priority=10,
        primary_key_columns='["CountryID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_reference_tables", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=3, bronze_table="wwi_application_deliverymethods", silver_table="slv_delivery_method",
        category="reference", load_type="full", load_priority=10,
        primary_key_columns='["DeliveryMethodID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_reference_tables", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=4, bronze_table="wwi_application_paymentmethods", silver_table="slv_payment_method",
        category="reference", load_type="full", load_priority=10,
        primary_key_columns='["PaymentMethodID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_reference_tables", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=5, bronze_table="wwi_application_transactiontypes", silver_table="slv_transaction_type",
        category="reference", load_type="full", load_priority=10,
        primary_key_columns='["TransactionTypeID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_reference_tables", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=6, bronze_table="wwi_sales_buyinggroups", silver_table="slv_buying_group",
        category="reference", load_type="full", load_priority=10,
        primary_key_columns='["BuyingGroupID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_reference_tables", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=7, bronze_table="wwi_sales_customercategories", silver_table="slv_customer_category",
        category="reference", load_type="full", load_priority=10,
        primary_key_columns='["CustomerCategoryID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_reference_tables", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=8, bronze_table="wwi_warehouse_colors", silver_table="slv_color",
        category="reference", load_type="full", load_priority=10,
        primary_key_columns='["ColorID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_reference_tables", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=9, bronze_table="wwi_warehouse_packagetypes", silver_table="slv_package_type",
        category="reference", load_type="full", load_priority=10,
        primary_key_columns='["PackageTypeID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_reference_tables", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),

    # ========== CATEGORY 2: MASTER TABLES ==========
    Row(config_id=10, bronze_table="wwi_application_people", silver_table="slv_person",
        category="master", load_type="full", load_priority=20,
        primary_key_columns='["PersonID"]', business_key_columns='["PersonID"]',
        watermark_column="_bronze_loaded_at", last_watermark=None,
        transformation_notebook="nb_silver_master_person", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=11, bronze_table="wwi_application_stateprovinces", silver_table="slv_state_province",
        category="master", load_type="full", load_priority=20,
        primary_key_columns='["StateProvinceID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_reference_tables", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=12, bronze_table="wwi_sales_customers", silver_table="slv_customer",
        category="master", load_type="full", load_priority=20,
        primary_key_columns='["CustomerID"]', business_key_columns='["CustomerID"]',
        watermark_column="_bronze_loaded_at", last_watermark=None,
        transformation_notebook="nb_silver_master_customer", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=13, bronze_table="wwi_purchasing_suppliers", silver_table="slv_supplier",
        category="master", load_type="full", load_priority=20,
        primary_key_columns='["SupplierID"]', business_key_columns='["SupplierID"]',
        watermark_column="_bronze_loaded_at", last_watermark=None,
        transformation_notebook="nb_silver_master_supplier", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=14, bronze_table="wwi_purchasing_suppliercategories", silver_table="slv_supplier_category",
        category="master", load_type="full", load_priority=20,
        primary_key_columns='["SupplierCategoryID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_reference_tables", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=15, bronze_table="wwi_warehouse_stockitems", silver_table="slv_stock_item",
        category="master", load_type="full", load_priority=20,
        primary_key_columns='["StockItemID"]', business_key_columns='["StockItemID"]',
        watermark_column="_bronze_loaded_at", last_watermark=None,
        transformation_notebook="nb_silver_master_stock_item", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=16, bronze_table="wwi_warehouse_stockgroups", silver_table="slv_stock_group",
        category="master", load_type="full", load_priority=20,
        primary_key_columns='["StockGroupID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_reference_tables", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),

    # ========== CATEGORY 3: TRANSACTION TABLES ==========
    Row(config_id=17, bronze_table="wwi_sales_orders", silver_table="slv_order",
        category="transaction", load_type="incremental", load_priority=30,
        primary_key_columns='["OrderID"]', business_key_columns=None,
        watermark_column="_bronze_loaded_at", last_watermark="1900-01-01T00:00:00",
        transformation_notebook="nb_silver_txn_orders", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=18, bronze_table="wwi_sales_orderlines", silver_table="slv_order_line",
        category="transaction", load_type="incremental", load_priority=31,
        primary_key_columns='["OrderLineID"]', business_key_columns=None,
        watermark_column="_bronze_loaded_at", last_watermark="1900-01-01T00:00:00",
        transformation_notebook="nb_silver_txn_orders", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=19, bronze_table="wwi_sales_invoices", silver_table="slv_invoice",
        category="transaction", load_type="incremental", load_priority=30,
        primary_key_columns='["InvoiceID"]', business_key_columns=None,
        watermark_column="_bronze_loaded_at", last_watermark="1900-01-01T00:00:00",
        transformation_notebook="nb_silver_txn_invoices", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=20, bronze_table="wwi_sales_invoicelines", silver_table="slv_invoice_line",
        category="transaction", load_type="incremental", load_priority=31,
        primary_key_columns='["InvoiceLineID"]', business_key_columns=None,
        watermark_column="_bronze_loaded_at", last_watermark="1900-01-01T00:00:00",
        transformation_notebook="nb_silver_txn_invoices", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=21, bronze_table="wwi_sales_customertransactions", silver_table="slv_customer_transaction",
        category="transaction", load_type="incremental", load_priority=32,
        primary_key_columns='["CustomerTransactionID"]', business_key_columns=None,
        watermark_column="_bronze_loaded_at", last_watermark="1900-01-01T00:00:00",
        transformation_notebook="nb_silver_txn_financials", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=22, bronze_table="wwi_sales_specialdeals", silver_table="slv_special_deal",
        category="transaction", load_type="full", load_priority=30,
        primary_key_columns='["SpecialDealID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_txn_orders", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=23, bronze_table="wwi_purchasing_purchaseorders", silver_table="slv_purchase_order",
        category="transaction", load_type="incremental", load_priority=30,
        primary_key_columns='["PurchaseOrderID"]', business_key_columns=None,
        watermark_column="_bronze_loaded_at", last_watermark="1900-01-01T00:00:00",
        transformation_notebook="nb_silver_txn_purchases", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=24, bronze_table="wwi_purchasing_purchaseorderlines", silver_table="slv_purchase_order_line",
        category="transaction", load_type="incremental", load_priority=31,
        primary_key_columns='["PurchaseOrderLineID"]', business_key_columns=None,
        watermark_column="_bronze_loaded_at", last_watermark="1900-01-01T00:00:00",
        transformation_notebook="nb_silver_txn_purchases", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=25, bronze_table="wwi_purchasing_suppliertransactions", silver_table="slv_supplier_transaction",
        category="transaction", load_type="incremental", load_priority=32,
        primary_key_columns='["SupplierTransactionID"]', business_key_columns=None,
        watermark_column="_bronze_loaded_at", last_watermark="1900-01-01T00:00:00",
        transformation_notebook="nb_silver_txn_financials", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),

    # ========== CATEGORY 4: INVENTORY TABLES ==========
    Row(config_id=26, bronze_table="wwi_warehouse_stockitemholdings", silver_table="slv_stock_item_holding",
        category="inventory", load_type="full", load_priority=40,
        primary_key_columns='["StockItemID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_inventory", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=27, bronze_table="wwi_warehouse_stockitemstockgroups", silver_table="slv_stock_item_stock_group",
        category="inventory", load_type="full", load_priority=40,
        primary_key_columns='["StockItemStockGroupID"]', business_key_columns=None,
        watermark_column=None, last_watermark=None,
        transformation_notebook="nb_silver_inventory", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=28, bronze_table="wwi_warehouse_stockitemtransactions", silver_table="slv_stock_item_transaction",
        category="inventory", load_type="incremental", load_priority=41,
        primary_key_columns='["StockItemTransactionID"]', business_key_columns=None,
        watermark_column="_bronze_loaded_at", last_watermark="1900-01-01T00:00:00",
        transformation_notebook="nb_silver_inventory", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=29, bronze_table="wwi_warehouse_vehicletemperatures", silver_table="slv_vehicle_temperature",
        category="inventory", load_type="incremental", load_priority=42,
        primary_key_columns='["VehicleTemperatureID"]', business_key_columns=None,
        watermark_column="_bronze_loaded_at", last_watermark="1900-01-01T00:00:00",
        transformation_notebook="nb_silver_iot_temperatures", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
    
    Row(config_id=30, bronze_table="wwi_warehouse_coldroomtemperatures", silver_table="slv_cold_room_temperature",
        category="inventory", load_type="incremental", load_priority=42,
        primary_key_columns='["ColdRoomTemperatureID"]', business_key_columns=None,
        watermark_column="_bronze_loaded_at", last_watermark="1900-01-01T00:00:00",
        transformation_notebook="nb_silver_iot_temperatures", dq_checks_enabled=True, is_active=True,
        created_at=datetime.utcnow(), updated_at=datetime.utcnow()),
]

# Create DataFrame and write to table
config_df = spark.createDataFrame(config_data)
config_df.write.format("delta").mode("overwrite").saveAsTable("_silver_load_config")

print(f"Loaded {config_df.count()} configuration records")
display(spark.table("_silver_load_config").orderBy("config_id"))
```

---

## Verify Configuration

```python
# Summary by category
spark.sql("""
    SELECT 
        category,
        load_type,
        COUNT(*) as table_count
    FROM _silver_load_config
    WHERE is_active = true
    GROUP BY category, load_type
    ORDER BY category, load_type
""").display()

# List all active tables
spark.sql("""
    SELECT 
        config_id,
        bronze_table,
        silver_table,
        category,
        load_type,
        transformation_notebook
    FROM _silver_load_config
    WHERE is_active = true
    ORDER BY category, load_priority, config_id
""").display()
```

---

## Next: Create Utility Notebook

After running this setup, proceed to `nb_silver_utilities.py` for common transformation functions.
