# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f4ac55a7-ed20-4139-8a6b-6c40b9eb157d",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "4ff4e458-6d35-4bf0-944e-bb512481f096",
# META       "known_lakehouses": [
# META         {
# META           "id": "f4ac55a7-ed20-4139-8a6b-6c40b9eb157d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Create Bronze ingestion configuration table
from pyspark.sql.types import *

schema = StructType([
    StructField("source_schema", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("bronze_table", StringType(), False),
    StructField("load_type", StringType(), False),  # "full" or "incremental"
    StructField("watermark_column", StringType(), True),
    StructField("is_active", BooleanType(), False),
    StructField("priority", IntegerType(), False)
])

data = [
    # Full load tables (dimensions/reference)
    ("Sales", "Customers", "wwi_sales_customers", "full", None, True, 2),
    ("Sales", "CustomerCategories", "wwi_sales_customercategories", "full", None, True, 2),
    ("Sales", "BuyingGroups", "wwi_sales_buyinggroups", "full", None, True, 2),
    ("Application", "People", "wwi_application_people", "full", None, True, 2),
    ("Application", "Cities", "wwi_application_cities", "full", None, True, 2),
    ("Application", "StateProvinces", "wwi_application_stateprovinces", "full", None, True, 2),
    ("Application", "Countries", "wwi_application_countries", "full", None, True, 2),
    ("Application", "PaymentMethods", "wwi_application_paymentmethods", "full", None, True, 3),
    ("Application", "DeliveryMethods", "wwi_application_deliverymethods", "full", None, True, 3),
    ("Application", "TransactionTypes", "wwi_application_transactiontypes", "full", None, True, 3),
    ("Warehouse", "StockItems", "wwi_warehouse_stockitems", "full", None, True, 2),
    ("Warehouse", "Colors", "wwi_warehouse_colors", "full", None, True, 3),
    ("Warehouse", "PackageTypes", "wwi_warehouse_packagetypes", "full", None, True, 3),
    ("Warehouse", "StockGroups", "wwi_warehouse_stockgroups", "full", None, True, 3),
    ("Purchasing", "Suppliers", "wwi_purchasing_suppliers", "full", None, True, 2),
    ("Purchasing", "SupplierCategories", "wwi_purchasing_suppliercategories", "full", None, True, 3),
    
    # Incremental load tables (facts/transactions)
    ("Sales", "Orders", "wwi_sales_orders", "incremental", "LastEditedWhen", True, 1),
    ("Sales", "OrderLines", "wwi_sales_orderlines", "incremental", "LastEditedWhen", True, 1),
    ("Sales", "Invoices", "wwi_sales_invoices", "incremental", "LastEditedWhen", True, 1),
    ("Sales", "InvoiceLines", "wwi_sales_invoicelines", "incremental", "LastEditedWhen", True, 1),
    ("Sales", "CustomerTransactions", "wwi_sales_customertransactions", "incremental", "LastEditedWhen", True, 1),
    ("Purchasing", "PurchaseOrders", "wwi_purchasing_purchaseorders", "incremental", "LastEditedWhen", True, 1),
    ("Purchasing", "PurchaseOrderLines", "wwi_purchasing_purchaseorderlines", "incremental", "LastEditedWhen", True, 1),
    ("Purchasing", "SupplierTransactions", "wwi_purchasing_suppliertransactions", "incremental", "LastEditedWhen", True, 1),
    ("Warehouse", "StockItemTransactions", "wwi_warehouse_stockitemtransactions", "incremental", "LastEditedWhen", True, 1)
]

df = spark.createDataFrame(data, schema)
df.write.format("delta").mode("overwrite").saveAsTable("bronze_config")

print("✅ Created bronze_config table")
display(spark.sql("SELECT * FROM bronze_config ORDER BY load_type, priority, source_table"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# Create watermark table if not exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze_watermark (
        table_name STRING,
        watermark_column STRING,
        watermark_value TIMESTAMP,
        last_updated TIMESTAMP
    )
    USING DELTA
""")
print("✅ Watermark table ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''
Ah, Fabric Lakehouse Lookup doesn't support direct queries like Azure Data Factory does. Let's work around this.
Workaround: Create Views for Config
Option A: Create Filtered Views in Lakehouse
In your notebook, create views that the Lookup can read:
'''

spark.sql("""
    CREATE OR REPLACE VIEW v_bronze_config_full_load AS
    SELECT source_schema, source_table, bronze_table, load_type, watermark_column
    FROM bronze_config
    WHERE load_type = 'full' AND is_active = 1
""")

spark.sql("""
    CREATE OR REPLACE VIEW v_bronze_config_incremental AS
    SELECT source_schema, source_table, bronze_table, load_type, watermark_column
    FROM bronze_config
    WHERE load_type = 'incremental' AND is_active = true
""")

print("Views Created")
display(spark.sql("SELECT * FROM v_bronze_config_full_load"))
display(spark.sql("SELECT * FROM v_bronze_config_incremental"))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
