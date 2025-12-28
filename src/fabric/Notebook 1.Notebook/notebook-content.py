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

# Cell 1: Create watermark tracking table
spark.sql('''
    CREATE TABLE IF NOT EXISTS bronze_watermark(
        table_name STRING
        , watermark_column STRING
        , watermark_value TIMESTAMP
        , last_updated timestamp
    )
    USING DELTA
''')
print("Watermark table created!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Test load - Sales.Customers
from pyspark.sql.functions import lit,current_timestamp

# Read from SQL Server using your Fabric connection
source_df = spark.read \
    .format("sqlserver") \
    .option("host", "localhost") \
    .option("port", "1433") \
    .option("database", "WideWorldImporters") \
    .option("dbtable", "Sales.Customers") \
    .option("connectionProvider", "zzzz wwi-sqlserver-onprem") \
    .load()

print(f"✅ Extracted {source_df.count()} rows from Sales.Customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
