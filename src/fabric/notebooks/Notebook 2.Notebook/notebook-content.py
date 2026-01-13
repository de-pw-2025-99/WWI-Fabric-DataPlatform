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

# Test connection and load one table
from pyspark.sql.functions import lit, current_timestamp

# Read from SQL Server
source_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://localhost:1433;databaseName=WideWorldImporters") \
    .option("dbtable", "Sales.Customers") \
    .option("user", "YOUR_USER") \
    .option("password", "YOUR_PASSWORD") \
    .load()

print(f"Rows extracted: {source_df.count()}")

# Add metadata
bronze_df = source_df \
    .withColumn("_source_system", lit("wwi_sqlserver")) \
    .withColumn("_source_table", lit("Sales.Customers")) \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_batch_id", lit("test_run_001")) \
    .withColumn("_is_deleted", lit(False))

# Save to Bronze
bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("wwi_sales_customers")

print("Table created successfully!")

# Verify
display(spark.sql("SELECT * FROM wwi_sales_customers LIMIT 5"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
