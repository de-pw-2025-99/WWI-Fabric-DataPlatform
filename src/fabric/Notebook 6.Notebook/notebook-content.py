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

# MAGIC %%sql
# MAGIC DESCRIBE detail dbo.Application_Countries

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, round

# Replace with your table name
table_name = "dbo.application_countries"

# Get details and convert bytes to GB
details_df = spark.sql(f"DESCRIBE DETAIL {table_name}")
size_stats = details_df.select(
    col("name"),
    col("numFiles"),
    round(col("sizeInBytes") / (1024**2), 2).alias("sizeInMB"),
    round(col("sizeInBytes") / (1024**3), 2).alias("sizeInGB")
)

display(size_stats)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
