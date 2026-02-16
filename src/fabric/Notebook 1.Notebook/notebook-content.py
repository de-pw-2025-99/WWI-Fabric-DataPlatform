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
# MAGIC -- ALTER TABLE dbo.wwi_application_cities
# MAGIC -- ADD COLUMN _bronze_loaded_at TIMESTAMP 
# MAGIC 
# MAGIC -- Ensure you are using the correct catalog and schema
# MAGIC UPDATE wwi_application_cities
# MAGIC SET _bronze_loaded_at = CAST('2013-01-01 10:00:00' AS TIMESTAMP);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import lit

# Load the Delta table
deltaTable = DeltaTable.forName(spark, "dbo.wwi_application_cities")

# Update the column
deltaTable.update(
    condition = None, # Update all rows
    set = { "_bronze_load_at": lit("2013-01-01 10:00:00").cast("timestamp") }
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_bronze.dbo.wwi_application_cities LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
