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

from pyspark.sql.functions import col, current_timestamp, lit, unix_timestamp
from datetime import datetime, timedelta

# 1. Configuration
schema_name = "dbo"
hours_threshold = 24  # Find tables not updated in the last 24 hours

# 2. Get all tables in the schema
tables = spark.catalog.listTables(schema_name)
stale_tables = []

for t in tables:
    if t.tableType != 'VIEW':
        # Get the last modification time from Delta metadata
        details = spark.sql(f"DESCRIBE DETAIL {schema_name}.{t.name}").collect()[0]
        last_mod = details['lastModified']
        
        # Calculate if the table is older than our threshold
        if last_mod < (datetime.now() - timedelta(hours=hours_threshold)):
            stale_tables.append({
                "TableName": t.name,
                "LastModified": last_mod,
                "HoursSinceUpdate": round((datetime.now() - last_mod).total_seconds() / 3600, 2)
            })

# 3. Create DataFrame and display
if stale_tables:
    df_stale = spark.createDataFrame(stale_tables)
    display(df_stale.orderBy(col("HoursSinceUpdate").desc()))
else:
    print(f"✅ All tables in '{schema_name}' have been updated within the last {hours_threshold} hours.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
