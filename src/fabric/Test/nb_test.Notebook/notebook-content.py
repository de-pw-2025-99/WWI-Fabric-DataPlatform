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

from datetime import datetime

tiso = datetime.utcnow().isoformat()
t = datetime.utcnow()


print(t)
print(tiso)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set the schema you want to remove
schema_name = "dbo"

# 1. Get a list of all tables in that schema
tables = spark.sql(f"SHOW TABLES IN {schema_name}").collect()

# 2. Loop through and drop each table
for row in tables:
    table_name = row['tableName']
    print(f"Dropping table: {schema_name}.{table_name}")
    spark.sql(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")

# 3. Finally, drop the empty schema
spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_schema = 'dbo_BrandNew'
target_schema = 'dbo'

tables = spark.sql(f"SHOW TABLES IN {source_schema}").collect()

display(tables)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_schema = 'dbo_BrandNew'
target_schema = 'dbo'

tables = spark.sql(f"SHOW TABLES IN {source_schema}").collect()

# print(tables)

for row in tables:
    table_name = row['tableName']
    df = spark.read.table(f"dbo_BrandNew.{table_name}")
    # 2. Write to target schema
    df.write.format("delta").mode("overwrite").saveAsTable(f"dbo.{table_name}")
    # 3. Delete the old table (optional)
    # spark.sql(f"DROP TABLE dbo_BrandNew.{table_name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
