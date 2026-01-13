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

df = spark.read.table("test.Sales_Customers")
df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
