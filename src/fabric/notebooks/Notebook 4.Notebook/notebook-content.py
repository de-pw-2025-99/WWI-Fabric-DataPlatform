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

df = spark.read.table("test.`Application.Countries`")
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.catalog.listTables());
display(spark.catalog.listTables('test'));


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Returns the current catalog and database/schema
print(f"Current Catalog: {spark.catalog.currentCatalog()}")
print(f"Current Database/Schema: {spark.catalog.currentDatabase()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Configuration ---
target_lakehouse = "test"
dry_run = True  # Set to False to apply changes

# 1. List all tables in the lakehouse
tables = spark.catalog.listTables(target_lakehouse)

print(f"Checking for tables in '{target_lakehouse}' that need renaming...\n")

for t in tables:
    if "." in t.name:
        # Create the new name (e.g., Application.Countries -> Application_Countries)
        new_name = t.name.replace(".", "_")
        
        # We must use backticks for the source because it contains a dot
        old_full_name = f"{target_lakehouse}.`{t.name}`"
        new_full_name = f"{target_lakehouse}.{new_name}"
        
        if dry_run:
            print(f"[DRY RUN] Would rename: {old_full_name} -> {new_full_name}")
        else:
            print(f"Renaming: {t.name} to {new_name}...")
            spark.sql(f"ALTER TABLE {old_full_name} RENAME TO {new_full_name}")

if dry_run:
    print("\nDry run complete. No changes were made.")
else:
    print("\nAll tables successfully renamed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
