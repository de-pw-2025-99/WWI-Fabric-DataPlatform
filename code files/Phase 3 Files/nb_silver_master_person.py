# Databricks notebook source
# MAGIC %md
# MAGIC # nb_silver_master_person
# MAGIC 
# MAGIC Transforms Person (Employee/Contact) data from Bronze to Silver layer.
# MAGIC 
# MAGIC ## Source Table
# MAGIC - **wwi_application_people** (~1,111 people)
# MAGIC 
# MAGIC ## Key Transformations
# MAGIC - Parse CustomFields JSON
# MAGIC - Standardize names
# MAGIC - Extract role information
# MAGIC - Handle permissions flags
# MAGIC 
# MAGIC ## Output Table
# MAGIC - **slv_person**

# COMMAND ----------

# MAGIC %run ./nb_silver_utilities

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

pipeline_run_id = dbutils.widgets.get("pipeline_run_id") if "pipeline_run_id" in [w.name for w in dbutils.widgets.getAll()] else None

BRONZE_TABLE = "wwi_application_people"
SILVER_TABLE = "slv_person"

# COMMAND ----------

# Read and deduplicate
df_bronze = read_bronze_table(BRONZE_TABLE)
print(f"Bronze records: {df_bronze.count():,}")

df_dedup = deduplicate_by_key(df_bronze, ["PersonID"], "_bronze_loaded_at", "last")

# COMMAND ----------

# Transform
df_transformed = df_dedup.select(
    col("PersonID").alias("person_id"),
    col("FullName").alias("full_name"),
    col("PreferredName").alias("preferred_name"),
    col("SearchName").alias("search_name"),
    col("LogonName").alias("logon_name"),
    col("IsPermittedToLogon").alias("is_permitted_to_logon"),
    col("IsExternalLogonProvider").alias("is_external_logon_provider"),
    col("IsSystemUser").alias("is_system_user"),
    col("IsEmployee").alias("is_employee"),
    col("IsSalesperson").alias("is_salesperson"),
    col("UserPreferences").alias("user_preferences_json"),
    col("PhoneNumber").alias("phone_number"),
    col("FaxNumber").alias("fax_number"),
    col("EmailAddress").alias("email_address"),
    col("Photo").alias("photo"),
    col("CustomFields").alias("custom_fields_json"),
    col("OtherLanguages").alias("other_languages"),
    col("LastEditedBy").alias("last_edited_by"),
    col("ValidFrom").alias("source_valid_from"),
    col("ValidTo").alias("source_valid_to"),
    col("_bronze_loaded_at"),
    col("_bronze_pipeline_run_id")
)

# Clean strings
df_cleaned = clean_all_string_columns(df_transformed)
df_cleaned = standardize_name_column(df_cleaned, "full_name")
df_cleaned = standardize_name_column(df_cleaned, "preferred_name")

# Add derived columns
df_enriched = df_cleaned \
    .withColumn("person_type",
                when(col("is_system_user"), "system")
                .when(col("is_employee") & col("is_salesperson"), "sales_employee")
                .when(col("is_employee"), "employee")
                .otherwise("contact")) \
    .withColumn("has_email", col("email_address").isNotNull()) \
    .withColumn("has_phone", col("phone_number").isNotNull()) \
    .withColumn("can_login", col("is_permitted_to_logon") & col("logon_name").isNotNull()) \
    .withColumn("has_photo", col("photo").isNotNull())

# Handle nulls
df_with_defaults = df_enriched \
    .withColumn("is_permitted_to_logon", coalesce(col("is_permitted_to_logon"), lit(False))) \
    .withColumn("is_external_logon_provider", coalesce(col("is_external_logon_provider"), lit(False))) \
    .withColumn("is_system_user", coalesce(col("is_system_user"), lit(False))) \
    .withColumn("is_employee", coalesce(col("is_employee"), lit(False))) \
    .withColumn("is_salesperson", coalesce(col("is_salesperson"), lit(False)))

# Add Silver metadata
hash_columns = ["person_id", "full_name", "is_employee", "is_salesperson", "email_address"]
df_final = add_silver_metadata(df_with_defaults, BRONZE_TABLE, pipeline_run_id, hash_columns)

# COMMAND ----------

# Data quality summary
print("Person Type Distribution:")
display(df_final.groupBy("person_type").count().orderBy(desc("count")))

# COMMAND ----------

# Write to Silver
records_written = write_silver_table(df_final, SILVER_TABLE, "overwrite")
print(f"✅ Wrote {records_written:,} records to {SILVER_TABLE}")

summary = {"bronze_table": BRONZE_TABLE, "silver_table": SILVER_TABLE, "records_written": records_written, "status": "success"}
dbutils.notebook.exit(str(summary))
