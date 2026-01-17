# Databricks notebook source
# MAGIC %md
# MAGIC # nb_silver_master_supplier
# MAGIC 
# MAGIC Transforms Supplier data from Bronze to Silver layer.
# MAGIC 
# MAGIC ## Source Table
# MAGIC - **wwi_purchasing_suppliers** (~13 suppliers)
# MAGIC 
# MAGIC ## Output Table
# MAGIC - **slv_supplier**

# COMMAND ----------

# MAGIC %run ./nb_silver_utilities

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

pipeline_run_id = dbutils.widgets.get("pipeline_run_id") if "pipeline_run_id" in [w.name for w in dbutils.widgets.getAll()] else None

BRONZE_TABLE = "wwi_purchasing_suppliers"
SILVER_TABLE = "slv_supplier"

# COMMAND ----------

# Read and deduplicate
df_bronze = read_bronze_table(BRONZE_TABLE)
print(f"Bronze records: {df_bronze.count():,}")

df_dedup = deduplicate_by_key(df_bronze, ["SupplierID"], "_bronze_loaded_at", "last")

# COMMAND ----------

# Transform
df_transformed = df_dedup.select(
    col("SupplierID").alias("supplier_id"),
    col("SupplierName").alias("supplier_name"),
    col("SupplierCategoryID").alias("supplier_category_id"),
    col("PrimaryContactPersonID").alias("primary_contact_person_id"),
    col("AlternateContactPersonID").alias("alternate_contact_person_id"),
    col("DeliveryMethodID").alias("delivery_method_id"),
    col("DeliveryCityID").alias("delivery_city_id"),
    col("PostalCityID").alias("postal_city_id"),
    col("SupplierReference").alias("supplier_reference"),
    col("BankAccountName").alias("bank_account_name"),
    col("BankAccountBranch").alias("bank_account_branch"),
    col("BankAccountCode").alias("bank_account_code"),
    col("BankAccountNumber").alias("bank_account_number"),
    col("BankInternationalCode").alias("bank_international_code"),
    col("PaymentDays").alias("payment_days"),
    col("InternalComments").alias("internal_comments"),
    col("PhoneNumber").alias("phone_number"),
    col("FaxNumber").alias("fax_number"),
    col("WebsiteURL").alias("website_url"),
    col("DeliveryAddressLine1").alias("delivery_address_line1"),
    col("DeliveryAddressLine2").alias("delivery_address_line2"),
    col("DeliveryPostalCode").alias("delivery_postal_code"),
    col("PostalAddressLine1").alias("postal_address_line1"),
    col("PostalAddressLine2").alias("postal_address_line2"),
    col("PostalPostalCode").alias("postal_postal_code"),
    col("LastEditedBy").alias("last_edited_by"),
    col("ValidFrom").alias("source_valid_from"),
    col("ValidTo").alias("source_valid_to"),
    col("_bronze_loaded_at"),
    col("_bronze_pipeline_run_id")
)

# Clean and enrich
df_cleaned = clean_all_string_columns(df_transformed)
df_cleaned = standardize_name_column(df_cleaned, "supplier_name")

df_enriched = df_cleaned \
    .withColumn("has_bank_details", 
                col("bank_account_number").isNotNull() & (length(col("bank_account_number")) > 0)) \
    .withColumn("has_website", 
                col("website_url").isNotNull() & (length(col("website_url")) > 0)) \
    .withColumn("full_delivery_address",
                concat_ws(", ", col("delivery_address_line1"), col("delivery_address_line2"), col("delivery_postal_code")))

# Handle nulls
df_with_defaults = df_enriched \
    .withColumn("payment_days", coalesce(col("payment_days"), lit(30)))

# Add Silver metadata
hash_columns = ["supplier_id", "supplier_name", "supplier_category_id", "payment_days"]
df_final = add_silver_metadata(df_with_defaults, BRONZE_TABLE, pipeline_run_id, hash_columns)

# COMMAND ----------

# Write to Silver
records_written = write_silver_table(df_final, SILVER_TABLE, "overwrite")
print(f"✅ Wrote {records_written:,} records to {SILVER_TABLE}")

# COMMAND ----------

# Summary
summary = {"bronze_table": BRONZE_TABLE, "silver_table": SILVER_TABLE, "records_written": records_written, "status": "success"}
dbutils.notebook.exit(str(summary))
