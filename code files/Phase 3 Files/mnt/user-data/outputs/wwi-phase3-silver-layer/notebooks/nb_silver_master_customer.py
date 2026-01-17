# Databricks notebook source
# MAGIC %md
# MAGIC # nb_silver_master_customer
# MAGIC 
# MAGIC Transforms Customer data from Bronze to Silver layer.
# MAGIC 
# MAGIC ## Source Table
# MAGIC - **wwi_sales_customers** (~663 customers)
# MAGIC 
# MAGIC ## Key Transformations
# MAGIC 1. Deduplicate on CustomerID
# MAGIC 2. Handle null values for optional fields
# MAGIC 3. Standardize address formatting
# MAGIC 4. Join with reference tables for validation
# MAGIC 5. Add derived fields (customer age, credit tier)
# MAGIC 6. Prepare for potential SCD Type 2 tracking
# MAGIC 
# MAGIC ## Output Table
# MAGIC - **slv_customer**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ./nb_silver_utilities

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, date

# Pipeline parameters
pipeline_run_id = dbutils.widgets.get("pipeline_run_id") if "pipeline_run_id" in [w.name for w in dbutils.widgets.getAll()] else None

# Configuration
BRONZE_TABLE = "wwi_sales_customers"
SILVER_TABLE = "slv_customer"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze Data

# COMMAND ----------

# Read raw customer data from Bronze
df_bronze = read_bronze_table(BRONZE_TABLE)

print(f"Bronze records: {df_bronze.count():,}")
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Analyze Data Quality

# COMMAND ----------

# Check for duplicates
dup_stats = get_duplicate_stats(df_bronze, ["CustomerID"])
print(f"Duplicate Analysis:")
print(f"  Total records: {dup_stats['total_records']:,}")
print(f"  Unique keys: {dup_stats['unique_records']:,}")
print(f"  Duplicates: {dup_stats['duplicate_records']:,}")
print(f"  Duplicate rate: {dup_stats['duplicate_rate']}%")

# COMMAND ----------

# Check null rates for key columns
null_analysis = []
key_columns = [
    "CustomerID", "CustomerName", "CustomerCategoryID", 
    "BuyingGroupID", "DeliveryMethodID", "CreditLimit",
    "AccountOpenedDate", "StandardDiscountPercentage"
]

for col_name in key_columns:
    total, nulls = check_not_null(df_bronze, col_name)
    null_rate = (nulls / total * 100) if total > 0 else 0
    null_analysis.append({
        "column": col_name,
        "total": total,
        "nulls": nulls,
        "null_rate": f"{null_rate:.2f}%"
    })

display(spark.createDataFrame(null_analysis))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Deduplicate

# COMMAND ----------

# Deduplicate keeping the latest record per CustomerID
df_dedup = deduplicate_by_key(
    df_bronze, 
    key_columns=["CustomerID"],
    order_column="_bronze_loaded_at",
    keep="last"
)

print(f"After deduplication: {df_dedup.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Transform and Cleanse

# COMMAND ----------

# Define credit tiers based on credit limit
def get_credit_tier(credit_limit):
    """
    Categorize customers by credit limit.
    Returns: 'Unknown', 'Standard', 'Silver', 'Gold', 'Platinum'
    """
    return when(col(credit_limit).isNull(), "Unknown") \
           .when(col(credit_limit) < 1000, "Standard") \
           .when(col(credit_limit) < 2500, "Silver") \
           .when(col(credit_limit) < 5000, "Gold") \
           .otherwise("Platinum")


# Calculate customer tenure (years since account opened)
def calc_customer_tenure_years():
    """Calculate years since account opened."""
    return floor(datediff(current_date(), col("AccountOpenedDate")) / 365)


# Main transformation
df_transformed = df_dedup.select(
    # Primary Key
    col("CustomerID").alias("customer_id"),
    
    # Business Key
    col("CustomerName").alias("customer_name"),
    
    # Foreign Keys
    col("BillToCustomerID").alias("bill_to_customer_id"),
    col("CustomerCategoryID").alias("customer_category_id"),
    col("BuyingGroupID").alias("buying_group_id"),
    col("PrimaryContactPersonID").alias("primary_contact_person_id"),
    col("AlternateContactPersonID").alias("alternate_contact_person_id"),
    col("DeliveryMethodID").alias("delivery_method_id"),
    col("DeliveryCityID").alias("delivery_city_id"),
    col("PostalCityID").alias("postal_city_id"),
    
    # Financial
    col("CreditLimit").cast("decimal(18,2)").alias("credit_limit"),
    col("StandardDiscountPercentage").cast("decimal(18,3)").alias("standard_discount_percentage"),
    col("PaymentDays").alias("payment_days"),
    
    # Status Flags
    col("IsStatementSent").alias("is_statement_sent"),
    col("IsOnCreditHold").alias("is_on_credit_hold"),
    
    # Dates
    col("AccountOpenedDate").alias("account_opened_date"),
    
    # Contact Information
    col("PhoneNumber").alias("phone_number"),
    col("FaxNumber").alias("fax_number"),
    col("WebsiteURL").alias("website_url"),
    
    # Delivery Address
    col("DeliveryAddressLine1").alias("delivery_address_line1"),
    col("DeliveryAddressLine2").alias("delivery_address_line2"),
    col("DeliveryPostalCode").alias("delivery_postal_code"),
    col("DeliveryRun").alias("delivery_run"),
    col("RunPosition").alias("run_position"),
    
    # Postal Address  
    col("PostalAddressLine1").alias("postal_address_line1"),
    col("PostalAddressLine2").alias("postal_address_line2"),
    col("PostalPostalCode").alias("postal_postal_code"),
    
    # Audit columns from source
    col("LastEditedBy").alias("last_edited_by"),
    col("ValidFrom").alias("source_valid_from"),
    col("ValidTo").alias("source_valid_to"),
    
    # Bronze metadata (keep for lineage)
    col("_bronze_loaded_at"),
    col("_bronze_pipeline_run_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Clean String Columns

# COMMAND ----------

# Clean all string columns (trim whitespace, convert empty to null)
df_cleaned = clean_all_string_columns(df_transformed)

# Standardize name (proper case)
df_cleaned = standardize_name_column(df_cleaned, "customer_name")

print(f"After cleaning: {df_cleaned.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Add Derived Columns

# COMMAND ----------

# Add derived/calculated columns
df_enriched = df_cleaned \
    .withColumn("credit_tier", get_credit_tier("credit_limit")) \
    .withColumn("customer_tenure_years", calc_customer_tenure_years()) \
    .withColumn("has_buying_group", col("buying_group_id").isNotNull()) \
    .withColumn("has_credit_limit", col("credit_limit").isNotNull() & (col("credit_limit") > 0)) \
    .withColumn(
        "full_delivery_address",
        concat_ws(", ",
            col("delivery_address_line1"),
            col("delivery_address_line2"),
            col("delivery_postal_code")
        )
    ) \
    .withColumn(
        "full_postal_address",
        concat_ws(", ",
            col("postal_address_line1"),
            col("postal_address_line2"),
            col("postal_postal_code")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Handle Null Values

# COMMAND ----------

# Replace nulls with appropriate defaults
df_with_defaults = df_enriched \
    .withColumn("credit_limit", coalesce(col("credit_limit"), lit(0.00))) \
    .withColumn("standard_discount_percentage", coalesce(col("standard_discount_percentage"), lit(0.000))) \
    .withColumn("payment_days", coalesce(col("payment_days"), lit(30))) \
    .withColumn("is_statement_sent", coalesce(col("is_statement_sent"), lit(False))) \
    .withColumn("is_on_credit_hold", coalesce(col("is_on_credit_hold"), lit(False))) \
    .withColumn("customer_tenure_years", coalesce(col("customer_tenure_years"), lit(0)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Add Silver Metadata

# COMMAND ----------

# Columns to include in row hash (business columns for change detection)
hash_columns = [
    "customer_id", "customer_name", "customer_category_id", "buying_group_id",
    "delivery_method_id", "delivery_city_id", "credit_limit", 
    "standard_discount_percentage", "payment_days", "is_on_credit_hold",
    "delivery_address_line1", "delivery_postal_code"
]

df_final = add_silver_metadata(
    df_with_defaults, 
    source_table=BRONZE_TABLE,
    pipeline_run_id=pipeline_run_id,
    columns_to_hash=hash_columns
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Data Quality Checks

# COMMAND ----------

# Run quality checks
quality_results = []

# Check 1: Primary key not null
pk_check = run_quality_check(
    df_final,
    check_name="customer_id_not_null",
    check_dimension="completeness",
    check_func=lambda: check_not_null(df_final, "customer_id"),
    threshold_warn=1.0,
    threshold_fail=1.0
)
quality_results.append(pk_check)

# Check 2: Primary key unique
unique_check = run_quality_check(
    df_final,
    check_name="customer_id_unique", 
    check_dimension="uniqueness",
    check_func=lambda: check_unique(df_final, ["customer_id"]),
    threshold_warn=1.0,
    threshold_fail=1.0
)
quality_results.append(unique_check)

# Check 3: Customer name not null
name_check = run_quality_check(
    df_final,
    check_name="customer_name_not_null",
    check_dimension="completeness",
    check_func=lambda: check_not_null(df_final, "customer_name"),
    threshold_warn=1.0,
    threshold_fail=0.99
)
quality_results.append(name_check)

# Check 4: Valid credit limit (>= 0)
credit_check_df = df_final.filter(col("credit_limit") < 0)
credit_check = {
    "check_name": "credit_limit_non_negative",
    "check_dimension": "validity",
    "records_checked": df_final.count(),
    "records_passed": df_final.count() - credit_check_df.count(),
    "records_failed": credit_check_df.count(),
    "pass_rate": round((df_final.count() - credit_check_df.count()) / df_final.count(), 4),
    "threshold_warn": 1.0,
    "threshold_fail": 1.0,
    "status": "PASS" if credit_check_df.count() == 0 else "FAIL"
}
quality_results.append(credit_check)

# Display quality results
print("Data Quality Check Results:")
print("-" * 60)
for check in quality_results:
    status_emoji = "✅" if check["status"] == "PASS" else "⚠️" if check["status"] == "WARN" else "❌"
    print(f"{status_emoji} {check['check_name']}: {check['status']} (Pass rate: {check['pass_rate']:.2%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Referential Integrity Checks

# COMMAND ----------

# Load reference tables for FK validation
df_customer_category = spark.table("slv_customer_category")
df_delivery_method = spark.table("slv_delivery_method")
df_buying_group = spark.table("slv_buying_group")
df_city = spark.table("slv_city")

# Check customer_category_id
ri_category = run_quality_check(
    df_final,
    check_name="customer_category_fk_valid",
    check_dimension="referential",
    check_func=lambda: check_referential_integrity(
        df_final, "customer_category_id", df_customer_category, "customer_category_id"
    ),
    threshold_warn=1.0,
    threshold_fail=0.99
)
print(f"Customer Category FK: {ri_category['status']} ({ri_category['records_failed']} orphans)")

# Check delivery_method_id
ri_delivery = run_quality_check(
    df_final,
    check_name="delivery_method_fk_valid",
    check_dimension="referential",
    check_func=lambda: check_referential_integrity(
        df_final, "delivery_method_id", df_delivery_method, "delivery_method_id"
    ),
    threshold_warn=1.0,
    threshold_fail=0.99
)
print(f"Delivery Method FK: {ri_delivery['status']} ({ri_delivery['records_failed']} orphans)")

# Check buying_group_id (nullable - only check non-null values)
df_with_buying_group = df_final.filter(col("buying_group_id").isNotNull())
if df_with_buying_group.count() > 0:
    ri_buying = run_quality_check(
        df_with_buying_group,
        check_name="buying_group_fk_valid",
        check_dimension="referential",
        check_func=lambda: check_referential_integrity(
            df_with_buying_group, "buying_group_id", df_buying_group, "buying_group_id"
        ),
        threshold_warn=1.0,
        threshold_fail=0.99
    )
    print(f"Buying Group FK: {ri_buying['status']} ({ri_buying['records_failed']} orphans)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Final Schema Verification

# COMMAND ----------

# Verify final schema
print("Final Schema:")
df_final.printSchema()

# Show sample
print("\nSample Records:")
display(df_final.select(
    "customer_id", "customer_name", "customer_category_id",
    "credit_limit", "credit_tier", "customer_tenure_years",
    "delivery_city_id", "_silver_loaded_at"
).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Write to Silver

# COMMAND ----------

# Write to Silver layer
records_written = write_silver_table(
    df_final,
    table_name=SILVER_TABLE,
    mode="overwrite"
)

print(f"✅ Successfully wrote {records_written:,} records to {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13: Post-Load Verification

# COMMAND ----------

# Verify the data was written correctly
df_verify = spark.table(SILVER_TABLE)

print("Post-Load Verification:")
print(f"  Total records: {df_verify.count():,}")
print(f"  Unique customer_ids: {df_verify.select('customer_id').distinct().count():,}")
print(f"  Credit Tier Distribution:")

# Credit tier distribution
display(df_verify.groupBy("credit_tier").count().orderBy("credit_tier"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Prepare summary
summary = {
    "bronze_table": BRONZE_TABLE,
    "silver_table": SILVER_TABLE,
    "records_read": df_bronze.count(),
    "records_written": records_written,
    "quality_checks_passed": sum(1 for q in quality_results if q["status"] == "PASS"),
    "quality_checks_total": len(quality_results),
    "processing_timestamp": datetime.utcnow().isoformat()
}

print("=" * 70)
print("CUSTOMER TRANSFORMATION COMPLETE")
print("=" * 70)
print(f"Bronze Source: {summary['bronze_table']}")
print(f"Silver Target: {summary['silver_table']}")
print(f"Records Read: {summary['records_read']:,}")
print(f"Records Written: {summary['records_written']:,}")
print(f"Quality Checks: {summary['quality_checks_passed']}/{summary['quality_checks_total']} passed")
print("=" * 70)

# Return for pipeline
dbutils.notebook.exit(str(summary))
