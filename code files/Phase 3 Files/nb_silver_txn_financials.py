# Databricks notebook source
# MAGIC %md
# MAGIC # nb_silver_txn_financials
# MAGIC 
# MAGIC Transforms Financial Transaction data from Bronze to Silver layer.
# MAGIC 
# MAGIC ## Source Tables
# MAGIC - **wwi_sales_customertransactions** (~101,714 transactions)
# MAGIC - **wwi_purchasing_suppliertransactions** (~6,624 transactions)
# MAGIC 
# MAGIC ## Output Tables
# MAGIC - **slv_customer_transaction**
# MAGIC - **slv_supplier_transaction**

# COMMAND ----------

# MAGIC %run ./nb_silver_utilities

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

pipeline_run_id = dbutils.widgets.get("pipeline_run_id") if "pipeline_run_id" in [w.name for w in dbutils.widgets.getAll()] else None
is_full_load = dbutils.widgets.get("is_full_load") if "is_full_load" in [w.name for w in dbutils.widgets.getAll()] else "true"
is_full_load = is_full_load.lower() == "true"

results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_customer_transaction

# COMMAND ----------

def transform_customer_transactions(is_full: bool = True):
    bronze_table = "wwi_sales_customertransactions"
    silver_table = "slv_customer_transaction"
    
    config = get_config_by_table(bronze_table) if not is_full else None
    last_watermark = config.get("last_watermark", "1900-01-01T00:00:00") if config else "1900-01-01T00:00:00"
    current_watermark = datetime.utcnow().isoformat()
    
    if is_full:
        df = read_bronze_table(bronze_table)
    else:
        df = read_bronze_incremental(bronze_table, "_bronze_loaded_at", last_watermark, current_watermark)
    
    record_count = df.count()
    print(f"Records to process: {record_count:,}")
    
    if record_count == 0:
        return {"table": silver_table, "records": 0, "status": "no_data"}
    
    df = deduplicate_by_key(df, ["CustomerTransactionID"], "_bronze_loaded_at", "last")
    
    df_transformed = df.select(
        col("CustomerTransactionID").alias("customer_transaction_id"),
        col("CustomerID").alias("customer_id"),
        col("TransactionTypeID").alias("transaction_type_id"),
        col("InvoiceID").alias("invoice_id"),
        col("PaymentMethodID").alias("payment_method_id"),
        col("TransactionDate").cast("date").alias("transaction_date"),
        col("AmountExcludingTax").cast("decimal(18,2)").alias("amount_excluding_tax"),
        col("TaxAmount").cast("decimal(18,2)").alias("tax_amount"),
        col("TransactionAmount").cast("decimal(18,2)").alias("transaction_amount"),
        col("OutstandingBalance").cast("decimal(18,2)").alias("outstanding_balance"),
        col("FinalizationDate").cast("date").alias("finalization_date"),
        col("IsFinalized").alias("is_finalized"),
        col("LastEditedBy").alias("last_edited_by"),
        col("LastEditedWhen").alias("last_edited_when"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df_cleaned = clean_all_string_columns(df_transformed)
    
    df_enriched = df_cleaned \
        .withColumn("transaction_year", year(col("transaction_date"))) \
        .withColumn("transaction_month", month(col("transaction_date"))) \
        .withColumn("is_finalized", coalesce(col("is_finalized"), lit(False))) \
        .withColumn("is_payment", col("transaction_amount") < 0) \
        .withColumn("is_invoice", col("invoice_id").isNotNull()) \
        .withColumn("days_to_finalize",
                   when(col("finalization_date").isNotNull(),
                        datediff(col("finalization_date"), col("transaction_date")))
                   .otherwise(lit(None))) \
        .withColumn("effective_tax_rate",
                   when(col("amount_excluding_tax") != 0,
                        round(col("tax_amount") / col("amount_excluding_tax") * 100, 2))
                   .otherwise(lit(0)))
    
    hash_columns = ["customer_transaction_id", "customer_id", "transaction_date", 
                    "transaction_amount", "outstanding_balance", "is_finalized"]
    df_final = add_silver_metadata(df_enriched, bronze_table, pipeline_run_id, hash_columns)
    
    write_mode = "overwrite" if is_full else "append"
    if not is_full:
        records_written = merge_silver_table(df_final, silver_table, ["customer_transaction_id"])
    else:
        records_written = write_silver_table(df_final, silver_table, write_mode,
                                            partition_by=["transaction_year", "transaction_month"])
    
    if config and not is_full:
        update_watermark(config["config_id"], current_watermark)
    
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_customer_transactions(is_full_load))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_supplier_transaction

# COMMAND ----------

def transform_supplier_transactions(is_full: bool = True):
    bronze_table = "wwi_purchasing_suppliertransactions"
    silver_table = "slv_supplier_transaction"
    
    config = get_config_by_table(bronze_table) if not is_full else None
    last_watermark = config.get("last_watermark", "1900-01-01T00:00:00") if config else "1900-01-01T00:00:00"
    current_watermark = datetime.utcnow().isoformat()
    
    if is_full:
        df = read_bronze_table(bronze_table)
    else:
        df = read_bronze_incremental(bronze_table, "_bronze_loaded_at", last_watermark, current_watermark)
    
    record_count = df.count()
    print(f"Records to process: {record_count:,}")
    
    if record_count == 0:
        return {"table": silver_table, "records": 0, "status": "no_data"}
    
    df = deduplicate_by_key(df, ["SupplierTransactionID"], "_bronze_loaded_at", "last")
    
    df_transformed = df.select(
        col("SupplierTransactionID").alias("supplier_transaction_id"),
        col("SupplierID").alias("supplier_id"),
        col("TransactionTypeID").alias("transaction_type_id"),
        col("PurchaseOrderID").alias("purchase_order_id"),
        col("PaymentMethodID").alias("payment_method_id"),
        col("SupplierInvoiceNumber").alias("supplier_invoice_number"),
        col("TransactionDate").cast("date").alias("transaction_date"),
        col("AmountExcludingTax").cast("decimal(18,2)").alias("amount_excluding_tax"),
        col("TaxAmount").cast("decimal(18,2)").alias("tax_amount"),
        col("TransactionAmount").cast("decimal(18,2)").alias("transaction_amount"),
        col("OutstandingBalance").cast("decimal(18,2)").alias("outstanding_balance"),
        col("FinalizationDate").cast("date").alias("finalization_date"),
        col("IsFinalized").alias("is_finalized"),
        col("LastEditedBy").alias("last_edited_by"),
        col("LastEditedWhen").alias("last_edited_when"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df_cleaned = clean_all_string_columns(df_transformed)
    
    df_enriched = df_cleaned \
        .withColumn("transaction_year", year(col("transaction_date"))) \
        .withColumn("transaction_month", month(col("transaction_date"))) \
        .withColumn("is_finalized", coalesce(col("is_finalized"), lit(False))) \
        .withColumn("is_payment", col("transaction_amount") < 0) \
        .withColumn("is_purchase_order", col("purchase_order_id").isNotNull()) \
        .withColumn("days_to_finalize",
                   when(col("finalization_date").isNotNull(),
                        datediff(col("finalization_date"), col("transaction_date")))
                   .otherwise(lit(None)))
    
    hash_columns = ["supplier_transaction_id", "supplier_id", "transaction_date",
                    "transaction_amount", "outstanding_balance", "is_finalized"]
    df_final = add_silver_metadata(df_enriched, bronze_table, pipeline_run_id, hash_columns)
    
    write_mode = "overwrite" if is_full else "append"
    if not is_full:
        records_written = merge_silver_table(df_final, silver_table, ["supplier_transaction_id"])
    else:
        records_written = write_silver_table(df_final, silver_table, write_mode,
                                            partition_by=["transaction_year"])
    
    if config and not is_full:
        update_watermark(config["config_id"], current_watermark)
    
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_supplier_transactions(is_full_load))

# COMMAND ----------

# Summary
print("=" * 70)
print("FINANCIAL TRANSACTION TRANSFORMATIONS COMPLETE")
print("=" * 70)
for r in results:
    status_emoji = "✅" if r["status"] == "success" else "⚠️"
    print(f"{status_emoji} {r['table']}: {r['records']:,} records")

dbutils.notebook.exit(str(results))
