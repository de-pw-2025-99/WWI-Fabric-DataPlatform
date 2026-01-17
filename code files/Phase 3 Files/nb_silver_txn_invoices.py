# Databricks notebook source
# MAGIC %md
# MAGIC # nb_silver_txn_invoices
# MAGIC 
# MAGIC Transforms Invoice data from Bronze to Silver layer.
# MAGIC 
# MAGIC ## Source Tables
# MAGIC - **wwi_sales_invoices** (~70,510 invoices)
# MAGIC - **wwi_sales_invoicelines** (~228,265 lines)
# MAGIC 
# MAGIC ## Output Tables
# MAGIC - **slv_invoice**
# MAGIC - **slv_invoice_line**

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
# MAGIC ## Transform: slv_invoice

# COMMAND ----------

def transform_invoices(is_full: bool = True):
    bronze_table = "wwi_sales_invoices"
    silver_table = "slv_invoice"
    
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
    
    df = deduplicate_by_key(df, ["InvoiceID"], "_bronze_loaded_at", "last")
    
    df_transformed = df.select(
        col("InvoiceID").alias("invoice_id"),
        col("CustomerID").alias("customer_id"),
        col("BillToCustomerID").alias("bill_to_customer_id"),
        col("OrderID").alias("order_id"),
        col("DeliveryMethodID").alias("delivery_method_id"),
        col("ContactPersonID").alias("contact_person_id"),
        col("AccountsPersonID").alias("accounts_person_id"),
        col("SalespersonPersonID").alias("salesperson_person_id"),
        col("PackedByPersonID").alias("packed_by_person_id"),
        col("InvoiceDate").cast("date").alias("invoice_date"),
        col("CustomerPurchaseOrderNumber").alias("customer_purchase_order_number"),
        col("IsCreditNote").alias("is_credit_note"),
        col("CreditNoteReason").alias("credit_note_reason"),
        col("Comments").alias("comments"),
        col("DeliveryInstructions").alias("delivery_instructions"),
        col("InternalComments").alias("internal_comments"),
        col("TotalDryItems").alias("total_dry_items"),
        col("TotalChillerItems").alias("total_chiller_items"),
        col("DeliveryRun").alias("delivery_run"),
        col("RunPosition").alias("run_position"),
        col("ReturnedDeliveryData").alias("returned_delivery_data_json"),
        col("ConfirmedDeliveryTime").alias("confirmed_delivery_time"),
        col("ConfirmedReceivedBy").alias("confirmed_received_by"),
        col("LastEditedBy").alias("last_edited_by"),
        col("LastEditedWhen").alias("last_edited_when"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df_cleaned = clean_all_string_columns(df_transformed)
    
    df_enriched = df_cleaned \
        .withColumn("invoice_year", year(col("invoice_date"))) \
        .withColumn("invoice_month", month(col("invoice_date"))) \
        .withColumn("total_items", col("total_dry_items") + col("total_chiller_items")) \
        .withColumn("has_chiller_items", col("total_chiller_items") > 0) \
        .withColumn("is_delivered", col("confirmed_delivery_time").isNotNull())
    
    df_with_defaults = df_enriched \
        .withColumn("is_credit_note", coalesce(col("is_credit_note"), lit(False))) \
        .withColumn("total_dry_items", coalesce(col("total_dry_items"), lit(0))) \
        .withColumn("total_chiller_items", coalesce(col("total_chiller_items"), lit(0)))
    
    hash_columns = ["invoice_id", "customer_id", "invoice_date", "is_credit_note", "total_items"]
    df_final = add_silver_metadata(df_with_defaults, bronze_table, pipeline_run_id, hash_columns)
    
    write_mode = "overwrite" if is_full else "append"
    if not is_full:
        records_written = merge_silver_table(df_final, silver_table, ["invoice_id"])
    else:
        records_written = write_silver_table(df_final, silver_table, write_mode,
                                            partition_by=["invoice_year", "invoice_month"])
    
    if config and not is_full:
        update_watermark(config["config_id"], current_watermark)
    
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_invoices(is_full_load))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: slv_invoice_line

# COMMAND ----------

def transform_invoice_lines(is_full: bool = True):
    bronze_table = "wwi_sales_invoicelines"
    silver_table = "slv_invoice_line"
    
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
    
    df = deduplicate_by_key(df, ["InvoiceLineID"], "_bronze_loaded_at", "last")
    
    df_transformed = df.select(
        col("InvoiceLineID").alias("invoice_line_id"),
        col("InvoiceID").alias("invoice_id"),
        col("StockItemID").alias("stock_item_id"),
        col("Description").alias("description"),
        col("PackageTypeID").alias("package_type_id"),
        col("Quantity").cast("int").alias("quantity"),
        col("UnitPrice").cast("decimal(18,2)").alias("unit_price"),
        col("TaxRate").cast("decimal(18,3)").alias("tax_rate"),
        col("TaxAmount").cast("decimal(18,2)").alias("tax_amount"),
        col("LineProfit").cast("decimal(18,2)").alias("line_profit"),
        col("ExtendedPrice").cast("decimal(18,2)").alias("extended_price"),
        col("LastEditedBy").alias("last_edited_by"),
        col("LastEditedWhen").alias("last_edited_when"),
        col("_bronze_loaded_at"),
        col("_bronze_pipeline_run_id")
    )
    
    df_cleaned = clean_all_string_columns(df_transformed)
    
    # Calculated columns with validation
    df_enriched = df_cleaned \
        .withColumn("calculated_extended_price", round(col("quantity") * col("unit_price"), 2)) \
        .withColumn("calculated_tax", round(col("quantity") * col("unit_price") * col("tax_rate") / 100, 2)) \
        .withColumn("line_total_with_tax", round(col("extended_price") + col("tax_amount"), 2)) \
        .withColumn("profit_margin_pct", 
                   when(col("extended_price") > 0, 
                        round(col("line_profit") / col("extended_price") * 100, 2))
                   .otherwise(lit(0)))
    
    hash_columns = ["invoice_line_id", "invoice_id", "stock_item_id", "quantity", "unit_price", "extended_price"]
    df_final = add_silver_metadata(df_enriched, bronze_table, pipeline_run_id, hash_columns)
    
    write_mode = "overwrite" if is_full else "append"
    if not is_full:
        records_written = merge_silver_table(df_final, silver_table, ["invoice_line_id"])
    else:
        records_written = write_silver_table(df_final, silver_table, write_mode)
    
    if config and not is_full:
        update_watermark(config["config_id"], current_watermark)
    
    return {"table": silver_table, "records": records_written, "status": "success"}

results.append(transform_invoice_lines(is_full_load))

# COMMAND ----------

# Summary
print("=" * 70)
print("INVOICE TRANSFORMATIONS COMPLETE")
print("=" * 70)
for r in results:
    status_emoji = "✅" if r["status"] == "success" else "⚠️"
    print(f"{status_emoji} {r['table']}: {r['records']:,} records")

dbutils.notebook.exit(str(results))
