# Phase 3 Implementation Guide: Silver Layer Step-by-Step

This guide walks you through implementing the Silver layer transformations in Microsoft Fabric.

---

## Prerequisites Checklist

Before starting, ensure you have:

- [ ] Phase 2 complete - all 30 Bronze tables loaded in `lh_bronze`
- [ ] `lh_silver` lakehouse created in your Fabric workspace
- [ ] Fabric workspace with notebook capability
- [ ] Downloaded all Phase 3 files from this package

---

## Implementation Steps

### Step 1: Create Control Tables in lh_silver

**Time: ~10 minutes**

1. Open your Fabric workspace
2. Navigate to `lh_silver` lakehouse
3. Click **Open notebook** → **New notebook**
4. Name it: `00_setup_silver_control_tables`
5. Attach it to `lh_silver` (important!)
6. Copy the code from `01_silver_control_tables.md` - specifically:
   - The schema definitions for `_silver_load_config`
   - The schema definitions for `_silver_load_history`
   - The schema definitions for `_silver_data_quality_log`
   - The configuration data population script
7. Run all cells
8. Verify by running:
   ```python
   display(spark.table("_silver_load_config").orderBy("config_id"))
   ```

**Expected Result:** 30 configuration records created

---

### Step 2: Upload Utility Notebook

**Time: ~5 minutes**

1. In Fabric workspace, click **+ New** → **Import notebook**
2. Upload `notebooks/nb_silver_utilities.py`
3. Rename to `nb_silver_utilities` (remove .py extension if shown)
4. **Important:** Attach to `lh_silver` lakehouse
5. Run the notebook to verify - should see "Silver Layer Utilities Loaded Successfully"

> **Note:** This notebook uses `%run` to be imported by other notebooks. It must be in the same workspace.

---

### Step 3: Upload Transformation Notebooks

**Time: ~15 minutes**

Upload each notebook in this order:

| Order | Notebook File | Purpose |
|-------|---------------|---------|
| 1 | `nb_silver_reference_tables.py` | 12 reference/lookup tables |
| 2 | `nb_silver_master_person.py` | Person dimension |
| 3 | `nb_silver_master_customer.py` | Customer dimension |
| 4 | `nb_silver_master_supplier.py` | Supplier dimension |
| 5 | `nb_silver_master_stock_item.py` | Product dimension |
| 6 | `nb_silver_txn_orders.py` | Sales orders |
| 7 | `nb_silver_txn_invoices.py` | Sales invoices |
| 8 | `nb_silver_txn_purchases.py` | Purchase orders |
| 9 | `nb_silver_txn_financials.py` | Financial transactions |
| 10 | `nb_silver_inventory.py` | Inventory tables |
| 11 | `nb_silver_iot_temperatures.py` | IoT temperature readings |

For each notebook:
1. **Import** the notebook
2. **Attach** to `lh_silver` lakehouse
3. Verify the `%run ./nb_silver_utilities` cell works

---

### Step 4: Test Reference Tables First

**Time: ~10 minutes**

1. Open `nb_silver_reference_tables`
2. Click **Run all**
3. Monitor progress - should complete in ~2-3 minutes
4. Verify output shows 12 tables created:
   - slv_city
   - slv_country
   - slv_state_province
   - slv_delivery_method
   - slv_payment_method
   - slv_transaction_type
   - slv_buying_group
   - slv_customer_category
   - slv_supplier_category
   - slv_color
   - slv_package_type
   - slv_stock_group

5. Verify data:
   ```python
   # Run in a new cell
   for table in ["slv_city", "slv_country", "slv_customer_category"]:
       count = spark.table(table).count()
       print(f"{table}: {count:,} records")
   ```

---

### Step 5: Test Master Tables

**Time: ~15 minutes**

Run each master notebook individually:

1. **nb_silver_master_person**
   ```
   Expected: ~1,111 records in slv_person
   ```

2. **nb_silver_master_customer**
   ```
   Expected: ~663 records in slv_customer
   Watch for: Data quality check results
   ```

3. **nb_silver_master_supplier**
   ```
   Expected: ~13 records in slv_supplier
   ```

4. **nb_silver_master_stock_item**
   ```
   Expected: ~227 records in slv_stock_item
   Watch for: Price tier distribution
   ```

---

### Step 6: Test Transaction Tables

**Time: ~30 minutes**

Run in order (dependencies matter):

1. **nb_silver_txn_orders** (needs: customer, stock_item, person)
   ```
   Expected: 
   - slv_order: ~73,595 records
   - slv_order_line: ~231,412 records
   - slv_special_deal: ~3 records
   ```

2. **nb_silver_txn_invoices** (needs: orders)
   ```
   Expected:
   - slv_invoice: ~70,510 records
   - slv_invoice_line: ~228,265 records
   ```

3. **nb_silver_txn_purchases** (needs: supplier, stock_item)
   ```
   Expected:
   - slv_purchase_order: ~2,074 records
   - slv_purchase_order_line: ~8,367 records
   ```

4. **nb_silver_txn_financials** (needs: invoices, purchases)
   ```
   Expected:
   - slv_customer_transaction: ~101,714 records
   - slv_supplier_transaction: ~6,624 records
   ```

---

### Step 7: Test Inventory and IoT Tables

**Time: ~45 minutes (IoT tables are large)**

1. **nb_silver_inventory**
   ```
   Expected:
   - slv_stock_item_holding: ~227 records
   - slv_stock_item_stock_group: ~443 records
   - slv_stock_item_transaction: ~236,667 records
   ```

2. **nb_silver_iot_temperatures** (longest running!)
   ```
   Expected:
   - slv_vehicle_temperature: ~65,998 records
   - slv_cold_room_temperature: ~3,651,193 records
   
   Note: Cold room temperatures may take 15-30 minutes
   ```

---

### Step 8: Create the Pipeline (Optional)

If you want pipeline orchestration:

1. Create new Data Pipeline: `pl_silver_master_load`
2. Add **Notebook activities** for each notebook
3. Configure dependencies as shown in `pl_silver_master_load.json`
4. Set parameter `is_full_load` to "true" for initial run

**Pipeline Dependency Flow:**
```
Reference Tables
       ↓
   ┌───┴───┬───────┬───────┐
   ↓       ↓       ↓       ↓
Person  Customer Supplier Stock Item
   │       │       │       │
   └───────┴───┬───┴───────┘
               ↓
            Orders ───────────→ Invoices
               │                    │
               └────────┬───────────┘
                        ↓
              Financial Transactions
                        │
            Purchases ──┘
                        │
                        ↓
                   Inventory
                   
IoT Temperatures (runs in parallel with everything)
```

---

### Step 9: Verify Complete Silver Layer

Run this verification script:

```python
# Silver Layer Verification
silver_tables = [
    # Reference
    "slv_city", "slv_country", "slv_state_province",
    "slv_delivery_method", "slv_payment_method", "slv_transaction_type",
    "slv_buying_group", "slv_customer_category", "slv_supplier_category",
    "slv_color", "slv_package_type", "slv_stock_group",
    # Master
    "slv_person", "slv_customer", "slv_supplier", "slv_stock_item",
    # Transaction
    "slv_order", "slv_order_line", "slv_special_deal",
    "slv_invoice", "slv_invoice_line",
    "slv_purchase_order", "slv_purchase_order_line",
    "slv_customer_transaction", "slv_supplier_transaction",
    # Inventory
    "slv_stock_item_holding", "slv_stock_item_stock_group", "slv_stock_item_transaction",
    # IoT
    "slv_vehicle_temperature", "slv_cold_room_temperature"
]

print("=" * 60)
print("SILVER LAYER VERIFICATION")
print("=" * 60)

total_records = 0
for table in silver_tables:
    try:
        count = spark.table(table).count()
        total_records += count
        print(f"✅ {table}: {count:,}")
    except Exception as e:
        print(f"❌ {table}: MISSING - {str(e)[:50]}")

print("=" * 60)
print(f"TOTAL: {len(silver_tables)} tables, {total_records:,} records")
print("=" * 60)
```

**Expected Total:** ~4.8 million records across 30 Silver tables

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| `%run ./nb_silver_utilities` fails | Ensure utility notebook is in same workspace and named exactly `nb_silver_utilities` |
| "Table not found: lh_bronze.xxx" | Verify Bronze tables exist and notebook is attached to `lh_silver` |
| Memory errors on large tables | Increase Spark cluster size or process in batches |
| Referential integrity failures | Run notebooks in dependency order |

### Checking Logs

```python
# View load history
display(spark.table("_silver_load_history").orderBy(desc("start_time")).limit(20))

# Check for failures
display(spark.table("_silver_load_history").filter(col("status") == "Failed"))
```

---

## Next Steps

After Silver layer is complete:

1. ✅ Verify all 30 Silver tables exist with correct record counts
2. ✅ Review data quality check results
3. ✅ Set up scheduled pipeline for incremental loads
4. → **Phase 4: Gold Layer** - Dimensional modeling and star schema

---

## Quick Reference: Silver Table Summary

| Category | Tables | Total Records |
|----------|--------|---------------|
| Reference | 12 | ~38,500 |
| Master | 4 | ~2,000 |
| Transaction | 10 | ~650,000 |
| Inventory | 3 | ~237,000 |
| IoT | 2 | ~3,700,000 |
| **TOTAL** | **30** | **~4,800,000** |
