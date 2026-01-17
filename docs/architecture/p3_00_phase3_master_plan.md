# Phase 3: Silver Layer - Data Transformation

## Executive Summary

Transform 30 Bronze layer tables into cleansed, standardized, and business-ready Silver layer tables. This phase implements data quality enforcement, deduplication, schema standardization, and prepares data for Gold layer dimensional modeling.

---

## Phase 3 Objectives

| Objective | Description |
|-----------|-------------|
| **Data Cleansing** | Handle nulls, trim strings, standardize formats |
| **Deduplication** | Remove duplicates using business keys |
| **Schema Standardization** | Consistent naming, data types, and structures |
| **Data Quality** | Implement validation rules and quality metrics |
| **Business Keys** | Establish surrogate keys and business key mapping |
| **Audit Trail** | Maintain lineage with Silver layer metadata columns |
| **Incremental Processing** | Support both full refresh and incremental loads |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              lh_bronze (Source)                                  │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │  wwi_application_cities    wwi_sales_orders       wwi_warehouse_stockitems  ││
│  │  wwi_application_countries wwi_sales_orderlines   wwi_purchasing_suppliers  ││
│  │  ... (30 raw tables with _bronze_loaded_at, _bronze_pipeline_run_id)        ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        │ PySpark Notebooks
                                        │ (Run in lh_silver context)
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              lh_silver (Target)                                  │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │  DIMENSION STAGING           FACT STAGING              REFERENCE            ││
│  │  ─────────────────           ────────────              ─────────            ││
│  │  slv_customer               slv_order                 slv_city             ││
│  │  slv_supplier               slv_order_line            slv_country          ││
│  │  slv_stock_item             slv_invoice               slv_delivery_method  ││
│  │  slv_person                 slv_invoice_line          slv_payment_method   ││
│  │                             slv_purchase_order        slv_transaction_type ││
│  │                             slv_customer_transaction  slv_buying_group     ││
│  │                             slv_supplier_transaction  slv_customer_category││
│  │                                                                             ││
│  │  CONTROL TABLES                                                             ││
│  │  ─────────────────                                                          ││
│  │  _silver_load_config        (Transformation metadata)                       ││
│  │  _silver_load_history       (Load audit trail)                              ││
│  │  _silver_data_quality_log   (Quality check results)                         ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Silver Layer Table Classification

### Category 1: Reference Tables (Full Refresh)
Simple lookup tables - always full refresh, minimal transformation.

| Bronze Table | Silver Table | Transformation |
|--------------|--------------|----------------|
| wwi_application_cities | slv_city | Dedupe, standardize, add surrogate key |
| wwi_application_countries | slv_country | Dedupe, standardize |
| wwi_application_deliverymethods | slv_delivery_method | Dedupe, standardize |
| wwi_application_paymentmethods | slv_payment_method | Dedupe, standardize |
| wwi_application_transactiontypes | slv_transaction_type | Dedupe, standardize |
| wwi_sales_buyinggroups | slv_buying_group | Dedupe, standardize |
| wwi_sales_customercategories | slv_customer_category | Dedupe, standardize |
| wwi_warehouse_colors | slv_color | Dedupe, standardize |
| wwi_warehouse_packagetypes | slv_package_type | Dedupe, standardize |

### Category 2: Master/Dimension Tables (SCD Type 2 Candidates)
Business entities that change over time - may need history tracking.

| Bronze Table | Silver Table | Transformation |
|--------------|--------------|----------------|
| wwi_application_people | slv_person | Dedupe, flatten JSON, type standardization |
| wwi_application_stateprovinces | slv_state_province | Dedupe, geography handling |
| wwi_sales_customers | slv_customer | Dedupe, null handling, SCD prep |
| wwi_purchasing_suppliers | slv_supplier | Dedupe, null handling, SCD prep |
| wwi_purchasing_suppliercategories | slv_supplier_category | Dedupe, standardize |
| wwi_warehouse_stockitems | slv_stock_item | Dedupe, JSON handling, type conversion |
| wwi_warehouse_stockgroups | slv_stock_group | Dedupe, standardize |

### Category 3: Transaction Tables (Incremental)
High-volume transactional data - incremental processing required.

| Bronze Table | Silver Table | Transformation |
|--------------|--------------|----------------|
| wwi_sales_orders | slv_order | Dedupe on OrderID, date standardization |
| wwi_sales_orderlines | slv_order_line | Dedupe, decimal precision |
| wwi_sales_invoices | slv_invoice | Dedupe, date/time handling |
| wwi_sales_invoicelines | slv_invoice_line | Dedupe, calculation validation |
| wwi_sales_customertransactions | slv_customer_transaction | Dedupe, amount precision |
| wwi_sales_specialdeals | slv_special_deal | Dedupe, date range validation |
| wwi_purchasing_purchaseorders | slv_purchase_order | Dedupe, date handling |
| wwi_purchasing_purchaseorderlines | slv_purchase_order_line | Dedupe, calculation validation |
| wwi_purchasing_suppliertransactions | slv_supplier_transaction | Dedupe, amount precision |

### Category 4: Inventory/Operational Tables
Point-in-time state tables.

| Bronze Table | Silver Table | Transformation |
|--------------|--------------|----------------|
| wwi_warehouse_stockitemholdings | slv_stock_item_holding | Latest state per item |
| wwi_warehouse_stockitemstockgroups | slv_stock_item_stock_group | Bridge table, dedupe |
| wwi_warehouse_stockitemtransactions | slv_stock_item_transaction | Dedupe, audit trail |
| wwi_warehouse_vehicletemperatures | slv_vehicle_temperature | Time-series, outlier handling |
| wwi_warehouse_coldroomtemperatures | slv_cold_room_temperature | Time-series, outlier handling |

---

## Phase 3 Deliverables

### 3.1 Documentation
- [ ] Silver layer data dictionary
- [ ] Transformation mapping document
- [ ] Data quality rules specification
- [ ] ADR-005: Silver Layer Design Decisions

### 3.2 Control Framework
- [ ] `_silver_load_config` metadata table
- [ ] `_silver_load_history` audit table
- [ ] `_silver_data_quality_log` quality metrics table
- [ ] Stored procedures for Silver orchestration

### 3.3 Utility Notebooks
- [ ] `nb_silver_utilities` - Reusable transformation functions
- [ ] `nb_silver_data_quality` - DQ check framework

### 3.4 Transformation Notebooks (by category)
- [ ] `nb_silver_reference_tables` - All reference tables (batch)
- [ ] `nb_silver_master_person` - People/contacts
- [ ] `nb_silver_master_customer` - Customer dimension prep
- [ ] `nb_silver_master_supplier` - Supplier dimension prep
- [ ] `nb_silver_master_stock_item` - Product dimension prep
- [ ] `nb_silver_txn_orders` - Orders + Order Lines
- [ ] `nb_silver_txn_invoices` - Invoices + Invoice Lines
- [ ] `nb_silver_txn_purchases` - Purchase Orders + Lines
- [ ] `nb_silver_txn_financials` - Customer/Supplier Transactions
- [ ] `nb_silver_inventory` - Stock holdings and transactions
- [ ] `nb_silver_iot_temperatures` - Vehicle/Cold room temps

### 3.5 Pipeline Orchestration
- [ ] `pl_silver_master_load` - Master orchestrator
- [ ] `pl_silver_load_category` - Category-level parallel loading

---

## Implementation Timeline

| Week | Focus | Deliverables |
|------|-------|--------------|
| **Week 1** | Foundation | Control tables, utility notebook, DQ framework |
| **Week 2** | Reference + Master | Reference tables, Person, Customer, Supplier |
| **Week 3** | Transactions | Orders, Invoices, Purchases, Financials |
| **Week 4** | Inventory + Testing | Stock holdings, Temperatures, End-to-end testing |

---

## Silver Layer Metadata Columns

Every Silver table includes these audit columns:

```python
# Added to every Silver table
_silver_loaded_at       # TIMESTAMP - When record was loaded to Silver
_silver_pipeline_run_id # STRING - Fabric pipeline run ID
_silver_source_table    # STRING - Bronze source table name
_silver_row_hash        # STRING - SHA256 hash of business columns (for CDC)
_silver_is_current      # BOOLEAN - For SCD Type 2 tables only
_silver_valid_from      # TIMESTAMP - For SCD Type 2 tables only
_silver_valid_to        # TIMESTAMP - For SCD Type 2 tables only
```

---

## Data Quality Framework

### Quality Dimensions

| Dimension | Description | Example Checks |
|-----------|-------------|----------------|
| **Completeness** | Required fields are populated | CustomerID NOT NULL |
| **Uniqueness** | No duplicate records | Unique OrderID |
| **Validity** | Values within acceptable ranges | UnitPrice >= 0 |
| **Consistency** | Cross-field logic holds | OrderDate <= ExpectedDeliveryDate |
| **Referential** | FK relationships valid | CustomerID exists in slv_customer |
| **Timeliness** | Data freshness acceptable | _bronze_loaded_at within 24 hours |

### Quality Check Output

```python
# _silver_data_quality_log schema
{
    "check_id": "INT",
    "run_timestamp": "TIMESTAMP",
    "table_name": "STRING",
    "check_name": "STRING",
    "check_dimension": "STRING",  # completeness, uniqueness, etc.
    "check_sql": "STRING",
    "records_checked": "LONG",
    "records_failed": "LONG",
    "pass_percentage": "DECIMAL(5,2)",
    "status": "STRING",  # PASS, WARN, FAIL
    "threshold_warn": "DECIMAL(5,2)",
    "threshold_fail": "DECIMAL(5,2)"
}
```

---

## Next Steps

1. **Create control tables in lh_silver** - Metadata-driven approach
2. **Build utility notebook** - Common transformation functions
3. **Build DQ framework** - Reusable quality checks
4. **Start with reference tables** - Simplest transformations first
5. **Progress through categories** - Reference → Master → Transaction → Inventory

---

## File Structure for Phase 3

```
wwi-phase3-silver-layer/
├── 00_phase3_master_plan.md           # This document
├── 01_silver_control_tables.sql        # SQL DDL for control schema
├── 02_silver_data_dictionary.md        # Silver layer data dictionary
├── 03_transformation_mapping.md        # Bronze→Silver mapping
├── notebooks/
│   ├── nb_silver_utilities.py          # Reusable functions
│   ├── nb_silver_data_quality.py       # DQ framework
│   ├── nb_silver_reference_tables.py   # Reference table loads
│   ├── nb_silver_master_customer.py    # Customer transformation
│   ├── nb_silver_master_supplier.py    # Supplier transformation
│   ├── nb_silver_master_stock_item.py  # Stock item transformation
│   ├── nb_silver_master_person.py      # Person transformation
│   ├── nb_silver_txn_orders.py         # Orders transformation
│   ├── nb_silver_txn_invoices.py       # Invoices transformation
│   ├── nb_silver_txn_purchases.py      # Purchases transformation
│   ├── nb_silver_txn_financials.py     # Financial transactions
│   ├── nb_silver_inventory.py          # Inventory tables
│   └── nb_silver_iot_temperatures.py   # Temperature readings
└── pipelines/
    ├── pl_silver_master_load.json      # Master orchestrator
    └── pl_silver_load_category.json    # Category loader
```

Ready to begin implementation?
