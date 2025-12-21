# Naming Conventions

This document defines the naming standards for all artifacts in the WWI-Fabric-DataPlatform project.

## General Principles

1. **Consistency**: Use the same pattern across all artifacts
2. **Clarity**: Names should be self-documenting
3. **Brevity**: Keep names concise while maintaining clarity
4. **No Spaces**: Use underscores or PascalCase (context-dependent)
5. **Lowercase**: Prefer lowercase for technical artifacts

---

## Fabric Workspace Naming

### Workspaces

Pattern: `{project}_{environment}`

| Environment | Workspace Name |
|-------------|----------------|
| Development | `wwi_dev` |
| Test | `wwi_test` |
| Production | `wwi_prod` |

---

## Lakehouse Naming

### Lakehouses

Pattern: `lh_{layer}_{domain}`

| Layer | Lakehouse Name | Purpose |
|-------|----------------|---------|
| Bronze | `lh_bronze` | Raw data storage |
| Silver | `lh_silver` | Cleansed data |
| Gold | `lh_gold` | Curated business models |

### Tables

#### Bronze Layer Tables

Pattern: `{source}_{schema}_{table}`

Examples:
- `wwi_sales_orders`
- `wwi_sales_orderlines`
- `wwi_warehouse_stockitems`
- `file_csv_exchangerates`
- `file_json_promotions`

#### Silver Layer Tables

Pattern: `{domain}_{entity}`

Examples:
- `sales_orders`
- `sales_orderlines`
- `inventory_stockitems`
- `reference_countries`

#### Gold Layer Tables

Pattern: `{type}_{name}`

Types:
- `dim_` - Dimension tables
- `fact_` - Fact tables
- `agg_` - Aggregate tables
- `bridge_` - Bridge tables

Examples:
- `dim_customer`
- `dim_product`
- `dim_date`
- `dim_geography`
- `fact_sales`
- `fact_inventory`
- `agg_sales_monthly`

---

## Pipeline Naming

### Data Pipelines

Pattern: `pl_{layer}_{source/domain}_{action}`

Actions:
- `extract` - Extract from source
- `load` - Load to destination
- `transform` - Transform data
- `orchestrate` - Orchestration pipeline

Examples:
- `pl_bronze_wwi_extract`
- `pl_bronze_files_load`
- `pl_silver_sales_transform`
- `pl_gold_dimensions_load`
- `pl_master_orchestrate`

### Pipeline Activities

Pattern: `{action}_{target}`

Examples:
- `copy_orders`
- `lookup_metadata`
- `foreach_tables`
- `exec_notebook_transform`

---

## Notebook Naming

Pattern: `nb_{layer}_{domain}_{purpose}`

Examples:
- `nb_bronze_wwi_extract`
- `nb_silver_sales_transform`
- `nb_silver_quality_checks`
- `nb_gold_dim_customer`
- `nb_gold_fact_sales`
- `nb_util_common_functions`

---

## Semantic Model Naming

### Semantic Models

Pattern: `sm_{domain}_{purpose}`

Examples:
- `sm_sales_analytics`
- `sm_inventory_management`
- `sm_executive_dashboard`

### Measures

Pattern: `{Metric Name}` (Title Case with spaces)

Examples:
- `Total Sales`
- `Sales YTD`
- `Sales vs LY`
- `Avg Order Value`
- `Customer Count`

### Calculated Columns

Pattern: `{Column Name}` (Title Case with spaces)

Examples:
- `Full Name`
- `Age Group`
- `Sales Tier`

---

## Report Naming

Pattern: `rpt_{domain}_{purpose}`

Examples:
- `rpt_sales_overview`
- `rpt_inventory_status`
- `rpt_customer_analysis`
- `rpt_executive_kpi`

---

## Column Naming Standards

### Standard Columns (All Tables)

| Column | Description | Data Type |
|--------|-------------|-----------|
| `_extracted_at` | Timestamp of extraction | timestamp |
| `_loaded_at` | Timestamp of loading | timestamp |
| `_source_system` | Source system identifier | string |
| `_source_file` | Source file path (if applicable) | string |
| `_is_current` | Current record flag (SCD) | boolean |
| `_valid_from` | Record validity start (SCD2) | timestamp |
| `_valid_to` | Record validity end (SCD2) | timestamp |
| `_hash_key` | Hash of business key | string |
| `_hash_diff` | Hash of non-key columns | string |

### Business Key Columns

Pattern: `{entity}_key` (surrogate) or `{entity}_id` (natural)

Examples:
- `customer_key` (surrogate key)
- `customer_id` (natural/business key)
- `product_key`
- `order_id`

### Date Columns

Pattern: `{event}_date` or `{event}_datetime`

Examples:
- `order_date`
- `ship_date`
- `created_datetime`
- `modified_datetime`

### Flag/Boolean Columns

Pattern: `is_{condition}` or `has_{feature}`

Examples:
- `is_active`
- `is_deleted`
- `has_backorder`

### Amount/Quantity Columns

Pattern: `{measure}_{unit}` or `{measure}_amount`

Examples:
- `quantity_ordered`
- `unit_price`
- `total_amount`
- `tax_amount`

---

## File Naming

### Configuration Files

Pattern: `{purpose}.json` or `{purpose}_{environment}.json`

Examples:
- `source-tables.json`
- `transformation-rules.json`
- `dev.json`
- `prod.json`

### SQL Scripts

Pattern: `{sequence}_{action}_{object}.sql`

Examples:
- `01_create_schema.sql`
- `02_create_tables.sql`
- `03_insert_seed_data.sql`

### Sample Data Files

Pattern: `{source}_{entity}.{extension}`

Examples:
- `supplementary_countries.csv`
- `supplementary_exchangerates.json`

---

## Git Branch Naming

### Branch Types

| Type | Pattern | Example |
|------|---------|---------|
| Feature | `feature/{ticket}-{description}` | `feature/WWI-123-bronze-pipeline` |
| Bugfix | `bugfix/{ticket}-{description}` | `bugfix/WWI-456-fix-null-handling` |
| Hotfix | `hotfix/{ticket}-{description}` | `hotfix/WWI-789-prod-fix` |
| Release | `release/{version}` | `release/1.0.0` |

### Main Branches

- `main` - Production-ready code
- `develop` - Integration branch

---

## Environment Variables

Pattern: `{PROJECT}_{CATEGORY}_{NAME}`

Examples:
- `WWI_SQL_CONNECTION_STRING`
- `WWI_FABRIC_WORKSPACE_ID`
- `WWI_ADLS_ACCOUNT_NAME`

---

## Summary Table

| Artifact Type | Pattern | Example |
|---------------|---------|---------|
| Workspace | `{project}_{env}` | `wwi_dev` |
| Lakehouse | `lh_{layer}` | `lh_bronze` |
| Bronze Table | `{src}_{schema}_{table}` | `wwi_sales_orders` |
| Silver Table | `{domain}_{entity}` | `sales_orders` |
| Gold Table | `{type}_{name}` | `dim_customer` |
| Pipeline | `pl_{layer}_{domain}_{action}` | `pl_bronze_wwi_extract` |
| Notebook | `nb_{layer}_{domain}_{purpose}` | `nb_silver_sales_transform` |
| Semantic Model | `sm_{domain}_{purpose}` | `sm_sales_analytics` |
| Report | `rpt_{domain}_{purpose}` | `rpt_sales_overview` |
