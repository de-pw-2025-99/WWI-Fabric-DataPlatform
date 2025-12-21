# Data Dictionary

This document provides detailed information about the data sources, tables, and columns used in the WWI-Fabric-DataPlatform.

## Source System: Wide World Importers (SQL Server)

### Overview

Wide World Importers is a fictitious wholesale novelty goods importer and distributor. The database includes:

- **Sales**: Customer orders, invoices, and transactions
- **Purchasing**: Supplier orders and transactions
- **Warehouse**: Stock items and inventory management
- **Application**: Reference data (people, locations, etc.)

---

## Sales Schema

### Sales.Customers

Customer master data including billing and delivery information.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| CustomerID | int | No | Primary key |
| CustomerName | nvarchar(100) | No | Customer business name |
| BillToCustomerID | int | No | FK to billing customer |
| CustomerCategoryID | int | No | FK to customer category |
| BuyingGroupID | int | Yes | FK to buying group |
| PrimaryContactPersonID | int | No | FK to primary contact |
| DeliveryMethodID | int | No | FK to delivery method |
| DeliveryCityID | int | No | FK to delivery city |
| CreditLimit | decimal(18,2) | Yes | Credit limit amount |
| AccountOpenedDate | date | No | Date account opened |
| StandardDiscountPercentage | decimal(18,3) | No | Standard discount % |
| PaymentDays | int | No | Payment terms (days) |
| ValidFrom | datetime2 | No | **Watermark column** |
| ValidTo | datetime2 | No | Temporal end date |

### Sales.Orders

Sales order headers.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| OrderID | int | No | Primary key |
| CustomerID | int | No | FK to customer |
| SalespersonPersonID | int | No | FK to salesperson |
| OrderDate | date | No | Order date |
| ExpectedDeliveryDate | date | No | Expected delivery |
| CustomerPurchaseOrderNumber | nvarchar(20) | Yes | Customer PO number |
| IsUndersupplyBackordered | bit | No | Backorder flag |
| LastEditedBy | int | No | FK to editor |
| LastEditedWhen | datetime2 | No | **Watermark column** |

### Sales.OrderLines

Sales order line items.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| OrderLineID | int | No | Primary key |
| OrderID | int | No | FK to order |
| StockItemID | int | No | FK to stock item |
| Description | nvarchar(100) | No | Line description |
| PackageTypeID | int | No | FK to package type |
| Quantity | int | No | Quantity ordered |
| UnitPrice | decimal(18,2) | Yes | Unit price |
| TaxRate | decimal(18,3) | No | Tax rate % |
| LastEditedWhen | datetime2 | No | **Watermark column** |

---

## Warehouse Schema

### Warehouse.StockItems

Product/stock item master data.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| StockItemID | int | No | Primary key |
| StockItemName | nvarchar(100) | No | Item name |
| SupplierID | int | No | FK to supplier |
| ColorID | int | Yes | FK to color |
| UnitPackageID | int | No | FK to unit package |
| Brand | nvarchar(50) | Yes | Brand name |
| Size | nvarchar(20) | Yes | Size description |
| LeadTimeDays | int | No | Lead time in days |
| UnitPrice | decimal(18,2) | No | Unit price |
| RecommendedRetailPrice | decimal(18,2) | Yes | RRP |
| ValidFrom | datetime2 | No | **Watermark column** |
| ValidTo | datetime2 | No | Temporal end date |

### Warehouse.StockItemHoldings

Current inventory holdings per stock item.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| StockItemID | int | No | PK and FK to stock item |
| QuantityOnHand | int | No | Current quantity |
| BinLocation | nvarchar(20) | No | Bin location |
| ReorderLevel | int | No | Reorder level |
| TargetStockLevel | int | No | Target stock level |
| LastEditedWhen | datetime2 | No | **Watermark column** |

---

## Application Schema

### Application.People

People including employees and customer/supplier contacts.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| PersonID | int | No | Primary key |
| FullName | nvarchar(50) | No | Full name |
| PreferredName | nvarchar(50) | No | Preferred name |
| IsEmployee | bit | No | Employee flag |
| IsSalesperson | bit | No | Salesperson flag |
| EmailAddress | nvarchar(256) | Yes | Email address |
| ValidFrom | datetime2 | No | **Watermark column** |
| ValidTo | datetime2 | No | Temporal end date |

### Application.Cities

City reference data.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| CityID | int | No | Primary key |
| CityName | nvarchar(50) | No | City name |
| StateProvinceID | int | No | FK to state/province |
| LatestRecordedPopulation | bigint | Yes | Population |
| ValidFrom | datetime2 | No | **Watermark column** |

---

## Supplementary Data Sources

### Exchange Rates (CSV)

Daily currency exchange rates for financial reporting.

| Column | Data Type | Description |
|--------|-----------|-------------|
| date | date | Rate effective date |
| currency_code | string | ISO currency code |
| currency_name | string | Currency full name |
| exchange_rate_to_usd | decimal | Rate to USD |
| source | string | Data source |

### Promotions (JSON)

Marketing promotions and discount rules.

| Field | Data Type | Description |
|-------|-----------|-------------|
| promotion_id | string | Unique promotion ID |
| promotion_name | string | Promotion name |
| start_date | date | Start date |
| end_date | date | End date |
| discount_type | string | Type (percentage/fixed/tiered) |
| discount_value | decimal | Discount amount |
| applicable_stock_groups | array | Eligible stock groups |
| is_active | boolean | Active flag |

---

## Medallion Architecture Mapping

### Bronze Layer
- Direct copy from source with audit columns added
- Table naming: `{source}_{schema}_{table}`
- Example: `wwi_sales_orders`

### Silver Layer
- Cleansed and standardized data
- Table naming: `{domain}_{entity}`
- Example: `sales_orders`

### Gold Layer
- Dimensional model tables
- Table naming: `dim_{entity}` or `fact_{entity}`
- Example: `dim_customer`, `fact_sales`
