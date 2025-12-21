-- =============================================================================
-- WWI Source Database Exploration Queries
-- Purpose: Understand source data before building pipelines
-- =============================================================================

-- =============================================================================
-- 1. DATABASE OVERVIEW
-- =============================================================================

-- Get all schemas
SELECT 
    s.name AS schema_name,
    COUNT(t.name) AS table_count
FROM sys.schemas s
LEFT JOIN sys.tables t ON s.schema_id = t.schema_id
WHERE s.name IN ('Application', 'Sales', 'Purchasing', 'Warehouse')
GROUP BY s.name
ORDER BY s.name;

-- Get all tables with row counts
SELECT 
    s.name AS schema_name,
    t.name AS table_name,
    p.rows AS row_count,
    SUM(a.total_pages) * 8 / 1024 AS total_size_mb
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
WHERE s.name IN ('Application', 'Sales', 'Purchasing', 'Warehouse')
GROUP BY s.name, t.name, p.rows
ORDER BY s.name, t.name;

-- =============================================================================
-- 2. SALES SCHEMA EXPLORATION
-- =============================================================================

-- Sales.Customers - check data range and key columns
SELECT 
    COUNT(*) AS total_customers,
    COUNT(DISTINCT CustomerCategoryID) AS customer_categories,
    COUNT(DISTINCT BuyingGroupID) AS buying_groups,
    MIN(ValidFrom) AS earliest_record,
    MAX(ValidFrom) AS latest_record
FROM Sales.Customers;

-- Sales.Orders - understand order patterns
SELECT 
    COUNT(*) AS total_orders,
    COUNT(DISTINCT CustomerID) AS unique_customers,
    MIN(OrderDate) AS first_order_date,
    MAX(OrderDate) AS last_order_date,
    MIN(LastEditedWhen) AS earliest_edit,
    MAX(LastEditedWhen) AS latest_edit
FROM Sales.Orders;

-- Orders by year
SELECT 
    YEAR(OrderDate) AS order_year,
    COUNT(*) AS order_count,
    COUNT(DISTINCT CustomerID) AS unique_customers
FROM Sales.Orders
GROUP BY YEAR(OrderDate)
ORDER BY order_year;

-- =============================================================================
-- 3. WATERMARK COLUMNS FOR INCREMENTAL LOADS
-- =============================================================================

-- Get max watermark values for incremental load tracking
SELECT 'Sales.Orders' AS table_name, MAX(LastEditedWhen) AS max_watermark FROM Sales.Orders
UNION ALL SELECT 'Sales.OrderLines', MAX(LastEditedWhen) FROM Sales.OrderLines
UNION ALL SELECT 'Sales.Invoices', MAX(LastEditedWhen) FROM Sales.Invoices
UNION ALL SELECT 'Sales.Customers', MAX(ValidFrom) FROM Sales.Customers
UNION ALL SELECT 'Warehouse.StockItems', MAX(ValidFrom) FROM Warehouse.StockItems
UNION ALL SELECT 'Purchasing.PurchaseOrders', MAX(LastEditedWhen) FROM Purchasing.PurchaseOrders
UNION ALL SELECT 'Application.People', MAX(ValidFrom) FROM Application.People;
