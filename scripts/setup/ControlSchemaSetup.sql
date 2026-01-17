-- ============================================================================
-- WWI Fabric Migration - Control Schema Setup
-- Purpose: Create metadata-driven pipeline control tables
-- ============================================================================

-- ============================================================================
-- SECTION 1: Create Control Schema
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
BEGIN
    EXEC('CREATE SCHEMA control');
END
GO

-- ============================================================================
-- SECTION 2: Load Configuration Table
-- Stores metadata for all tables to be loaded into the Bronze layer
-- ============================================================================

-- CLEAN ALL TABLE

DROP TABLE IF EXISTS control.table_load_history
GO
DROP TABLE IF EXISTS control.batch_run_history
GO
DROP TABLE IF EXISTS control.load_config
GO




IF OBJECT_ID('control.load_config', 'U') IS NOT NULL
    DROP TABLE control.load_config;
GO

CREATE TABLE control.load_config (
    config_id               INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Source Information
    source_schema           VARCHAR(128) NOT NULL,
    source_table            VARCHAR(128) NOT NULL,
    source_query            NVARCHAR(MAX) NULL,             -- Custom SQL for tables with unsupported types (e.g., Geography)
    
    -- Target Information
    target_lakehouse        VARCHAR(128) NOT NULL DEFAULT 'lh_bronze',
    target_schema           VARCHAR(128) NOT NULL DEFAULT 'dbo',
    target_table            VARCHAR(128) NOT NULL,
    
    -- Load Configuration
    load_type               VARCHAR(20) NOT NULL,           -- 'full' or 'incremental'
    load_tier               INT NOT NULL DEFAULT 1,         -- 1=reference, 2=parent, 3=child
    load_priority           INT NOT NULL DEFAULT 100,       -- Lower = higher priority within tier
    
    -- Watermark Configuration (for incremental loads)
    watermark_column        VARCHAR(128) NULL,              -- Column to track changes
    watermark_data_type     VARCHAR(50) NULL,               -- 'datetime', 'int', 'bigint'
    last_watermark          VARCHAR(50) NULL,               -- Last successful extraction point
    
    -- Status and Control
    is_active               BIT NOT NULL DEFAULT 1,
    is_enabled_for_full     BIT NOT NULL DEFAULT 1,         -- Include in full refresh runs
    
    -- Metadata
    row_count_source        BIGINT NULL,                    -- Last known source row count
    row_count_loaded        BIGINT NULL,                    -- Rows loaded in last run
    last_load_status        VARCHAR(20) NULL,               -- 'Success', 'Failed', 'Running'
    last_load_start         DATETIME2 NULL,
    last_load_end           DATETIME2 NULL,
    last_error_message      NVARCHAR(MAX) NULL,
    
    -- Audit
    created_date            DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    created_by              VARCHAR(128) NOT NULL DEFAULT SYSTEM_USER,
    modified_date           DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    modified_by             VARCHAR(128) NOT NULL DEFAULT SYSTEM_USER,
    
    -- Constraints
    CONSTRAINT UQ_load_config_source UNIQUE (source_schema, source_table),
    CONSTRAINT CK_load_type CHECK (load_type IN ('full', 'incremental')),
    CONSTRAINT CK_load_tier CHECK (load_tier BETWEEN 1 AND 5)
);
GO

-- Create index for efficient tier-based lookups
CREATE NONCLUSTERED INDEX IX_load_config_tier_active 
ON control.load_config (load_tier, is_active) 
INCLUDE (source_schema, source_table, watermark_column, last_watermark, load_type);
GO

-- ============================================================================
-- SECTION 3: Batch Run History Table
-- Tracks each execution of the master pipeline
-- ============================================================================

IF OBJECT_ID('control.batch_run_history', 'U') IS NOT NULL
    DROP TABLE control.batch_run_history;
GO

CREATE TABLE control.batch_run_history (
    batch_run_id            INT IDENTITY(1,1) PRIMARY KEY,
    batch_cutoff_time       DATETIME2 NOT NULL,             -- The snapshot boundary
    pipeline_run_id         VARCHAR(50) NULL,               -- ADF/Fabric pipeline run ID
    
    -- Status
    status                  VARCHAR(20) NOT NULL DEFAULT 'Running',
    start_time              DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    end_time                DATETIME2 NULL,
    
    -- Statistics
    tables_total            INT NULL,
    tables_succeeded        INT NULL,
    tables_failed           INT NULL,
    total_rows_loaded       BIGINT NULL,
    
    -- Error tracking
    error_message           NVARCHAR(MAX) NULL,
    
    CONSTRAINT CK_batch_status CHECK (status IN ('Running', 'Succeeded', 'Failed', 'Cancelled'))
);
GO

-- ============================================================================
-- SECTION 4: Table Load History
-- Detailed history of each table load within a batch
-- ============================================================================
IF OBJECT_ID('control.table_load_history', 'U') IS NOT NULL
    DROP TABLE control.table_load_history;
GO

CREATE TABLE control.table_load_history (
    load_history_id         INT IDENTITY(1,1) PRIMARY KEY,
    batch_run_id            INT NOT NULL,
    config_id               INT NOT NULL,
    
    -- Execution details
    pipeline_run_id         VARCHAR(50) NULL,
    activity_run_id         VARCHAR(50) NULL,
    
    -- Timing
    start_time              DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    end_time                DATETIME2 NULL,
    duration_seconds        AS DATEDIFF(SECOND, start_time, end_time),
    
    -- Data movement stats
    rows_read               BIGINT NULL,
    rows_written            BIGINT NULL,
    data_read_bytes         BIGINT NULL,
    data_written_bytes      BIGINT NULL,
    
    -- Watermark tracking
    watermark_start         VARCHAR(50) NULL,               -- Watermark at start of load
    watermark_end           VARCHAR(50) NULL,               -- Watermark at end of load (cutoff)
    
    -- Status
    status                  VARCHAR(20) NOT NULL DEFAULT 'Running',
    error_message           NVARCHAR(MAX) NULL,
    
    CONSTRAINT FK_table_load_batch FOREIGN KEY (batch_run_id) 
        REFERENCES control.batch_run_history(batch_run_id),
    CONSTRAINT FK_table_load_config FOREIGN KEY (config_id) 
        REFERENCES control.load_config(config_id),
    CONSTRAINT CK_table_load_status CHECK (status IN ('Running', 'Succeeded', 'Failed', 'Skipped'))
);
GO

CREATE NONCLUSTERED INDEX IX_table_load_history_batch 
ON control.table_load_history (batch_run_id, config_id);
GO

-- ============================================================================
-- SECTION 5: Stored Procedures for Pipeline Operations
-- ============================================================================

-- Procedure to start a new batch run
IF OBJECT_ID('control.usp_start_batch_run', 'P') IS NOT NULL
    DROP PROCEDURE control.usp_start_batch_run;
GO

CREATE PROCEDURE control.usp_start_batch_run
    @batch_cutoff_time DATETIME2,
    @pipeline_run_id VARCHAR(50) = NULL,
    @batch_run_id INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO control.batch_run_history (batch_cutoff_time, pipeline_run_id, status)
    VALUES (@batch_cutoff_time, @pipeline_run_id, 'Running');
    
    SET @batch_run_id = SCOPE_IDENTITY();
    
    SELECT @batch_run_id AS batch_run_id;
END
GO

-- Procedure to complete a batch run
IF OBJECT_ID('control.usp_complete_batch_run', 'P') IS NOT NULL
    DROP PROCEDURE control.usp_complete_batch_run;
GO

CREATE PROCEDURE control.usp_complete_batch_run
    @batch_run_id INT,
    @status VARCHAR(20),
    @error_message NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Calculate statistics
    DECLARE @tables_total INT, @tables_succeeded INT, @tables_failed INT, @total_rows BIGINT;
    
    SELECT 
        @tables_total = COUNT(*),
        @tables_succeeded = SUM(CASE WHEN status = 'Succeeded' THEN 1 ELSE 0 END),
        @tables_failed = SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END),
        @total_rows = SUM(ISNULL(rows_written, 0))
    FROM control.table_load_history
    WHERE batch_run_id = @batch_run_id;
    
    -- Update batch run
    UPDATE control.batch_run_history
    SET status = @status,
        end_time = GETUTCDATE(),
        tables_total = @tables_total,
        tables_succeeded = @tables_succeeded,
        tables_failed = @tables_failed,
        total_rows_loaded = @total_rows,
        error_message = @error_message
    WHERE batch_run_id = @batch_run_id;
    
    -- If successful, update watermarks in load_config
    IF @status = 'Succeeded'
    BEGIN
        UPDATE lc
        SET lc.last_watermark = tlh.watermark_end,
            lc.last_load_status = 'Success',
            lc.last_load_end = tlh.end_time,
            lc.row_count_loaded = tlh.rows_written,
            lc.modified_date = GETUTCDATE()
        FROM control.load_config lc
        INNER JOIN control.table_load_history tlh ON lc.config_id = tlh.config_id
        WHERE tlh.batch_run_id = @batch_run_id
          AND tlh.status = 'Succeeded';
    END
END
GO

-- Procedure to get tables for a specific tier
IF OBJECT_ID('control.usp_get_tables_by_tier', 'P') IS NOT NULL
    DROP PROCEDURE control.usp_get_tables_by_tier;
GO

CREATE PROCEDURE control.usp_get_tables_by_tier
    @load_tier INT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        config_id,
        source_schema,
        source_table,
        source_query,
        target_lakehouse,
        target_schema,
        target_table,
        load_type,
        load_tier,
        watermark_column,
        watermark_data_type,
        ISNULL(last_watermark, '1900-01-01') AS last_watermark
    FROM control.load_config
    WHERE load_tier = @load_tier
      AND is_active = 1
    ORDER BY load_priority, source_schema, source_table;
END
GO

-- ============================================================================
-- SECTION 6: Populate with WWI Tables
-- ============================================================================

-- Clear existing data for fresh load
DELETE FROM control.table_load_history;
DELETE FROM control.batch_run_history;
DELETE FROM control.load_config;
GO

-- Reset identity
DBCC CHECKIDENT ('control.load_config', RESEED, 0);
GO

-- ============================================================================
-- TIER 1: Reference/Lookup Tables (Small, rarely change, use FULL load)
-- ============================================================================
INSERT INTO control.load_config 
(source_schema, source_table, target_table, load_type, load_tier, watermark_column, watermark_data_type, last_watermark, source_query)
VALUES
-- Application schema - Reference data (with Geography column handling)
-- These use FULL load - small tables where we want to capture deletes
('Application', 'Cities', 'wwi_application_cities', 'full', 1, NULL, NULL, NULL,
    'SELECT 
        CityID,
        CityName,
        StateProvinceID,
        Location.STAsText() AS Location_WKT,
        Location.Lat AS Location_Latitude,
        Location.Long AS Location_Longitude,
        LatestRecordedPopulation,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Application.Cities'),

('Application', 'Countries', 'wwi_application_countries', 'full', 1, NULL, NULL, NULL,
    'SELECT 
        CountryID,
        CountryName,
        FormalName,
        IsoAlpha3Code,
        IsoNumericCode,
        CountryType,
        LatestRecordedPopulation,
        Continent,
        Region,
        Subregion,
        Border.STAsText() AS Border_WKT,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Application.Countries'),

('Application', 'StateProvinces', 'wwi_application_stateprovinces', 'full', 1, NULL, NULL, NULL,
    'SELECT 
        StateProvinceID,
        StateProvinceCode,
        StateProvinceName,
        CountryID,
        SalesTerritory,
        Border.STAsText() AS Border_WKT,
        LatestRecordedPopulation,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Application.StateProvinces'),

-- Application schema - Reference data (no special handling needed, FULL load)
('Application', 'DeliveryMethods', 'wwi_application_deliverymethods', 'full', 1, NULL, NULL, NULL, NULL),
('Application', 'PaymentMethods', 'wwi_application_paymentmethods', 'full', 1, NULL, NULL, NULL, NULL),
('Application', 'People', 'wwi_application_people', 'full', 1, NULL, NULL, NULL, NULL),
('Application', 'TransactionTypes', 'wwi_application_transactiontypes', 'full', 1, NULL, NULL, NULL, NULL),

-- Warehouse schema - Dimension reference (FULL load)
('Warehouse', 'Colors', 'wwi_warehouse_colors', 'full', 1, NULL, NULL, NULL, NULL),
('Warehouse', 'PackageTypes', 'wwi_warehouse_packagetypes', 'full', 1, NULL, NULL, NULL, NULL);
GO

-- ============================================================================
-- TIER 2: Parent/Master Tables (Mix of FULL for small, INCREMENTAL for large)
-- ============================================================================
INSERT INTO control.load_config 
(source_schema, source_table, target_table, load_type, load_tier, watermark_column, watermark_data_type, last_watermark, source_query)
VALUES
-- Sales schema - Small lookup tables (FULL load)
('Sales', 'BuyingGroups', 'wwi_sales_buyinggroups', 'full', 2, NULL, NULL, NULL, NULL),
('Sales', 'CustomerCategories', 'wwi_sales_customercategories', 'full', 2, NULL, NULL, NULL, NULL),

-- Sales schema - Larger master table (INCREMENTAL)
('Sales', 'Customers', 'wwi_sales_customers', 'incremental', 2, 'ValidFrom', 'datetime', '2013-01-01', NULL),

-- Purchasing schema - Small lookup (FULL)
('Purchasing', 'SupplierCategories', 'wwi_purchasing_suppliercategories', 'full', 2, NULL, NULL, NULL, NULL),

-- Purchasing schema - Larger master table (INCREMENTAL)
('Purchasing', 'Suppliers', 'wwi_purchasing_suppliers', 'incremental', 2, 'ValidFrom', 'datetime', '2013-01-01', NULL),

-- Warehouse schema - Small lookup (FULL)
('Warehouse', 'StockGroups', 'wwi_warehouse_stockgroups', 'full', 2, NULL, NULL, NULL, NULL),

-- Warehouse schema - Larger product master (INCREMENTAL)
('Warehouse', 'StockItems', 'wwi_warehouse_stockitems', 'incremental', 2, 'ValidFrom', 'datetime', '2013-01-01', NULL);
GO

-- ============================================================================
-- TIER 3: Transaction Header Tables (Large, append-heavy, use INCREMENTAL)
-- These tables grow continuously and rarely have updates to historical records
-- ============================================================================
INSERT INTO control.load_config 
(source_schema, source_table, target_table, load_type, load_tier, watermark_column, watermark_data_type, last_watermark, source_query)
VALUES
-- Sales transactions - Headers (INCREMENTAL - high volume, append-mostly)
('Sales', 'Orders', 'wwi_sales_orders', 'incremental', 3, 'LastEditedWhen', 'datetime', '2013-01-01', NULL),
('Sales', 'Invoices', 'wwi_sales_invoices', 'incremental', 3, 'LastEditedWhen', 'datetime', '2013-01-01', NULL),
('Sales', 'SpecialDeals', 'wwi_sales_specialdeals', 'incremental', 3, 'LastEditedWhen', 'datetime', '2013-01-01', NULL),

-- Purchasing transactions - Headers (INCREMENTAL)
('Purchasing', 'PurchaseOrders', 'wwi_purchasing_purchaseorders', 'incremental', 3, 'LastEditedWhen', 'datetime', '2013-01-01', NULL);
GO

-- ============================================================================
-- TIER 4: Transaction Line/Detail Tables (Largest tables, use INCREMENTAL)
-- These are the highest-volume tables - full load would be too slow
-- ============================================================================
INSERT INTO control.load_config 
(source_schema, source_table, target_table, load_type, load_tier, watermark_column, watermark_data_type, last_watermark, source_query)
VALUES
-- Sales transaction lines (INCREMENTAL - very high volume)
('Sales', 'OrderLines', 'wwi_sales_orderlines', 'incremental', 4, 'LastEditedWhen', 'datetime', '2013-01-01', NULL),
('Sales', 'InvoiceLines', 'wwi_sales_invoicelines', 'incremental', 4, 'LastEditedWhen', 'datetime', '2013-01-01', NULL),
('Sales', 'CustomerTransactions', 'wwi_sales_customertransactions', 'incremental', 4, 'LastEditedWhen', 'datetime', '2013-01-01', NULL),

-- Purchasing transaction lines (INCREMENTAL)
('Purchasing', 'PurchaseOrderLines', 'wwi_purchasing_purchaseorderlines', 'incremental', 4, 'LastEditedWhen', 'datetime', '2013-01-01', NULL),
('Purchasing', 'SupplierTransactions', 'wwi_purchasing_suppliertransactions', 'incremental', 4, 'LastEditedWhen', 'datetime', '2013-01-01', NULL),

-- Warehouse movements (INCREMENTAL - high volume transaction log)
('Warehouse', 'StockItemTransactions', 'wwi_warehouse_stockitemtransactions', 'incremental', 4, 'LastEditedWhen', 'datetime', '2013-01-01', NULL);
GO

-- ============================================================================
-- TIER 5: Inventory/Snapshot Tables (Mix based on table behavior)
-- Some are current-state snapshots (FULL), others are time-series (INCREMENTAL)
-- ============================================================================
INSERT INTO control.load_config 
(source_schema, source_table, target_table, load_type, load_tier, watermark_column, watermark_data_type, last_watermark, source_query)
VALUES
-- Current state snapshot tables (FULL - these get updated, not appended)
-- StockItemHoldings contains current inventory levels that change constantly
('Warehouse', 'StockItemHoldings', 'wwi_warehouse_stockitemholdings', 'full', 5, NULL, NULL, NULL, NULL),

-- Small junction table (FULL - captures any relationship changes)
('Warehouse', 'StockItemStockGroups', 'wwi_warehouse_stockitemstockgroups', 'full', 5, NULL, NULL, NULL, NULL),

-- Time-series sensor data (INCREMENTAL - append-only, very high volume)
('Warehouse', 'ColdRoomTemperatures', 'wwi_warehouse_coldroomtemperatures', 'incremental', 5, 'RecordedWhen', 'datetime', '2013-01-01', NULL),
('Warehouse', 'VehicleTemperatures', 'wwi_warehouse_vehicletemperatures', 'incremental', 5, 'RecordedWhen', 'datetime', '2013-01-01', NULL);
GO

-- ============================================================================
-- SECTION 7: Verification Queries
-- ============================================================================

-- Summary by tier and load type
SELECT 
    load_tier,
    load_type,
    COUNT(*) AS table_count
FROM control.load_config
WHERE is_active = 1
GROUP BY load_tier, load_type
ORDER BY load_tier, load_type;

-- Full list of tables with load strategy
SELECT 
    config_id,
    source_schema,
    source_table,
    target_table,
    load_type,
    load_tier,
    watermark_column,
    CASE 
        WHEN source_query IS NOT NULL THEN 'Yes (Geography handling)'
        ELSE 'No'
    END AS has_custom_query,
    is_active
FROM control.load_config
--ORDER BY load_tier, load_type DESC, source_schema, source_table;
ORDER BY source_schema, source_table, load_tier, load_type DESC;


-- Summary statistics
SELECT 
    COUNT(*) AS total_tables,
    SUM(CASE WHEN load_type = 'full' THEN 1 ELSE 0 END) AS full_load_tables,
    SUM(CASE WHEN load_type = 'incremental' THEN 1 ELSE 0 END) AS incremental_tables,
    SUM(CASE WHEN source_query IS NOT NULL THEN 1 ELSE 0 END) AS tables_with_custom_query
FROM control.load_config
WHERE is_active = 1;

PRINT 'Control schema setup complete!';

-- 1. Declare a variable to hold the count
DECLARE @TableCount INT;
-- 2. Assign the result of the query to the variable
SELECT @TableCount = COUNT(*) FROM control.load_config;
-- 3. Print the concatenated string
PRINT 'Total tables configured: ' + CAST(@TableCount AS VARCHAR(10));

GO

-------------------------------------------------------
-- AFTER SETUP, Tables haven't been registered yet

select  * from sys.tables t
left join control.load_config c
	on c.source_table = t.name
where 
	temporal_type <> 1 
	AND SCHEMA_NAME(schema_id) <> 'control'
	AND t.name <> 'sysdiagrams'
	AND c.source_table is null


-------------------  My Addition ----------- Tables need special handling 

DECLARE @sql NVARCHAR(MAX)
SET @sql = 'SELECT [SupplierID]
      ,[SupplierName]
      ,[SupplierCategoryID]
      ,[PrimaryContactPersonID]
      ,[AlternateContactPersonID]
      ,[DeliveryMethodID]
      ,[DeliveryCityID]
      ,[PostalCityID]
      ,[SupplierReference]
      ,[BankAccountName]
      ,[BankAccountBranch]
      ,[BankAccountCode]
      ,[BankAccountNumber]
      ,[BankInternationalCode]
      ,[PaymentDays]
      ,[InternalComments]
      ,[PhoneNumber]
      ,[FaxNumber]
      ,[WebsiteURL]
      ,[DeliveryAddressLine1]
      ,[DeliveryAddressLine2]
      ,[DeliveryPostalCode] 
      ,[DeliveryLocation].STAsText() AS [DeliveryLocation]
      ,[PostalAddressLine1]
      ,[PostalAddressLine2]
      ,[PostalPostalCode]
      ,[LastEditedBy]
      ,[ValidFrom]
      ,[ValidTo]
  FROM [Purchasing].[Suppliers]'

UPDATE a
SET source_query = @sql
--SELECT * 
FROM control.load_config a
WHERE a.source_table = 'Suppliers'
GO

DECLARE @sql NVARCHAR(MAX)
SET @sql = 'SELECT [CustomerID]
      ,[CustomerName]
      ,[BillToCustomerID]
      ,[CustomerCategoryID]
      ,[BuyingGroupID]
      ,[PrimaryContactPersonID]
      ,[AlternateContactPersonID]
      ,[DeliveryMethodID]
      ,[DeliveryCityID]
      ,[PostalCityID]
      ,[CreditLimit]
      ,[AccountOpenedDate]
      ,[StandardDiscountPercentage]
      ,[IsStatementSent]
      ,[IsOnCreditHold]
      ,[PaymentDays]
      ,[PhoneNumber]
      ,[FaxNumber]
      ,[DeliveryRun]
      ,[RunPosition]
      ,[WebsiteURL]
      ,[DeliveryAddressLine1]
      ,[DeliveryAddressLine2]
      ,[DeliveryPostalCode]
      ,[DeliveryLocation].STAsText() AS [DeliveryLocation]
      ,[PostalAddressLine1]
      ,[PostalAddressLine2]
      ,[PostalPostalCode]
      ,[LastEditedBy]
      ,[ValidFrom]
      ,[ValidTo]
  FROM [Sales].[Customers]'

UPDATE a
SET source_query = @sql
--SELECT * 
FROM control.load_config a
WHERE a.source_table = 'Customers'