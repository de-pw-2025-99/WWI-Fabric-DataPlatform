# Environment Setup Guide

This guide walks through setting up the complete development environment for the WWI-Fabric-DataPlatform project.

## Prerequisites Checklist

- [ ] Azure subscription (with Owner or Contributor access)
- [ ] Microsoft Fabric capacity (trial or paid)
- [ ] SQL Server (local) with Wide World Importers database
- [ ] Git client installed
- [ ] Azure CLI installed
- [ ] Visual Studio Code (recommended)
- [ ] Power BI Desktop (optional, for local development)

## Step 1: Verify SQL Server Setup

### 1.1 Confirm Wide World Importers Database

Connect to your local SQL Server and verify:

```sql
-- Check database exists
SELECT name, state_desc 
FROM sys.databases 
WHERE name = 'WideWorldImporters';

-- Check table counts
USE WideWorldImporters;
SELECT 
    s.name AS SchemaName,
    COUNT(*) AS TableCount
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
GROUP BY s.name
ORDER BY s.name;
```

Expected schemas: `Application`, `Purchasing`, `Sales`, `Warehouse`

### 1.2 Create Service Account (Recommended)

For production-like setup, create a dedicated service account:

```sql
-- Create login for Fabric integration
USE master;
CREATE LOGIN fabric_etl WITH PASSWORD = 'YourStrongPassword123!';

USE WideWorldImporters;
CREATE USER fabric_etl FOR LOGIN fabric_etl;

-- Grant read permissions
ALTER ROLE db_datareader ADD MEMBER fabric_etl;

-- Grant execute on specific schemas if needed
GRANT EXECUTE ON SCHEMA::Application TO fabric_etl;
```

### 1.3 Enable SQL Server for Remote Connections

1. Open SQL Server Configuration Manager
2. Enable TCP/IP protocol
3. Set TCP Port (default: 1433)
4. Restart SQL Server service
5. Configure Windows Firewall to allow port 1433

## Step 2: Azure Resources Setup

### 2.1 Install Azure CLI

If not already installed:

```bash
# Windows (PowerShell)
winget install Microsoft.AzureCLI

# macOS
brew install azure-cli

# Linux
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

### 2.2 Login to Azure

```bash
az login
az account list --output table
az account set --subscription "Your Subscription Name"
```

### 2.3 Create Resource Group

```bash
# Set variables
RESOURCE_GROUP="rg-wwi-fabric-platform"
LOCATION="eastus"  # Choose your preferred region

# Create resource group
az group create \
  --name $RESOURCE_GROUP \
  --location $LOCATION \
  --tags Project=WWI-Fabric-DataPlatform Environment=Development
```

### 2.4 Create Azure Data Lake Storage Gen2

```bash
# Set variables (storage account name must be globally unique)
STORAGE_ACCOUNT="stwwifabric$(openssl rand -hex 4)"
CONTAINER_LANDING="landing"
CONTAINER_ARCHIVE="archive"
CONTAINER_EXTERNAL="external"

# Create storage account with hierarchical namespace (ADLS Gen2)
az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --tags Project=WWI-Fabric-DataPlatform

# Create containers
az storage container create --name $CONTAINER_LANDING --account-name $STORAGE_ACCOUNT
az storage container create --name $CONTAINER_ARCHIVE --account-name $STORAGE_ACCOUNT
az storage container create --name $CONTAINER_EXTERNAL --account-name $STORAGE_ACCOUNT

# Get storage account key (save this securely)
az storage account keys list \
  --account-name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --query '[0].value' -o tsv
```

### 2.5 Note Your Azure Resources

Record these values for later use:

| Resource | Value |
|----------|-------|
| Resource Group | `rg-wwi-fabric-platform` |
| Storage Account | `stwwifabricXXXX` |
| Storage Account Key | `(from above command)` |

## Step 3: Microsoft Fabric Setup

### 3.1 Access Microsoft Fabric

1. Go to [https://app.fabric.microsoft.com](https://app.fabric.microsoft.com)
2. Sign in with your Microsoft account
3. If using trial, start trial from the account menu

### 3.2 Create Development Workspace

1. Click **Workspaces** in the left navigation
2. Click **+ New workspace**
3. Configure:
   - **Name**: `wwi_dev`
   - **Description**: `WWI Data Platform - Development Environment`
   - **License mode**: Trial or Fabric capacity
4. Click **Apply**

### 3.3 Create Lakehouses

In the `wwi_dev` workspace:

1. Click **+ New** → **Lakehouse**
2. Create three lakehouses:
   - `lh_bronze` - Raw data layer
   - `lh_silver` - Cleansed data layer  
   - `lh_gold` - Curated business layer

### 3.4 Configure OneLake Shortcut to ADLS Gen2 (Optional)

This allows Fabric to access your ADLS Gen2 storage:

1. Open `lh_bronze` Lakehouse
2. Click **...** next to Files → **New shortcut**
3. Select **Azure Data Lake Storage Gen2**
4. Enter connection details:
   - URL: `https://<storage-account>.dfs.core.windows.net/`
   - Authentication: Account key
5. Select container and create shortcut

## Step 4: Git Repository Setup (GitHub)

### 4.1 Create GitHub Repository

1. Go to [https://github.com/new](https://github.com/new)
2. Create new repository:
   - **Name**: `WWI-Fabric-DataPlatform`
   - **Visibility**: Private
   - Do NOT initialize with README

### 4.2 Clone and Add Files

```bash
# Clone empty repository
git clone https://github.com/YOUR_USERNAME/WWI-Fabric-DataPlatform.git
cd WWI-Fabric-DataPlatform

# Extract project files to this folder
# Then add all files
git add .
git commit -m "Initial project structure and documentation"
git push origin main

# Create develop branch
git checkout -b develop
git push -u origin develop
```

### 4.3 Create GitHub Personal Access Token

1. Go to GitHub → **Settings** → **Developer settings** → **Personal access tokens** → **Tokens (classic)**
2. Click **Generate new token (classic)**
3. Configure:
   - **Note**: `Fabric-Git-Integration`
   - **Expiration**: 90 days
   - **Scopes**: Select `repo` (full control)
4. Click **Generate token**
5. **COPY AND SAVE THE TOKEN** - you won't see it again!

## Step 5: Connect Fabric Workspace to GitHub

1. Open your `wwi_dev` workspace in Fabric
2. Click **Workspace settings** (gear icon)
3. Select **Git integration**
4. Choose **GitHub**
5. Click **Add account** and enter your PAT
6. Select:
   - **Repository**: `WWI-Fabric-DataPlatform`
   - **Branch**: `develop`
   - **Git folder**: `/src/fabric`
7. Click **Connect and sync**

## Step 6: Install Self-Hosted Integration Runtime

Required for connecting Fabric to on-premises SQL Server.

### 6.1 Download SHIR

1. In Fabric, go to **Settings** → **Manage connections and gateways**
2. Click **+ New** → **On-premises data gateway**
3. Download the installation package

### 6.2 Install SHIR

1. Run the installer on a machine with SQL Server access
2. Sign in with your Microsoft account (company credentials for Fabric)
3. Register the gateway with a name (e.g., `WWI-OnPrem-Gateway`)
4. Note the gateway name for pipeline configuration

### 6.3 Create Connection in Fabric

1. Go to **Settings** → **Manage connections and gateways**
2. Click **+ New connection**
3. Configure:
   - **Connection name**: `conn_wwi_sqlserver`
   - **Connection type**: SQL Server
   - **Server**: Your SQL Server name
   - **Database**: WideWorldImporters
   - **Authentication**: Basic (use fabric_etl account)
   - **Gateway**: Select your installed gateway
4. Test and save connection

## Step 7: Verify Setup

### 7.1 Verification Checklist

- [ ] SQL Server accessible with fabric_etl account
- [ ] Azure resource group created
- [ ] ADLS Gen2 storage account with containers
- [ ] Fabric workspace `wwi_dev` created
- [ ] Three lakehouses created (bronze, silver, gold)
- [ ] Git repository created and cloned
- [ ] Fabric workspace connected to Git
- [ ] SHIR installed and gateway registered
- [ ] SQL Server connection configured in Fabric

### 7.2 Test SQL Connection

1. In Fabric, create a new **Data pipeline**
2. Add a **Lookup** activity
3. Configure:
   - Source: SQL Server connection
   - Query: `SELECT TOP 10 * FROM Sales.Orders`
4. Debug and verify results

## Next Steps

Once environment setup is complete, proceed to:

→ [Phase 2: Bronze Layer - Raw Data Ingestion](./phase2-bronze-layer.md)

## Troubleshooting

### SHIR Connection Issues

1. Verify SQL Server allows remote connections
2. Check Windows Firewall rules
3. Ensure TCP/IP is enabled in SQL Server Configuration
4. Verify service account has proper permissions

### Git Sync Issues

1. Ensure branch exists and is not protected
2. Check authentication token hasn't expired
3. Verify folder path in Git settings

### Fabric Capacity Issues

1. Check trial hasn't expired
2. Verify capacity is assigned to workspace
3. Contact admin if capacity is paused
