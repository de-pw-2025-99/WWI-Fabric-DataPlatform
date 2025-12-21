# Solution Architecture

## Overview

This document describes the technical architecture for the WWI-Fabric-DataPlatform, a data engineering solution that migrates Wide World Importers from on-premises SQL Server to Microsoft Fabric.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                ON-PREMISES                                           │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │                           SQL SERVER                                         │    │
│  │  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐        │    │
│  │  │ WideWorldImporters│  │ Integration       │  │ Self-Hosted       │        │    │
│  │  │ Database          │  │ Runtime (SHIR)    │  │ Integration       │        │    │
│  │  │                   │  │                   │  │ Runtime           │        │    │
│  │  └───────────────────┘  └─────────┬─────────┘  └─────────┬─────────┘        │    │
│  └──────────────────────────────────┬┴────────────────────────────────────────┘    │
└─────────────────────────────────────┼───────────────────────────────────────────────┘
                                      │
                              Secure Connection
                                      │
┌─────────────────────────────────────┼───────────────────────────────────────────────┐
│                                     │         AZURE                                  │
│  ┌──────────────────────────────────┴──────────────────────────────────────────┐    │
│  │                        AZURE DATA LAKE STORAGE GEN2                          │    │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │    │
│  │  │ landing/        │  │ archive/        │  │ external/       │              │    │
│  │  │ (staging area)  │  │ (raw backups)   │  │ (partner data)  │              │    │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘              │    │
│  └──────────────────────────────────┬──────────────────────────────────────────┘    │
│                                     │                                                │
│                              OneLake Shortcut                                        │
│                                     │                                                │
└─────────────────────────────────────┼───────────────────────────────────────────────┘
                                      │
┌─────────────────────────────────────┼───────────────────────────────────────────────┐
│                                     │      MICROSOFT FABRIC                          │
│  ┌──────────────────────────────────┴──────────────────────────────────────────┐    │
│  │                              FABRIC WORKSPACE                                │    │
│  │                                                                              │    │
│  │  ┌────────────────────────────────────────────────────────────────────┐     │    │
│  │  │                        DATA PIPELINES                               │     │    │
│  │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │     │    │
│  │  │  │ pl_bronze_*  │  │ pl_silver_*  │  │ pl_gold_*    │              │     │    │
│  │  │  │ (Extract)    │  │ (Transform)  │  │ (Load DW)    │              │     │    │
│  │  │  └──────────────┘  └──────────────┘  └──────────────┘              │     │    │
│  │  └────────────────────────────────────────────────────────────────────┘     │    │
│  │                                     │                                        │    │
│  │  ┌────────────────────────────────────────────────────────────────────┐     │    │
│  │  │                          LAKEHOUSES                                 │     │    │
│  │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │     │    │
│  │  │  │ lh_bronze    │  │ lh_silver    │  │ lh_gold      │              │     │    │
│  │  │  │ (Raw Data)   │  │ (Cleansed)   │  │ (Curated)    │              │     │    │
│  │  │  │              │  │              │  │              │              │     │    │
│  │  │  │ Delta Tables │  │ Delta Tables │  │ Star Schema  │              │     │    │
│  │  │  └──────────────┘  └──────────────┘  └──────────────┘              │     │    │
│  │  └────────────────────────────────────────────────────────────────────┘     │    │
│  │                                     │                                        │    │
│  │  ┌────────────────────────────────────────────────────────────────────┐     │    │
│  │  │                       PYSPARK NOTEBOOKS                             │     │    │
│  │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │     │    │
│  │  │  │ nb_bronze_*  │  │ nb_silver_*  │  │ nb_gold_*    │              │     │    │
│  │  │  │              │  │              │  │              │              │     │    │
│  │  │  └──────────────┘  └──────────────┘  └──────────────┘              │     │    │
│  │  └────────────────────────────────────────────────────────────────────┘     │    │
│  │                                     │                                        │    │
│  │  ┌────────────────────────────────────────────────────────────────────┐     │    │
│  │  │                        SEMANTIC LAYER                               │     │    │
│  │  │  ┌─────────────────────────────────────────────────────┐           │     │    │
│  │  │  │            Power BI Semantic Model                   │           │     │    │
│  │  │  │            (Direct Lake Mode)                        │           │     │    │
│  │  │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐              │           │     │    │
│  │  │  │  │dim_*    │  │fact_*   │  │Measures │              │           │     │    │
│  │  │  │  └─────────┘  └─────────┘  └─────────┘              │           │     │    │
│  │  │  └─────────────────────────────────────────────────────┘           │     │    │
│  │  └────────────────────────────────────────────────────────────────────┘     │    │
│  │                                     │                                        │    │
│  │  ┌────────────────────────────────────────────────────────────────────┐     │    │
│  │  │                         POWER BI REPORTS                            │     │    │
│  │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │     │    │
│  │  │  │ rpt_sales_*  │  │rpt_inventory │  │rpt_executive │              │     │    │
│  │  │  └──────────────┘  └──────────────┘  └──────────────┘              │     │    │
│  │  └────────────────────────────────────────────────────────────────────┘     │    │
│  │                                                                              │    │
│  └──────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────────────┐    │
│  │                              DEVOPS INTEGRATION                               │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                        │    │
│  │  │ GitHub       │  │ Git Repos    │  │ CI/CD        │                        │    │
│  │  │              │  │              │  │ Pipelines    │                        │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘                        │    │
│  └──────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                      │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Source Systems

#### SQL Server (On-Premises)
- **Database**: Wide World Importers (OLTP)
- **Connection**: Self-Hosted Integration Runtime (SHIR)
- **Extraction**: Full and incremental loads via CDC or timestamp

#### Supplementary Files
- **Types**: CSV, JSON
- **Location**: ADLS Gen2 or local file system
- **Purpose**: Reference data, external data sources

### 2. Azure Components

#### Azure Data Lake Storage Gen2
- **Purpose**: External storage, landing zone, archive
- **Containers**:
  - `landing/` - Temporary staging for large files
  - `archive/` - Historical raw data backups
  - `external/` - Third-party data files

#### Self-Hosted Integration Runtime
- **Purpose**: Secure connection to on-premises SQL Server
- **Location**: On-premises server with SQL Server access
- **Security**: Encrypted connection, no inbound firewall rules

### 3. Microsoft Fabric Components

#### Lakehouses

| Lakehouse | Purpose | Data Format |
|-----------|---------|-------------|
| `lh_bronze` | Raw data exactly as extracted | Delta (source schema) |
| `lh_silver` | Cleansed and standardized data | Delta (standardized) |
| `lh_gold` | Business-ready dimensional model | Delta (star schema) |

#### Data Pipelines

| Pipeline Category | Purpose |
|-------------------|---------|
| Bronze Pipelines | Extract from sources, load raw |
| Silver Pipelines | Transform and cleanse |
| Gold Pipelines | Build dimensional model |
| Orchestration | Master pipeline coordination |

#### Notebooks

| Notebook Category | Purpose | Language |
|-------------------|---------|----------|
| Bronze | Minimal processing, audit columns | PySpark |
| Silver | Data quality, transformations | PySpark |
| Gold | Dimensional modeling, KPIs | PySpark/SQL |
| Utilities | Reusable functions | PySpark |

### 4. Semantic Layer

#### Power BI Semantic Model
- **Mode**: Direct Lake (queries Delta tables directly)
- **Contents**: Dimensions, Facts, Measures, Hierarchies
- **Features**: Row-Level Security, Calculated columns

### 5. DevOps Integration

#### Git Integration
- **Platform**: GitHub
- **Strategy**: Feature branching with PR reviews
- **Sync**: Fabric workspace connected to Git

#### CI/CD Pipelines
- **Build**: Validate notebooks, run tests
- **Deploy**: Promote across environments (Dev → Test → Prod)

## Data Flow

### Batch Processing Flow

```
1. Extraction (Bronze)
   Source → SHIR → Fabric Pipeline → Bronze Lakehouse
   
2. Transformation (Silver)
   Bronze Tables → PySpark Notebook → Silver Lakehouse
   
3. Modeling (Gold)
   Silver Tables → PySpark Notebook → Gold Lakehouse
   
4. Serving
   Gold Tables → Direct Lake → Power BI Reports
```

### Future: Real-Time Flow

```
1. Event Capture
   Source Changes → Event Hub → Eventstream
   
2. Stream Processing
   Eventstream → Real-time transformations
   
3. Serving
   KQL Database → Real-time Dashboard
```

## Security Architecture

### Network Security
- SHIR for on-premises connectivity (no public endpoints)
- Private endpoints for ADLS Gen2 (optional)
- Fabric capacity in secured tenant

### Data Security
- Row-Level Security in Power BI
- Column-level masking for sensitive data
- Audit logging enabled

### Access Control
- Azure AD authentication
- Fabric workspace roles
- Lakehouse permissions

## Environment Strategy

| Environment | Purpose | Fabric Workspace | Git Branch |
|-------------|---------|------------------|------------|
| Development | Build and test | `wwi_dev` | `develop` |
| Test | UAT and validation | `wwi_test` | `release/*` |
| Production | Live data | `wwi_prod` | `main` |

## Monitoring and Operations

### Pipeline Monitoring
- Fabric monitoring hub
- Pipeline run history
- Alert rules for failures

### Data Quality Monitoring
- Quality checks in Silver layer
- Data profiling reports
- Anomaly detection (future)

### Performance Monitoring
- Query performance in Lakehouse
- Semantic model refresh times
- Report rendering performance
