# ADR-001: Medallion Architecture

## Status

Accepted

## Date

2024-12-21

## Context

We are building a data platform to migrate Wide World Importers from on-premises SQL Server to Microsoft Fabric. We need to establish a data architecture pattern that:

1. Supports incremental data processing
2. Enables data quality improvements at each stage
3. Provides clear separation of concerns
4. Allows for easy troubleshooting and data lineage
5. Scales with increasing data volumes
6. Aligns with Microsoft Fabric best practices

## Decision

We will implement the **Medallion Architecture** (also known as Multi-Hop Architecture) with three layers:

### Bronze Layer (Raw)
- **Purpose**: Land raw data exactly as received from source systems
- **Format**: Delta tables preserving source schema
- **Processing**: Minimal transformation (add audit columns only)
- **Retention**: Keep historical snapshots for reprocessing

### Silver Layer (Cleansed)
- **Purpose**: Cleansed, conformed, and standardized data
- **Format**: Delta tables with optimized schema
- **Processing**: Data quality checks, deduplication, type standardization
- **Retention**: Current state with change history (SCD Type 2 where needed)

### Gold Layer (Curated)
- **Purpose**: Business-ready data models optimized for analytics
- **Format**: Delta tables in dimensional model (Star Schema)
- **Processing**: Business logic, aggregations, KPI calculations
- **Retention**: Current analytical state with aggregated history

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           SOURCE SYSTEMS                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                      │
│  │ SQL Server  │  │ CSV Files   │  │ JSON APIs   │                      │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                      │
└─────────┼────────────────┼────────────────┼─────────────────────────────┘
          │                │                │
          ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  BRONZE (Raw)                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ • Full/incremental extracts                                      │    │
│  │ • Source schema preserved                                        │    │
│  │ • Audit columns added (_extracted_at, _source_system)           │    │
│  │ • No business logic                                              │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  SILVER (Cleansed)                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ • Data quality validated                                         │    │
│  │ • Duplicates removed                                             │    │
│  │ • Data types standardized                                        │    │
│  │ • Nulls handled                                                  │    │
│  │ • Cross-source integrated                                        │    │
│  │ • SCD Type 1/2 applied                                           │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  GOLD (Curated)                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ • Dimensional model (Star Schema)                                │    │
│  │ • Business logic applied                                         │    │
│  │ • KPIs calculated                                                │    │
│  │ • Aggregations pre-computed                                      │    │
│  │ • Optimized for reporting                                        │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  SEMANTIC LAYER                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ • Power BI Direct Lake                                           │    │
│  │ • Business-friendly names                                        │    │
│  │ • Calculated measures                                            │    │
│  │ • Row-level security                                             │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

## Consequences

### Positive

1. **Clear Data Lineage**: Easy to trace data from source to consumption
2. **Reprocessing Capability**: Can rebuild Silver/Gold from Bronze if logic changes
3. **Isolation**: Issues in one layer don't corrupt other layers
4. **Incremental Processing**: Each layer can process incrementally
5. **Performance**: Gold layer optimized for query performance
6. **Flexibility**: Different consumers can access appropriate layer
7. **Fabric Alignment**: Matches Microsoft's recommended Lakehouse patterns

### Negative

1. **Storage Overhead**: Data duplicated across layers
2. **Complexity**: More pipelines to maintain than single-layer approach
3. **Latency**: Data passes through multiple stages

### Mitigations

1. **Storage**: Delta Lake compression and Z-ordering reduce storage
2. **Complexity**: Metadata-driven pipelines reduce maintenance burden
3. **Latency**: Acceptable for batch processing; streaming for real-time needs

## Alternatives Considered

### Option A: Single Layer (Direct Load)
- **Rejected**: No separation of concerns, hard to maintain data quality

### Option B: Two Layers (Raw + Transformed)
- **Rejected**: Insufficient separation between cleansing and business logic

### Option C: Data Vault 2.0
- **Rejected**: Over-engineered for current requirements, steeper learning curve

## References

- [Microsoft Fabric Lakehouse Best Practices](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview)
- [Medallion Architecture - Databricks](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake Documentation](https://docs.delta.io/)
