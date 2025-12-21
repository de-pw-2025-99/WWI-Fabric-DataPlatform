# WWI-Fabric-DataPlatform

A production-grade data platform migrating Wide World Importers from on-premises SQL Server to Microsoft Fabric using Medallion Architecture.

## 🎯 Project Overview

This project demonstrates an end-to-end data engineering solution that:
- Migrates data from on-premises SQL Server to Microsoft Fabric
- Implements Medallion Architecture (Bronze → Silver → Gold)
- Uses metadata-driven pipeline design patterns
- Follows enterprise CI/CD practices
- Provides self-service analytics through Power BI

## 🏗️ Architecture

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   SQL Server     │     │   CSV/JSON       │     │  Event Streams   │
│   (WWI OLTP)     │     │   Files          │     │  (Future)        │
└────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘
         │                        │                        │
         └────────────────────────┼────────────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │   Fabric Data Pipeline  │
                    │   (Metadata-Driven)     │
                    └────────────┬────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        FABRIC LAKEHOUSE                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐              │
│  │   BRONZE    │───▶│   SILVER    │───▶│    GOLD     │             │
│  │   (Raw)     │    │ (Cleansed)  │    │  (Curated)  │              │
│  └─────────────┘    └─────────────┘    └─────────────┘              │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   Power BI Direct Lake  │
                    │   (Semantic Model)      │
                    └─────────────────────────┘
```

## 📁 Repository Structure

```
WWI-Fabric-DataPlatform/
├── docs/                    # Documentation
│   ├── architecture/        # Architecture diagrams and decisions
│   ├── design/             # Data models, naming conventions
│   ├── runbooks/           # Operational guides
│   └── decisions/          # Architecture Decision Records (ADRs)
├── src/                    # Source code
│   ├── fabric/             # Fabric artifacts
│   │   ├── pipelines/      # Data pipelines (Bronze/Silver/Gold)
│   │   ├── notebooks/      # PySpark notebooks
│   │   ├── semantic-models/# Power BI semantic models
│   │   └── reports/        # Power BI reports
│   ├── sql/                # SQL scripts
│   └── config/             # Configuration files
├── tests/                  # Test scripts
├── infrastructure/         # IaC templates
├── pipelines/              # CI/CD pipelines
├── scripts/                # Setup and utility scripts
└── data/                   # Sample data files
```

## 🚀 Getting Started

### Prerequisites

- SQL Server (local) with Wide World Importers database
- Azure subscription
- Microsoft Fabric capacity (trial or paid)
- Git client
- Azure CLI
- Visual Studio Code (recommended)

### Setup Steps

1. Clone this repository
2. Follow [Environment Setup Guide](docs/runbooks/environment-setup.md)
3. Provision Azure resources using Bicep templates
4. Configure Fabric workspace with Git integration

## 📊 Data Sources

| Source | Type | Description |
|--------|------|-------------|
| Wide World Importers | SQL Server | Main OLTP database |
| Supplementary Files | CSV/JSON | Additional reference data |
| Event Streams | Event Hub | Real-time data (Phase 7) |

## 🏷️ Naming Conventions

See [Naming Conventions](docs/design/naming-conventions.md) for detailed standards.

## 🔄 CI/CD

This project uses GitHub Actions / Azure DevOps Pipelines for:
- Automated testing
- Environment promotion (Dev → Test → Prod)
- Fabric artifact deployment

## 📈 Project Phases

- [x] Phase 1: Foundation & Environment Setup
- [ ] Phase 2: Bronze Layer - Raw Data Ingestion
- [ ] Phase 3: Silver Layer - Data Transformation
- [ ] Phase 4: Gold Layer - Business Models
- [ ] Phase 5: Semantic Layer & Reporting
- [ ] Phase 6: DevOps & CI/CD
- [ ] Phase 7: Advanced Features (Real-time)

## 📝 License

This project is for educational purposes.

## 🤝 Contributing

This is a personal learning project. Feel free to fork and adapt for your own learning.
