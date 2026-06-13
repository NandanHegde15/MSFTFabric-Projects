# MSFTFabric-Projects

A comprehensive collection of Microsoft Fabric projects demonstrating enterprise data solutions with modern cloud architecture, analytics, and automation. This repository showcases advanced implementations using T-SQL, Python, and Jupyter Notebooks (60.4%, 26.7%, and 12.9% respectively).

## 📋 Repository Overview

This repository contains production-grade Microsoft Fabric implementations for enterprise scenarios including healthcare analytics, data governance, and cloud infrastructure automation. Each project demonstrates best practices in data engineering, analytics, and automation using the Microsoft data and AI platform.

**Language Composition:**
- **T-SQL (60.4%)** - Database design, transformations, and data pipelines
- **Python (26.7%)** - Data processing, automation, and integration logic
- **Jupyter Notebooks (12.9%)** - Interactive exploration, transformation jobs, and documentation

---

## 🚀 Featured Projects

### 1. **Metadata Driven Framework** - Healthcare Data Analytics Platform

A comprehensive Microsoft Fabric-based healthcare analytics solution built on FHIR (Fast Healthcare Interoperability Resources) standards.

**Key Features:**
- Multi-layer medallion architecture (Raw → Silver → Gold layers)
- FHIR-compliant healthcare data modeling
- Automated ETL pipelines for data ingestion and transformation
- Power BI semantic models for enterprise reporting
- Row-level security (RLS) for data governance
- AI-powered analytics with Copilot integration

**Technologies:**
- Microsoft Fabric (Warehouse, Lakehouse, Notebooks)
- T-SQL for data transformations
- dbt (Data Build Tool) for orchestration
- Python/PySpark for complex transformations
- Power BI for business intelligence

**Use Cases:**
- Patient 360° analytics
- Insurance claims processing
- Healthcare provider financial analysis
- Data-driven clinical decision support

📖 [View Full Documentation](Metadata%20Driven%20Framework/README.md) | 📺 [Demo Video](https://www.youtube.com/watch?v=Z7mjHmX8-40)

---

### 2. **AutoShield for Azure** - Smart IP Whitelisting & Firewall Sync Engine

An intelligent automation framework that streamlines Azure firewall management with automatic IP range synchronization.

**Key Features:**
- Automated extraction and flattening of Azure IP ranges
- Real-time firewall rule management across Azure services
- Dynamic IP synchronization for SQL Databases and Storage Accounts
- Centralized configuration management via Fabric SQL
- REST-based integration with Azure Functions
- Automated compliance and security updates

**Technologies:**
- Microsoft Fabric Data Pipelines
- T-SQL for data extraction and management
- Azure Functions for firewall automation
- Fabric SQL Database for configuration
- External REST endpoints for integration

**Use Cases:**
- Multi-tenant Azure infrastructure management
- Automatic IP whitelisting for SQL Servers
- Storage Account firewall synchronization
- Compliance and security automation

📖 [View Full Documentation](AutoShield%20for%20Azure%20_%20Smart%20IP%20Whitelisting%20and%20Firewall%20Sync%20Engine/README.md)

---

## 🏗️ Architecture Highlights

### Common Architectural Patterns

**Medallion Architecture (Lakehouse)**
```
Raw Data Layer → Bronze → Silver Layer → Gold Layer → Analytics & Reports
```

**Data Flow**
1. **Ingestion** - Multiple data sources via APIs, databases, or files
2. **Transformation** - Complex business logic and data quality checks
3. **Aggregation** - Dimensional modeling and analytics-ready datasets
4. **Consumption** - Power BI, APIs, and downstream systems

**Technology Stack**
- **Data Platform:** Microsoft Fabric (Warehouse, Lakehouse, Notebooks)
- **Orchestration:** Data Pipelines, dbt
- **Computing:** PySpark, Python, T-SQL
- **Analytics:** Power BI, Semantic Models
- **Automation:** Azure Functions, Fabric SQL triggers
- **Governance:** Row-Level Security (RLS), Metadata management

---

## 🔑 Core Capabilities

| Capability | Description |
|-----------|-------------|
| **Data Ingestion** | Multi-source APIs, batch processing, real-time streaming |
| **Transformation** | Complex ETL/ELT with dbt, PySpark, and T-SQL |
| **Analytics** | Dimensional modeling, aggregations, and business metrics |
| **Reporting** | Power BI dashboards, semantic models, interactive analytics |
| **Automation** | Scheduled pipelines, event-driven workflows, Azure integration |
| **Governance** | Row-level security, metadata management, audit trails |
| **Security** | Encryption, role-based access, compliance frameworks |

---

## 📚 Getting Started

### Prerequisites
- Microsoft Fabric workspace access (Admin/Contributor role)
- VS Code or Visual Studio for development
- Git for version control
- Python 3.8+ (for local development)
- Basic SQL and T-SQL knowledge

### Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/NandanHegde15/MSFTFabric-Projects.git
   ```

2. **Explore individual projects**
   - Start with [Metadata Driven Framework](Metadata%20Driven%20Framework/) for healthcare analytics
   - or [AutoShield for Azure](AutoShield%20for%20Azure%20_%20Smart%20IP%20Whitelisting%20and%20Firewall%20Sync%20Engine/) for infrastructure automation

3. **Follow project-specific setup guides**
   - Each project contains detailed README and setup instructions
   - Review architecture documentation
   - Check prerequisites and dependencies

### Documentation Structure

Each project includes:
- **README.md** - Project overview and quick start
- **PROJECT_UNDERSTANDING.md** - Detailed architecture and design
- **Code files** - Implementation in T-SQL, Python, Jupyter Notebooks
- **Configuration guides** - Setup and deployment instructions

---

## 🛠️ Development Stack

### Languages & Tools

| Tool | Purpose | Usage |
|------|---------|-------|
| **T-SQL** | Data modeling, ETL, stored procedures, triggers | 60.4% |
| **Python** | Data processing, automation, Azure integration | 26.7% |
| **Jupyter Notebooks** | Interactive development, documentation, transformation jobs | 12.9% |

### Key Technologies

- **Microsoft Fabric**
  - Warehouse (SQL Data Warehouse)
  - Lakehouse (Data Lake Storage)
  - Notebooks (PySpark, Python)
  - Data Pipelines (Orchestration)
  - Semantic Models (Analytics)

- **Data Engineering**
  - dbt (Data Build Tool)
  - PySpark
  - Pandas, NumPy

- **Cloud Services**
  - Azure Functions
  - Azure SQL Database
  - Azure Storage
  - REST APIs

- **Analytics & BI**
  - Power BI
  - DAX expressions
  - Semantic modeling

---

## 📊 Project Statistics

| Metric | Value |
|--------|-------|
| Projects | 2 |
| Primary Language | T-SQL (60.4%) |
| Repository ID | 1046377511 |
| Latest Update | 2026 |

---

## 🤝 Contributing

We welcome contributions to improve and expand these projects!

### How to Contribute

1. **Fork the repository**
2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature
   ```
3. **Make changes and test thoroughly**
4. **Submit a pull request** with detailed description

### Code Standards

- **SQL:** Follow T-SQL best practices, use clear naming conventions
- **Python:** Follow PEP 8 style guide
- **Documentation:** Update READMEs and add inline comments
- **Testing:** Validate all changes before submitting

---

## 📖 Documentation

- **[Metadata Driven Framework](Metadata%20Driven%20Framework/README.md)** - Healthcare analytics platform documentation
- **[AutoShield for Azure](AutoShield%20for%20Azure%20_%20Smart%20IP%20Whitelisting%20and%20Firewall%20Sync%20Engine/README.md)** - IP automation engine documentation
- **[Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)**
- **[T-SQL Documentation](https://learn.microsoft.com/en-us/sql/)**
- **[Python Best Practices](https://peps.python.org/pep-0008/)**

---

## 🔗 Related Resources

- [Microsoft Fabric Learn Hub](https://learn.microsoft.com/en-us/fabric/)
- [Azure Data Architect](https://learn.microsoft.com/en-us/azure/architecture/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Power BI Best Practices](https://docs.microsoft.com/en-us/power-bi/)
- [FHIR Standard](https://www.hl7.org/fhir/)

---

## 📝 License

These projects are provided as-is for educational and enterprise reference purposes.

---

## 📞 Support & Questions

For questions, issues, or collaboration opportunities:
- Open an issue on GitHub
- Review project-specific documentation
- Check troubleshooting sections in individual project READMEs

---

**Last Updated:** June 2026

**Repository Owner:** [NandanHegde15](https://github.com/NandanHegde15)

