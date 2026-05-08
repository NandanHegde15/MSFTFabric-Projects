# Fabric Hackathon Project - Complete Understanding

## 📋 Project Overview

This is a **Microsoft Fabric-based Healthcare Data Analytics Platform** built on FHIR (Fast Healthcare Interoperability Resources) standards. The project demonstrates a complete end-to-end data pipeline that ingests healthcare data, transforms it through multiple layers, and exposes it via analytical models and reports.

**Technology Stack:**
- Microsoft Fabric (Data Engineering & Analytics)
- SQL Data Warehouse
- dbt (Data Build Tool) for transformations
- Power BI for reporting & analytics
- Python & PySpark for data processing
- FHIR Ontology for healthcare data modeling


---

## 🏗️ Architecture Overview

### High-Level Data Flow

```
Raw FHIR Data (Lakehouse)
    ↓
Data Processing (Notebooks + Pipelines)
    ↓
Silver Layer (Cleaned & Standardized)
    ↓
Gold Layer (Business-Ready Analytics)
    ↓
Semantic Model (Power BI)
    ↓
Reports & Dashboards
```

---

## 📁 Project Structure & Components

### 1. **HackathonWH.Warehouse** - SQL Data Warehouse
**Purpose:** Central data repository with multi-layer medallion architecture

**Structure:**
```
HackathonWH/
├── dbo/
│   └── Tables/
│       └── Date.sql (dimension table)
├── Silver/ (Cleaned, Standardized Layer)
│   ├── Tables/
│   │   ├── Claim.sql
│   │   ├── ClaimCareTeam.sql
│   │   ├── ClaimDiagnosis.sql
│   │   ├── ClaimItem.sql
│   │   ├── ClaimProcedure.sql
│   │   ├── Coverage.sql
│   │   ├── Organization.sql
│   │   ├── Patient.sql
│   │   └── ... (other FHIR resources)
│   └── Views/ (Derived views for analytics)
├── Gold/ (Business Analytics Layer)
│   └── Tables/
│       ├── Patient360.sql
│       ├── ClaimSummary.sql
│       ├── FinancialSummaryByOrg.sql
│       ├── InvoiceReconciliation.sql
│       └── LastWorkingDayOfMonth.sql
├── MetaData/
│   └── Tables/
│       └── Dictionary.sql
└── Security/ (Row-Level Security)
    ├── Gold.sql
    ├── MetaData.sql
    └── Silver.sql
```

**Key Tables:**

| Table | Purpose | Layer |
|-------|---------|-------|
| **Claim** | FHIR Claim resource (insurance claims) | Silver |
| **Patient** | Patient demographics & identifiers | Silver |
| **Coverage** | Insurance coverage information | Silver |
| **Organization** | Healthcare organizations & payors | Silver |
| **Patient360** | Comprehensive 360° patient view with demographics, coverage, and claims summary | Gold |
| **ClaimSummary** | Aggregated claim metrics by patient | Gold |
| **FinancialSummaryByOrg** | Organization-level financial analytics | Gold |
| **InvoiceReconciliation** | Invoice tracking & reconciliation | Gold |
| **Dictionary** | Metadata/lookup tables | MetaData |



---

### 2. **FHIRModel.SemanticModel** - Power BI Semantic Model
**Purpose:** Defines analytical dimensions, measures, and relationships for Power BI reporting

**Configuration Files:**
- `model.tmdl` - Main model definition (7 tables: Patient360, InvoiceReconciliation, ClaimSummary, FinancialSummaryByOrg, Date, LastWorkingDayOfMonth, Dictionary)
- `database.tmdl` - Database-level settings
- `relationships.tmdl` - Table relationships & referential integrity
- `expressions.tmdl` - DAX calculations & measures
- `cultures/en-US.tmdl` - English localizations

**Data Source:** DirectLake connection to HackathonWH warehouse

**Features:**
- Query order optimization for DirectLake
- Power BI time intelligence enabled
- Advanced tooling support (Copilot, Web modeling)

---

### 3. **DBT_FHIR.DataBuildToolJob** - dbt Transformation Orchestration
**Purpose:** Manages SQL transformations via dbt with 4 parallel threads


**Key Points:**
- Targets the Silver layer for transformations
- Full refresh enabled for comprehensive data rebuild
- 4 parallel threads for performance optimization

---

### 4. **HackathonLh.Lakehouse** - Data Lake
**Purpose:** Raw data ingestion layer for FHIR data

**Purpose:** Raw data ingestion and file storage
- Stores raw FHIR JSON files
- Serves as source for data processing pipelines
- Example path: `Files/raw/fhir/coverage/08042026122125_Coverage.json`

---

### 5. **PL_Master_Hackathon.DataPipeline** - Data Ingestion Pipeline
**Purpose:** Orchestrates API data extraction and warehouse loading

**Key Operations:**
1. **Lookup Activity** - Queries API configuration from `Config.ApiConfig` table
   - Joins with `Config.ApiPagination` for pagination settings
   - Retrieves: API endpoints, pagination info, request limits

2. **Data Flow** - Extracts healthcare data from APIs
   - Transforms FHIR resources
   - Loads into warehouse (Silver layer)

3. **Error Handling** - Monitored and logged

**Data Sources:**
- **Primary:** SQL Hackathon Database (`Config.ApiConfig`)
- **Pagination:** Configured per API endpoint

---

### 6. **PL_MetaData.DataPipeline** - Metadata Pipeline
**Purpose:** Manages dictionary and configuration tables

---

### 7. **Data Processing Notebooks** - Spark Transformation Jobs

#### **NB_DataProcessing.Notebook** (PySpark)
**Purpose:** Processes raw FHIR JSON files into structured tables

**Operations:**
1. Reads FHIR JSON from Lakehouse `Files/` directory
2. Parses and flattens nested JSON structures
3. Loads into Lakehouse Tables layer
4. Uses ABFSS paths for OneLake integration

#### **NB_Metadata.Notebook**
**Purpose:** Processes and manages metadata/lookup data

#### **NB_RunDBT.Notebook**
**Purpose:** Orchestrates dbt transformations from within Fabric

---

### 8. **SqlHackathon.SQLDatabase** - Configuration Database
**Purpose:** Centralized configuration management

**Schema Structure:**
```
├── Config/
│   ├── Tables/
│   │   ├── ApiConfig (API endpoints & credentials)
│   │   └── ApiPagination (Pagination settings)
├── Log/
│   ├── StoredProcedures/
│   └── Tables/ (Audit & error logging)
└── Security/
    └── ... (User access controls)
```

---

### 9. **OL_FHIR.Ontology** - FHIR Data Modeling
**Purpose:** Defines healthcare data semantics using FHIR standards

**Components:**
- **Entity Types** - FHIR resource types (Patient, Claim, Coverage, Organization, etc.)
- **Relationship Types** - Connections between resources

---

### 10. **HackathonEnv.Environment** - Spark Compute Configuration
**Purpose:** Defines compute resources and Python dependencies

**Configuration:**
```yaml
dependencies:
  - pip:
      - openai==2.30.0  # AI/LLM integration
```

**Spark Compute Settings:** Configured in `Sparkcompute.yml`

---

### 11. **Report Generated by Copilot.Report** - Power BI Report
**Purpose:** Pre-built analytics dashboards

**Contains:**
- Report pages with visualizations
- Connected to FHIRModel semantic model
- Static resources for branding/assets

---

### 12. **IQDA_Hackathon.DataAgent** - Data Agent
**Purpose:** AI-powered data exploration (Copilot integration)
- Enables natural language queries on healthcare data
- Configuration files in `Files/Config/`

---

## 🔄 Data Flow & Processing Pipeline

### End-to-End Pipeline Sequence

```
1. API SOURCE
   └─→ External healthcare APIs provide FHIR resources
       (e.g., EHR systems, insurance platforms)

2. INGESTION (PL_Master_Hackathon.DataPipeline)
   ├─→ Lookup API configuration from SqlHackathon.Config.ApiConfig
   ├─→ Extract data via REST APIs
   └─→ Stage raw data in HackathonLh.Lakehouse

3. RAW PROCESSING (NB_DataProcessing.Notebook)
   ├─→ Read FHIR JSON files (abfss:// paths)
   ├─→ Parse and flatten nested structures
   ├─→ Validate FHIR compliance
   └─→ Load to Lakehouse Tables

4. TRANSFORMATION - SILVER LAYER (DBT_FHIR.DataBuildToolJob + Notebooks)
   ├─→ dbt transforms raw tables
   ├─→ Apply business rules & standardization
   ├─→ Handle type conversions & data cleaning
   ├─→ Create slowly-changing dimensions
   └─→ Load to HackathonWH.Silver schema
       ├── Claim (insurance claims)
       ├── Patient (demographics)
       ├── Coverage (insurance policies)
       ├── Organization (providers & payors)
       └── ... (other FHIR resources)

5. AGGREGATION - GOLD LAYER (SQL Views & Tables)
   ├─→ Create enterprise-grade analytic tables
   ├─→ Denormalize for performance
   ├─→ Apply business metrics & KPIs
   └─→ Load to HackathonWH.Gold schema
       ├── Patient360 (customer 360 view)
       ├── ClaimSummary (claim analytics)
       ├── FinancialSummaryByOrg (org metrics)
       ├── InvoiceReconciliation (invoice tracking)
       └── LastWorkingDayOfMonth (calendar helper)

6. SEMANTIC MODELING (FHIRModel.SemanticModel)
   ├─→ Define Power BI dimensions & measures
   ├─→ Configure relationships between tables
   ├─→ Create DAX calculations
   └─→ Set up row-level security

7. REPORTING & ANALYTICS
   ├─→ Power BI connected to semantic model
   ├─→ Interactive dashboards & reports
   ├─→ Drill-down capabilities
   └─→ AI-powered insights (Data Agent)

8. SECURITY & GOVERNANCE
   ├─→ Row-level security applied
   ├─→ Metadata tracking & audit logs
   └─→ Access control enforcement
```

---

## 🔑 Key Entities & Data Model

### Core FHIR Resources

| Resource | Meaning | Tables |
|----------|---------|--------|
| **Patient** | Individual receiving healthcare | Silver.Patient, Gold.Patient360 |
| **Claim** | Insurance claim for services | Silver.Claim, Silver.ClaimItem, Silver.ClaimProcedure, Silver.ClaimDiagnosis, Silver.ClaimCareTeam, Gold.ClaimSummary |
| **Coverage** | Insurance policy details | Silver.Coverage |
| **Organization** | Healthcare provider organizations | Silver.Organization |
| **Practitioner** | Healthcare professionals | Silver.Practitioner |
| **Appointment** | Scheduled visits | Silver.Appointment |
| **Condition** | Patient diagnoses | Silver.Condition |
| **Medication** | Prescribed medicines | Silver.Medication |

### Gold Layer Analytics

**Patient360** - Comprehensive patient analytics table
- Patient demographics (name, gender, DOB, contact)
- Coverage information (insurance, network, dates)
- Claim metrics (total count, amounts, dates)
- Invoice summary (billing, discounts, net/gross)
- Last updated timestamps

---

## 🛠️ Technical Configurations

### Database Connections

| Component | Type | Connection |
|-----------|------|-----------|
| **HackathonWH** | SQL Data Warehouse | `HackathonWH.sqlproj` |
| **SqlHackathon** | SQL Database (Config) | Config schemas |
| **OneLake** | Lakehouse | ABFSS paths |


### Python Dependencies
- `openai==2.30.0` - For AI/Copilot features

---

## 📊 Analytics Capabilities

### Reporting & BI Features
1. **Interactive Dashboards** - Power BI reports on healthcare KPIs
2. **Patient 360 Views** - Complete patient history & metrics
3. **Financial Analytics** - Revenue, billing, reconciliation
4. **Organizational Metrics** - Provider/payer performance
5. **Time Intelligence** - Historical trend analysis
6. **AI-Powered Insights** - Data Agent with natural language queries

### Key Metrics
- Total claims & claimed amounts
- Active vs. cancelled claims
- Invoice tracking & discounts
- Coverage dates & insurance networks
- Patient demographics & contact info
- Net-to-gross billing percentages

---

## 🔐 Security & Governance

### Row-Level Security (RLS)
- **Gold RLS** - Restricts access to organization-level data
- **Silver RLS** - Restricts access to detailed transaction records
- **MetaData RLS** - Restricts access to dictionary/lookup tables

### User Roles
- Enforced via SQL Server security

---

## 📈 Deployment & Operations

### File Organization
- **.platform files** - Fabric metadata files
- **.gitignore files** - Git configuration
- **.sqlproj files** - SQL project definitions

### Versioning & Metadata
- `alm.settings.json` - Application Lifecycle Management configuration
- `lakehouse.metadata.json` - Lakehouse structural metadata
- `shortcuts.metadata.json` - OneLake shortcuts & data lineage

---

## 🚀 Development & Workflow

### dbt Workflow
1. Define models in `DBT_FHIR/Code/dbt/models/`
2. Configure profiles in `dbt-content.json`
3. Run `dbt build` with 4 parallel threads
4. Output loads to Silver schema
5. Gold tables can reference Silver

### Notebook Workflow
1. Define data sources (Lakehouse files or tables)
2. Process with PySpark
3. Transform data
4. Load to target tables
5. Chain with pipelines for orchestration

### Pipeline Workflow
1. Configure API sources in `SqlHackathon.Config.ApiConfig`
2. Define pagination in `Config.ApiPagination`
3. Create pipeline activities (Lookup → DataFlow → Load)
4. Schedule for recurring ingestion
5. Monitor via pipeline runs & logs

---

## 🔗 Integration Points

### External Systems
- **EHR/API Providers** - FHIR-compliant healthcare systems
- **OpenAI** - LLM integration for AI features
- **Power BI** - Analytics & reporting

### Internal Integration
- **Lakehouse ↔ Warehouse** - Raw to structured data
- **Notebooks ↔ Pipelines** - Orchestration & transformation
- **dbt ↔ Power BI** - Analytics model building
- **SQL DB ↔ Pipelines** - Configuration management

---

## 📝 Summary

This **Fabric Hackathon** is a comprehensive healthcare analytics platform demonstrating:

✅ **Modern Data Stack** - Microsoft Fabric's integrated analytics
✅ **Healthcare Domain** - FHIR standards-based modeling
✅ **Enterprise Patterns** - Medallion architecture (Silver/Gold layers)
✅ **Data Governance** - Security, metadata, audit trails
✅ **Automation** - Pipelines, dbt, notebooks for end-to-end orchestration
✅ **Business Intelligence** - Power BI with semantic modeling
✅ **AI Integration** - Copilot & Data Agent for intelligent exploration
✅ **Scalability** - Multi-layer processing with parallel execution

**Use Cases:**
- Patient 360 analytics for care coordination
- Claims & financial analytics for billing optimization
- Insurance network performance analysis
- Healthcare provider benchmarking
- Regulatory reporting & audit trails
- Predictive analytics on healthcare trends

---

## 🔍 Next Steps for Developers

1. **Understand Data Sources** - Review FHIR API documentation
2. **Explore Warehouse Schema** - Query Silver & Gold tables
3. **Review dbt Models** - Understand transformation logic
4. **Test Pipelines** - Run data ingestion & transformation
5. **Build Reports** - Create Power BI dashboards from semantic model
6. **Tune Performance** - Optimize queries & indexing
7. **Implement Security** - Configure row-level security policies
8. **Deploy to Production** - Use ALM settings for release management

---
This document provides a comprehensive understanding of the Fabric Hackathon project, covering architecture, components, data flow, and technical configurations. It serves as a guide for developers to navigate and contribute effectively to the project.