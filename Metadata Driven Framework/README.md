# Fabric Hackathon - Healthcare Data Analytics Platform

## Introduction

**Fabric Hackathon** is a Microsoft Fabric-based end-to-end healthcare analytics platform built on FHIR (Fast Healthcare Interoperability Resources) standards. This project demonstrates modern data engineering practices including:

- **Multi-layer data architecture** (Raw → Silver → Gold layers)
- **FHIR healthcare data modeling** with support for patients, claims, coverage, and organizations
- **Automated data pipelines** for ingestion, transformation, and aggregation
- **Power BI semantic models** for enterprise analytics and reporting
- **Row-level security** for data governance and privacy
- **AI-powered insights** using Copilot & Data Agent capabilities

### Key Objectives
✅ Ingest healthcare data from FHIR-compliant APIs
✅ Transform and standardize data following healthcare best practices
✅ Create analytical datasets for business intelligence
✅ Provide 360° patient views with comprehensive analytics
✅ Enable data-driven decision-making for healthcare organizations

---

## Architecture Overview

The platform follows a **medallion architecture**:

```
Raw Data (Lakehouse) → Silver Layer (Cleaned) → Gold Layer (Analytics) → Power BI (Reporting)
```

**Core Components:**
- **HackathonWH.Warehouse** - SQL Data Warehouse (medallion architecture)
- **DBT_FHIR.DataBuildToolJob** - Data transformation orchestration
- **HackathonLh.Lakehouse** - Raw data storage
- **PL_Master_Hackathon.DataPipeline** - API ingestion pipeline
- **FHIRModel.SemanticModel** - Power BI analytics model
- **NB_*.Notebook** - PySpark data processing jobs
- **SqlHackathon.SQLDatabase** - Configuration & metadata

📖 **For complete architecture details, see [PROJECT_UNDERSTANDING.md](PROJECT_UNDERSTANDING.md)**

---

## Getting Started

### Prerequisites

1. **Microsoft Fabric Workspace Access**
   - Admin or Contributor role required

2. **Tools & Software**
   - Visual Studio Code or Visual Studio
   - SQL Server Management Studio (SSMS) - optional
   - Git for version control
   - Python 3.8+ for local development

3. **Dependencies**
   ```
   - Microsoft Fabric (Workspace provisioned)
   - SQL Data Warehouse endpoint: HackathonWH
   - Lakehouse storage: HackathonLh
   - OneLake integration enabled
   - dbt Core installed locally (optional)
   - Python packages: openai==2.30.0
   ```

### Project Setup

#### Step 1: Clone & Explore
```bash
git clone <repository-url>
cd FabricHackathon
```

#### Step 2: Connect to Fabric
1. Open Microsoft Fabric workspace
2. Navigate to your workspace: ``
3. Open each item (Warehouse, Semantic Model, Pipelines, Notebooks)

#### Step 3: Verify Data Connections
- **HackathonWH Warehouse** - Should be accessible
- **SqlHackathon Database** - Contains API configuration
- **HackathonLh Lakehouse** - Ready for raw data ingestion

#### Step 4: Review Core Tables
```sql
-- Check Silver layer tables
SELECT * FROM HackathonWH.Silver.Patient;
SELECT * FROM HackathonWH.Silver.Claim;
SELECT * FROM HackathonWH.Silver.Coverage;

-- Check Gold layer analytics
SELECT * FROM HackathonWH.Gold.Patient360;
SELECT * FROM HackathonWH.Gold.ClaimSummary;
```

---

## Workflow & Operations

### 1. Data Ingestion

**Configure API Sources:**
```sql
-- In SqlHackathon database
INSERT INTO Config.ApiConfig 
(ApiName, Endpoint, AuthType, Credentials, IsActive)
VALUES ('Patient API', 'https://api.provider.com/fhir/Patient', 'Bearer', '...', 1);
```

**Run Ingestion Pipeline:**
1. Open `PL_Master_Hackathon.DataPipeline`
2. Click "Run" to trigger data extraction
3. Monitor pipeline run status
4. Check raw data in `HackathonLh/Files/raw/fhir/`

### 2. Data Transformation

**Process FHIR Data (PySpark Notebook):**
```python
# Edit NB_DataProcessing.Notebook
WORKSPACE_ID = ""
LAKEHOUSE_ID = ""
JSON_FILE_PATH = "Files/raw/fhir/[resource]/[timestamp]_[ResourceType].json"
TARGET_TABLE = "[ResourceType]"

# Run the notebook to parse & load data
```

**Run dbt Transformations:**
```bash
# Locally (optional)
dbt run --profiles-dir . --project-dir ./DBT_FHIR/Code/dbt

# Via Notebook
# Edit NB_RunDBT.Notebook and execute
```

**Or run dbt job via Fabric UI:**
1. Open `DBT_FHIR.DataBuildToolJob`
2. Click "Run job"
3. Transformations output to Silver schema
4. Gold layer tables depend on Silver

### 3. Analytics & Reporting

**Access Power BI Model:**
1. Open `FHIRModel.SemanticModel`
2. Review tables, relationships, and measures
3. Publish to Power BI Service

**Build Reports:**
1. Create new Power BI report
2. Connect to `FHIRModel.SemanticModel`
3. Build dashboards from:
   - **Patient360** - Patient analytics
   - **ClaimSummary** - Claims metrics
   - **FinancialSummaryByOrg** - Organization performance
   - **Date** - Time dimension

### 4. Metadata & Configuration

**Update Metadata:**
```sql
-- In SqlHackathon
INSERT INTO MetaData.Dictionary (EntityKey, EntityValue, Description)
VALUES ('ClaimStatus', 'active', 'Active insurance claims');
```

---

## Key Tables & Schemas

### Silver Layer (Source of Truth)
| Table | Content |
|-------|---------|
| Patient | Demographics, identifiers, contact info |
| Claim | Insurance claims with status, dates, amounts |
| Coverage | Insurance policies, networks, dates |
| Organization | Providers, payors, healthcare organizations |
| ClaimItem | Line items within claims |
| ClaimDiagnosis | Clinical diagnoses per claim |
| ClaimProcedure | Medical procedures per claim |
| ClaimCareTeam | Care team members per claim |

### Gold Layer (Analytics)
| Table | Content |
|-------|---------|
| **Patient360** | Comprehensive patient view (demographic + claims + coverage) |
| **ClaimSummary** | Aggregated claim metrics |
| **FinancialSummaryByOrg** | Organization-level financial KPIs |
| **InvoiceReconciliation** | Invoice tracking & reconciliation |
| **LastWorkingDayOfMonth** | Calendar helper for month-end reports |

---

## Build & Testing

### Database Schema Validation
```sql
-- Test Silver layer completeness
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'Silver'
ORDER BY TABLE_NAME;

-- Test Gold layer completeness
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'Gold'
ORDER BY TABLE_NAME;
```

### Pipeline Validation
1. Monitor `PL_Master_Hackathon.DataPipeline` runs
2. Check for failed activities
3. Verify row counts in target tables
4. Review error logs in `SqlHackathon.Log.StoredProcedures`

### dbt Testing
```bash
# Local dbt tests
dbt test --select [model_name]

# Via Fabric after running job
SELECT COUNT(*) FROM Silver.[table_name] WHERE [key_column] IS NULL
```

### Semantic Model Validation
1. Open `FHIRModel.SemanticModel`
2. Verify all relationships are active
3. Test DAX calculations in Power BI Desktop
4. Check for invalid column references

---

## Performance Optimization

### dbt Configuration
- **Threads:** 4 parallel execution threads (in `dbt-content.json`)
- **Refresh:** Full refresh enabled for data consistency
- **Schema:** Targets Silver layer for optimal performance

### Query Optimization
- Gold tables are denormalized for query performance
- Patient360 includes pre-computed aggregations
- Indexes recommended on key columns (patient_id, claim_id)

### Spark Configuration
- Configure in `HackathonEnv.Environment/Setting/Sparkcompute.yml`
- Scale compute based on data volume
- Monitor notebook execution times

---

## Security & Access Control

### Row-Level Security (RLS)
Configured in `HackathonWH/Security/`:
- **Gold.sql** - Restricts Gold layer access
- **Silver.sql** - Restricts Silver layer access
- **MetaData.sql** - Restricts dictionary access

**Apply RLS:**
```sql
-- Example: Restrict to organization
CREATE SECURITY POLICY [OrganizationPolicy]
ADD FILTER PREDICATE [security].[OrgFilter]([OrganizationId])
ON [Gold].[FinancialSummaryByOrg];
```

### User Access
- Role: Admin/Contributor
- Additional users: Configure in `SqlHackathon/Security/`

---

## Troubleshooting

### Common Issues

**1. Pipeline Fails - API Connection Error**
- Check API credentials in `Config.ApiConfig`
- Verify endpoint URL and authentication type
- Test connection manually

**2. dbt Job Fails - Symbol Not Found**
- Run `dbt deps` to update dependencies
- Verify HackathonWH warehouse is accessible
- Check dbt profile configuration in `dbt-content.json`

**3. Notebook Error - File Not Found**
- Verify ABFSS path: `abfss://[workspace-id]@onelake.dfs.fabric.microsoft.com`
- Check file exists in Lakehouse `Files/` directory
- Confirm workspace and lakehouse IDs match

**4. Power BI - Semantic Model Won't Refresh**
- Verify DirectLake connection to HackathonWH
- Check data warehouse is online
- Review Power BI refresh history for errors

### Monitoring & Logs

**Monitor Fabric Jobs:**
- Workspace → Monitor Hub → Data pipelines / Notebooks
- Check activity logs for errors
- Review row counts after transformations

**Check SQL Logs:**
```sql
SELECT TOP 100 * FROM SqlHackathon.Log.ErrorLog 
ORDER BY LogDateTime DESC;
```

---

## Contributing

### Development Workflow

1. **Create Feature Branch**
   ```bash
   git checkout -b feature/[feature-name]
   ```

2. **Make Changes**
   - Add/modify SQL in warehouse
   - Update dbt models
   - Test notebooks
   - Update semantic model

3. **Test Changes**
   - Run pipelines
   - Execute notebooks
   - Validate data quality
   - Test Power BI reports

4. **Submit PR**
   - Document changes in PR description
   - Include before/after metrics
   - Get code review

### Code Standards

- **SQL:** Use `Silver` prefix for transformation tables
- **dbt:** Follow dbt best practices (models, tests, docs)
- **Python:** Follow PEP 8 style guide
- **Power BI:** Use consistent naming (PascalCase for measures)

### Documentation

- Update [PROJECT_UNDERSTANDING.md](PROJECT_UNDERSTANDING.md) for architectural changes
- Document new tables in schema comments
- Add dbt model descriptions
- Update Power BI measure documentation

---

## Resources

### Documentation
- [PROJECT_UNDERSTANDING.md](PROJECT_UNDERSTANDING.md) - Complete architecture & setup
- [Microsoft Fabric Docs](https://learn.microsoft.com/en-us/fabric/)
- [FHIR Standard](https://www.hl7.org/fhir/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Power BI Docs](https://docs.microsoft.com/en-us/power-bi/)

### Related Projects
- Microsoft Fabric Samples
- Healthcare Analytics templates
- FHIR conformance suites

---

## License & Support

**Last Updated:** May 2026

---

## Quick Start Checklist

- [ ] Access Fabric Workspace
- [ ] Review PROJECT_UNDERSTANDING.md
- [ ] Connect to HackathonWH warehouse
- [ ] Explore Silver & Gold tables
- [ ] Run a sample pipeline
- [ ] Execute a transformation notebook
- [ ] Open Power BI semantic model
- [ ] Build a test report
- [ ] Review security settings
- [ ] Configure for your use case