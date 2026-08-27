import type { ExamId } from './exams';

export interface Flashcard {
  id: string;
  exam: ExamId;
  domainId: string;
  term: string;
  definition: string;
  /** The thing people actually get wrong about this term. */
  gotcha?: string;
  tags: string[];
}

export const FLASHCARDS: Flashcard[] = [
  // ============================================================ DP-600
  // ---- Maintain a data analytics solution
  {
    id: 'fc-600-001',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    term: 'Workspace roles',
    definition:
      'Four roles, least to most privileged: Viewer, Contributor, Member, Admin. Contributor can create and edit items; Member can additionally share items and add other members; Admin can add other admins, update the workspace and delete it.',
    gotcha:
      'Viewer does NOT get data access to a lakehouse or warehouse by default — reading through the SQL analytics endpoint still needs ReadData on the item.',
    tags: ['security', 'roles'],
  },
  {
    id: 'fc-600-002',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    term: 'Item permissions (Read / ReadData / ReadAll / Build)',
    definition:
      'Read lets a user see item metadata and connect. ReadData grants access through the SQL analytics endpoint. ReadAll grants access to the underlying OneLake files via Apache Spark. Build lets a user create new content from a semantic model.',
    gotcha:
      'Sharing a lakehouse without ticking the extra boxes grants Read only — the user sees the item but every query returns a permission error.',
    tags: ['security', 'sharing'],
  },
  {
    id: 'fc-600-003',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    term: 'Endorsement',
    definition:
      'A trust signal attached to an item: Promoted (any contributor can apply), Certified (only users authorised by the tenant admin) and Master data (for authoritative reference data).',
    gotcha:
      'Endorsement is discovery metadata only — it grants nobody any permission.',
    tags: ['governance'],
  },
  {
    id: 'fc-600-004',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    term: 'Sensitivity labels',
    definition:
      'Microsoft Purview Information Protection labels applied to Fabric items. Labels flow downstream: a label on a lakehouse is inherited by the semantic model and reports built on it, and it persists into exported files.',
    gotcha:
      'Downstream inheritance is automatic; upstream is not. Labelling a report never labels the model it reads.',
    tags: ['governance', 'purview'],
  },
  {
    id: 'fc-600-005',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    term: 'Git integration',
    definition:
      'Connects a workspace to a single branch of an Azure DevOps or GitHub repository. Supported items are serialised into folders; you commit from and update the workspace through the Source control pane.',
    gotcha:
      'The relationship is one workspace to one branch. For a feature-branch workflow each developer needs their own workspace.',
    tags: ['lifecycle', 'cicd'],
  },
  {
    id: 'fc-600-006',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    term: 'Deployment pipelines',
    definition:
      'Promote content between stages (classically Development, Test, Production; up to ten stages). Each stage is backed by a workspace. Deployment rules repoint data sources and parameters so promoted items hit the right environment.',
    gotcha:
      'Deployment rules are configured on the TARGET stage, and they only apply to content that was deployed through the pipeline.',
    tags: ['lifecycle', 'cicd'],
  },
  {
    id: 'fc-600-007',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    term: 'XMLA endpoint',
    definition:
      'An Analysis Services protocol endpoint onto semantic models hosted on Fabric or Premium capacity. Read-only supports tools such as DAX Studio and Excel; Read/Write additionally allows external tools such as Tabular Editor to alter the model.',
    gotcha:
      'Read/Write is a capacity setting, and it is off by default. Publishing over an XMLA-modified model from Power BI Desktop can overwrite external changes.',
    tags: ['lifecycle', 'tools'],
  },
  {
    id: 'fc-600-008',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    term: 'Lineage view and impact analysis',
    definition:
      'Lineage view shows the chain from source through lakehouse, semantic model and report. Impact analysis lists the downstream artefacts a change would affect and lets you notify their owners.',
    tags: ['governance', 'lifecycle'],
  },
  {
    id: 'fc-600-009',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    term: 'OneLake data access roles',
    definition:
      'Folder-level security defined on a lakehouse. A role lists the folders or tables its members can read, giving row-free, path-based access control that applies to Spark and to OneLake APIs.',
    gotcha:
      'This is coarse, path-level security. Row filtering still needs SQL row-level security or semantic model RLS.',
    tags: ['security', 'onelake'],
  },
  {
    id: 'fc-600-010',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    term: 'Where RLS is enforced',
    definition:
      'Three separate places: a SQL security policy on the warehouse or SQL analytics endpoint, roles with DAX filter expressions inside an Import or DirectQuery semantic model, and OneLake data access roles for file paths.',
    gotcha:
      'A Direct Lake model does NOT inherit SQL RLS from the endpoint. If SQL RLS is present, the model falls back to DirectQuery so the policy can be applied.',
    tags: ['security', 'rls', 'direct-lake'],
  },

  // ---- Prepare data
  {
    id: 'fc-600-011',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'OneLake',
    definition:
      'One logical, tenant-wide data lake provisioned automatically with the tenant. It is built on ADLS Gen2 and speaks the ADLS Gen2 API, stores tables as Delta Parquet, and every Fabric workload reads and writes the same copy.',
    gotcha:
      'One OneLake per tenant — you cannot create a second one, and you cannot turn it off.',
    tags: ['onelake', 'storage'],
  },
  {
    id: 'fc-600-012',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'Shortcut',
    definition:
      'A pointer to data that lives elsewhere, with no copy and no scheduled refresh. Internal shortcuts target other OneLake locations; external shortcuts target ADLS Gen2, Amazon S3, S3-compatible stores, Google Cloud Storage or Dataverse.',
    gotcha:
      'A shortcut is read-through: data is always current, but query performance depends on the remote store, and deleting the shortcut never deletes the source.',
    tags: ['onelake', 'ingest'],
  },
  {
    id: 'fc-600-013',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'Mirroring',
    definition:
      'Near-real-time, low-cost replication of an operational database into OneLake as Delta tables. Supported sources include Azure SQL Database, Azure SQL Managed Instance, Azure Cosmos DB, Azure Database for PostgreSQL, Snowflake and Fabric-mirrored databases.',
    gotcha:
      'The mirrored copy is read-only in Fabric. Write back to the source system, not to the mirror.',
    tags: ['ingest', 'mirroring'],
  },
  {
    id: 'fc-600-014',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'Lakehouse vs Warehouse',
    definition:
      'A lakehouse is files plus Delta tables, written with Spark or pipelines, and exposes a read-only SQL analytics endpoint. A warehouse is a full T-SQL engine with DDL and DML — you can INSERT, UPDATE, DELETE and create stored procedures.',
    gotcha:
      'If a scenario says "the team only knows T-SQL and needs to write data", the answer is a warehouse, because the lakehouse SQL endpoint is read-only.',
    tags: ['architecture', 'warehouse', 'lakehouse'],
  },
  {
    id: 'fc-600-015',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'SQL analytics endpoint',
    definition:
      'A read-only T-SQL surface generated automatically over the Delta tables in a lakehouse or mirrored database. It supports SELECT, views, functions and security objects, but not INSERT, UPDATE or DELETE.',
    gotcha:
      'You CAN create views and a security policy on the endpoint even though you cannot modify data through it.',
    tags: ['warehouse', 'lakehouse', 'tsql'],
  },
  {
    id: 'fc-600-016',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'Delta Lake',
    definition:
      'The open table format underneath every Fabric table: Parquet data files plus a _delta_log transaction log that provides ACID transactions, schema enforcement, time travel and MERGE.',
    gotcha:
      'Time travel is bounded by what VACUUM has kept. Vacuum with a short retention and old versions become unreadable.',
    tags: ['delta', 'storage'],
  },
  {
    id: 'fc-600-017',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'V-Order',
    definition:
      'A write-time optimisation applied to Parquet files — sorting, row-group distribution, dictionary encoding and compression — that makes files far cheaper for the VertiPaq engine to read. It keeps files fully Parquet-compliant.',
    gotcha:
      'V-Order costs write time. For write-heavy staging or bronze tables it is often worth disabling; for gold tables feeding Direct Lake, leave it on.',
    tags: ['performance', 'delta', 'direct-lake'],
  },
  {
    id: 'fc-600-018',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'OPTIMIZE and VACUUM',
    definition:
      'OPTIMIZE compacts many small Parquet files into fewer large ones (bin-compaction) and can apply V-Order and Z-ordering. VACUUM permanently deletes data files no longer referenced by the log, outside the retention window (default seven days).',
    gotcha:
      'They solve different problems. OPTIMIZE fixes read performance; VACUUM reclaims storage and is what actually removes the old files.',
    tags: ['performance', 'delta', 'maintenance'],
  },
  {
    id: 'fc-600-019',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'Medallion architecture',
    definition:
      'Bronze holds raw, append-only landed data. Silver holds cleaned, conformed, deduplicated data. Gold holds business-ready, aggregated, star-schema data that serves semantic models and reports.',
    tags: ['architecture', 'patterns'],
  },
  {
    id: 'fc-600-020',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'Dataflow Gen2',
    definition:
      'Power Query Online at Fabric scale. Low-code M transformations with an explicit output destination (lakehouse, warehouse, KQL database or SQL database), plus optional staging for large sources.',
    gotcha:
      'Without a configured data destination a Gen2 dataflow computes results but persists nothing you can query downstream.',
    tags: ['ingest', 'transform', 'dataflow'],
  },
  {
    id: 'fc-600-021',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'Star schema',
    definition:
      'Narrow, numeric fact tables joined to wide, descriptive dimension tables through single-column surrogate keys. It is the shape both the VertiPaq engine and DAX are designed for.',
    gotcha:
      'Snowflaking dimensions adds joins and slows DAX. Flatten dimensions in the gold layer instead.',
    tags: ['modelling', 'transform'],
  },
  {
    id: 'fc-600-022',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'Slowly changing dimensions',
    definition:
      'Type 1 overwrites the attribute and keeps no history. Type 2 inserts a new row per change with surrogate key, start date, end date and a current flag, preserving full history.',
    gotcha:
      'Type 2 means the fact table must join on the surrogate key, not the business key, or every historical fact snaps to the current version.',
    tags: ['modelling', 'transform', 'scd'],
  },
  {
    id: 'fc-600-023',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'Warehouse table constraints',
    definition:
      'Fabric Warehouse supports PRIMARY KEY, UNIQUE and FOREIGN KEY only as NOT ENFORCED metadata. They inform the optimiser and modelling tools; the engine never validates them.',
    gotcha:
      'Declaring a NOT ENFORCED primary key on a column that actually holds duplicates produces silently wrong query results.',
    tags: ['warehouse', 'tsql', 'gotcha'],
  },
  {
    id: 'fc-600-024',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'COPY INTO',
    definition:
      'The high-throughput bulk-load T-SQL statement for Fabric Warehouse. It reads PARQUET or CSV from ADLS Gen2 or Blob storage, supports wildcards, an ERRORFILE location and several authentication methods.',
    tags: ['warehouse', 'ingest', 'tsql'],
  },
  {
    id: 'fc-600-025',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'Cross-database queries',
    definition:
      'Inside one workspace you can query across warehouses and lakehouse SQL analytics endpoints with three-part names (database.schema.table), with no data movement, by adding the other item to your query editor.',
    gotcha:
      'Three-part naming works within a workspace. Reaching another workspace needs a shortcut or a mirrored copy.',
    tags: ['warehouse', 'tsql'],
  },
  {
    id: 'fc-600-026',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    term: 'Small file problem',
    definition:
      'Thousands of tiny Parquet files force the engine to open, list and decode far more metadata than data, wrecking scan performance. Caused by frequent micro-batches and over-partitioning.',
    gotcha:
      'The fix is OPTIMIZE plus fewer, larger partitions — not a bigger Spark pool.',
    tags: ['performance', 'delta'],
  },

  // ---- Semantic models
  {
    id: 'fc-600-027',
    exam: 'DP-600',
    domainId: 'dp600-models',
    term: 'Direct Lake',
    definition:
      'A storage mode that loads Delta Parquet columns from OneLake straight into the VertiPaq engine on demand, giving near-Import query speed with no refresh or data duplication.',
    gotcha:
      'It requires Fabric capacity and Delta tables in OneLake. Columns are paged into memory on first use, so the first query after a data change is slower.',
    tags: ['semantic-model', 'direct-lake', 'storage-mode'],
  },
  {
    id: 'fc-600-028',
    exam: 'DP-600',
    domainId: 'dp600-models',
    term: 'Direct Lake fallback',
    definition:
      'When a Direct Lake model hits something it cannot serve — a SQL view, SQL row-level security, or a capacity guardrail such as rows per table — it silently falls back to DirectQuery over the SQL analytics endpoint. Controlled by the DirectLakeBehavior property: Automatic, DirectLakeOnly or DirectQueryOnly.',
    gotcha:
      'Set DirectLakeOnly while testing so fallback surfaces as an error instead of hiding as a slow report.',
    tags: ['semantic-model', 'direct-lake', 'performance'],
  },
  {
    id: 'fc-600-029',
    exam: 'DP-600',
    domainId: 'dp600-models',
    term: 'Import / DirectQuery / Dual',
    definition:
      'Import caches a compressed copy in memory (fastest, needs refresh). DirectQuery leaves data at source and translates DAX to native queries (always current, slowest). Dual lets a table behave as either, chosen per query, and is the standard mode for dimensions in a composite model.',
    gotcha:
      'Dual exists to stop a DirectQuery fact table dragging Import dimensions into limited relationships.',
    tags: ['semantic-model', 'storage-mode'],
  },
  {
    id: 'fc-600-030',
    exam: 'DP-600',
    domainId: 'dp600-models',
    term: 'Composite model',
    definition:
      'One semantic model mixing storage modes and sources — for example an Import dimension, a DirectQuery fact, and a DirectQuery connection to another published semantic model.',
    gotcha:
      'Relationships that span sources become limited relationships: they are evaluated at query time and cannot use the fast VertiPaq path.',
    tags: ['semantic-model', 'storage-mode'],
  },
  {
    id: 'fc-600-031',
    exam: 'DP-600',
    domainId: 'dp600-models',
    term: 'Calculation group',
    definition:
      'A set of calculation items, each a DAX expression using SELECTEDMEASURE(), that can be applied to any measure. Replaces dozens of near-identical time-intelligence measures with one reusable set.',
    gotcha:
      'Calculation items apply in precedence order; two groups touching the same measure need explicit precedence or results are unpredictable.',
    tags: ['semantic-model', 'dax'],
  },
  {
    id: 'fc-600-032',
    exam: 'DP-600',
    domainId: 'dp600-models',
    term: 'Dynamic format strings',
    definition:
      'A DAX expression that returns the format string for a measure or calculation item, so the same measure can render as currency, percentage or plain number depending on context.',
    tags: ['semantic-model', 'dax'],
  },
  {
    id: 'fc-600-033',
    exam: 'DP-600',
    domainId: 'dp600-models',
    term: 'Large semantic model storage format',
    definition:
      'Stores the model in a paged, chunked format so it can grow beyond the default in-memory limit and enables read/write XMLA operations. Configured per model, or as a workspace default.',
    gotcha:
      'It is a prerequisite for models over the default size limit and for incremental refresh partitions managed via XMLA.',
    tags: ['semantic-model', 'performance'],
  },
  {
    id: 'fc-600-034',
    exam: 'DP-600',
    domainId: 'dp600-models',
    term: 'Incremental refresh',
    definition:
      'Partitions a table by date using the reserved RangeStart and RangeEnd Power Query parameters, then refreshes only recent partitions. Optionally detects data changes and keeps real-time data in a DirectQuery partition.',
    gotcha:
      'RangeStart and RangeEnd must be date/time typed and the filter must be applied so the source can fold it — otherwise every partition scans the whole table.',
    tags: ['semantic-model', 'performance', 'refresh'],
  },
  {
    id: 'fc-600-035',
    exam: 'DP-600',
    domainId: 'dp600-models',
    term: 'RLS vs OLS',
    definition:
      'Row-level security filters rows via a DAX expression on a role. Object-level security hides whole tables or columns from a role, so they vanish from the field list and any dependent visual errors.',
    gotcha:
      'OLS is defined with external tools (Tabular Editor), not in the Power BI Desktop RLS dialog.',
    tags: ['semantic-model', 'security', 'rls'],
  },
  {
    id: 'fc-600-036',
    exam: 'DP-600',
    domainId: 'dp600-models',
    term: 'Filter context vs row context',
    definition:
      'Filter context is the set of filters applied to the model by visuals, slicers and CALCULATE. Row context is the current row during iteration (calculated columns, iterators such as SUMX). Context transition — triggered by CALCULATE, including implicit measure calls — turns row context into filter context.',
    gotcha:
      'A measure referenced inside SUMX is implicitly wrapped in CALCULATE, so context transition happens whether you wrote it or not.',
    tags: ['dax'],
  },
  {
    id: 'fc-600-037',
    exam: 'DP-600',
    domainId: 'dp600-models',
    term: 'CALCULATE',
    definition:
      'Evaluates an expression in a modified filter context. Filter arguments replace filters on the same column by default; use KEEPFILTERS to intersect instead, and REMOVEFILTERS or ALL to clear.',
    tags: ['dax'],
  },
  {
    id: 'fc-600-038',
    exam: 'DP-600',
    domainId: 'dp600-models',
    term: 'USERELATIONSHIP vs TREATAS',
    definition:
      'USERELATIONSHIP activates an existing inactive physical relationship inside CALCULATE. TREATAS applies a table of values as a filter on a target column, creating a virtual relationship where no physical one exists.',
    tags: ['dax', 'modelling'],
  },
  {
    id: 'fc-600-039',
    exam: 'DP-600',
    domainId: 'dp600-models',
    term: 'Performance Analyzer, DAX Query View, Best Practice Analyzer',
    definition:
      'Performance Analyzer times each visual and exposes its generated DAX. DAX Query View runs and tunes DAX in Power BI Desktop. Best Practice Analyzer (Tabular Editor) checks the model against modelling rules such as missing formatting or unsuitable data types.',
    tags: ['semantic-model', 'performance', 'tools'],
  },
  {
    id: 'fc-600-040',
    exam: 'DP-600',
    domainId: 'dp600-models',
    term: 'User-defined aggregations',
    definition:
      'A pre-aggregated Import table mapped to a detail DirectQuery table. Queries that can be answered at the aggregate grain hit memory; anything finer transparently drills through to the source.',
    gotcha:
      'The aggregation table must be Import, and the detail table Dual or DirectQuery, or the mapping never gets used.',
    tags: ['semantic-model', 'performance'],
  },

  // ============================================================ DP-700
  // ---- Implement and manage an analytics solution
  {
    id: 'fc-700-001',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Fabric F SKUs',
    definition:
      'Capacity is sold as F SKUs whose number IS the capacity units: F2, F4, F8, F16, F32, F64, F128, F256, F512, F1024, F2048. An F64 provides 64 CUs of compute for every workload in the tenant capacity.',
    gotcha:
      'Every workload — Spark, warehouse, pipelines, Power BI — draws from the same CU pool.',
    tags: ['capacity', 'sku', 'cu-math'],
  },
  {
    id: 'fc-700-002',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'CU math',
    definition:
      'Consumption is measured in CU-seconds. An F SKU delivers (SKU number) CU-seconds every second, so an F64 supplies 64 x 30 = 1,920 CU-seconds per 30-second window and 64 x 86,400 = 5,529,600 CU-seconds per day.',
    gotcha:
      'Utilisation percentages in the Capacity Metrics app are CU-seconds consumed divided by CU-seconds available in that window — not CPU percent.',
    tags: ['capacity', 'cu-math'],
  },
  {
    id: 'fc-700-003',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Smoothing',
    definition:
      'Fabric spreads recorded consumption over time so short spikes do not throttle you. Background operations (refreshes, Spark jobs, pipelines) are smoothed over 24 hours; interactive operations are smoothed over a minimum of 5 minutes.',
    gotcha:
      'Smoothing is why a job that finished hours ago can still be the reason you are throttled now.',
    tags: ['capacity', 'cu-math', 'throttling'],
  },
  {
    id: 'fc-700-004',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Throttling stages',
    definition:
      'Based on future smoothed consumption (carry-forward): under 10 minutes, overage protection — everything still runs. 10 to 60 minutes, interactive delay (requests delayed ~20 seconds). 60 minutes to 24 hours, interactive rejection. Over 24 hours, background rejection — everything is rejected.',
    gotcha:
      'Background jobs keep running through the interactive stages. Only the final stage rejects them.',
    tags: ['capacity', 'throttling'],
  },
  {
    id: 'fc-700-005',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Bursting',
    definition:
      'Fabric may temporarily run a job using more CUs than the SKU nominally provides, so the job finishes faster. The extra consumption is still charged and is repaid through smoothing.',
    gotcha:
      'Bursting is not free headroom — it is borrowing against the next 24 hours.',
    tags: ['capacity', 'cu-math'],
  },
  {
    id: 'fc-700-006',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'F64 threshold',
    definition:
      'F64 is the Power BI P1 equivalent (F128 = P2, F256 = P3). At F64 and above, users with a free licence can view content in the workspace; below F64 every consumer needs a Power BI Pro or PPU licence.',
    gotcha:
      'The free-viewer benefit is per capacity size, not per tenant. Dropping from F64 to F32 to save money instantly breaks access for free users.',
    tags: ['capacity', 'sku', 'licensing'],
  },
  {
    id: 'fc-700-007',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Trial capacity',
    definition:
      'The Fabric trial gives a 60-day capacity with the compute of an F64 (64 CUs) plus OneLake storage, per user, for evaluation.',
    gotcha:
      'Trial capacity cannot be used for production and workspaces on it stop working when the trial ends — move them to a real capacity first.',
    tags: ['capacity', 'sku'],
  },
  {
    id: 'fc-700-008',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Pause and resume',
    definition:
      'Pausing an F capacity stops compute billing; OneLake storage is still billed. Any unbilled smoothed consumption is settled immediately on pause, and everything in the capacity becomes unavailable until you resume.',
    tags: ['capacity', 'cost'],
  },
  {
    id: 'fc-700-009',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Domains',
    definition:
      'A tenant-level grouping of workspaces along business lines (Finance, Sales), with optional subdomains. Domain admins can set domain-scoped defaults and the data hub can be filtered by domain.',
    gotcha:
      'A workspace belongs to at most one domain. Domains organise governance; they are not a security boundary.',
    tags: ['governance', 'workspace'],
  },
  {
    id: 'fc-700-010',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Spark starter pool vs custom pool',
    definition:
      'Starter pools are pre-warmed clusters that give a session in a few seconds with default node sizes. Custom pools let you fix node size, autoscale bounds and dynamic executor allocation, but a session on them must cold-start.',
    gotcha:
      'Attaching a custom environment or custom libraries also forfeits the pre-warmed start.',
    tags: ['spark', 'performance'],
  },
  {
    id: 'fc-700-011',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Environment item',
    definition:
      'A reusable Fabric item bundling Spark compute settings, Spark properties, and public or custom libraries. Attach it to notebooks and Spark job definitions, or set it as the workspace default.',
    gotcha:
      'Library changes need the environment to be published before sessions pick them up.',
    tags: ['spark', 'workspace'],
  },
  {
    id: 'fc-700-012',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'High concurrency mode',
    definition:
      'Lets multiple notebooks share one Spark session (and its CUs) when they use the same environment and identity, cutting both start-up time and consumption.',
    tags: ['spark', 'performance', 'capacity'],
  },
  {
    id: 'fc-700-013',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Pipeline vs notebook vs Airflow job',
    definition:
      'Pipelines: low-code control flow, connectors, scheduling and event triggers. Notebooks: code-first Spark transformation, and orchestration via notebookutils when logic is dynamic. Apache Airflow jobs: managed Airflow for teams that already own Python DAGs or need complex dependency graphs.',
    tags: ['orchestration'],
  },
  {
    id: 'fc-700-014',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Event-based triggers',
    definition:
      'Pipelines can start on a schedule or on an event. Fabric event triggers are backed by Activator and can fire on OneLake events (file created or deleted), Azure Blob Storage events, or Fabric workspace item events.',
    gotcha:
      'File-arrival triggers are event-driven, not polling — do not model them as a one-minute schedule.',
    tags: ['orchestration', 'activator'],
  },
  {
    id: 'fc-700-015',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Dynamic content and parameters',
    definition:
      'Pipeline expressions use @pipeline(), @activity(), @variables() and @item() inside dynamic content. Parameters are set at run time and passed down; variables are mutable within a run via Set variable.',
    gotcha:
      'ForEach with sequential unchecked runs in parallel (default batch of 20) — Set variable inside a parallel ForEach is a race condition.',
    tags: ['orchestration', 'pipelines'],
  },
  {
    id: 'fc-700-016',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Database projects',
    definition:
      'A SQL project (SDK-style .sqlproj) capturing warehouse schema as code. Build produces a dacpac, and publish deploys a declarative diff — the state-based counterpart to migration scripts.',
    tags: ['lifecycle', 'warehouse', 'cicd'],
  },
  {
    id: 'fc-700-017',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Dynamic data masking',
    definition:
      'A T-SQL column property that obfuscates values in query results for unprivileged users — default, email, random and partial masks. Data on disk is unchanged.',
    gotcha:
      'Masking is presentation-only. A user who can query can still infer masked values with a WHERE clause, so pair it with column-level security.',
    tags: ['security', 'warehouse', 'tsql'],
  },
  {
    id: 'fc-700-018',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Column-level vs row-level security in T-SQL',
    definition:
      'Column-level security is GRANT SELECT on specific columns (or DENY on the rest). Row-level security is an inline table-valued predicate function bound by CREATE SECURITY POLICY with a FILTER PREDICATE.',
    gotcha:
      'The RLS predicate function must be schemabinding and inline (RETURNS TABLE ... RETURN SELECT 1 ...), or CREATE SECURITY POLICY fails.',
    tags: ['security', 'warehouse', 'tsql', 'rls'],
  },
  {
    id: 'fc-700-019',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Workspace identity and trusted workspace access',
    definition:
      'A workspace identity is a managed service principal for the workspace. Trusted workspace access uses it so Fabric can reach a firewalled ADLS Gen2 account through a resource instance rule, with no public network exposure.',
    tags: ['security', 'workspace', 'networking'],
  },
  {
    id: 'fc-700-020',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    term: 'Activator',
    definition:
      'The no-code detection and action engine. It watches a stream, a Power BI visual or a Fabric event, evaluates conditions per object, and triggers an email, Teams message, Power Automate flow or Fabric item run.',
    gotcha:
      'Activator is stateful per object, so it can express "alert when this specific truck stays above 60 degrees for 10 minutes" — not just a threshold on a single reading.',
    tags: ['monitoring', 'rti', 'activator'],
  },

  // ---- Ingest and transform data
  {
    id: 'fc-700-021',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'Full load vs incremental load',
    definition:
      'A full load truncates and rewrites the target every run — simple, self-healing, expensive. An incremental load moves only rows changed since a stored high-water mark, using a timestamp, a sequence column or change data capture.',
    gotcha:
      'A high-water mark on a non-monotonic column silently drops rows. Use >= plus deduplication, or CDC.',
    tags: ['ingest', 'patterns'],
  },
  {
    id: 'fc-700-022',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'Watermark pattern',
    definition:
      'Store the last successfully processed value (usually max modified date) in a control table. Each run reads the watermark, pulls rows greater than it, and updates the watermark only after the load succeeds.',
    gotcha:
      'Update the watermark after the write commits, never before — otherwise a failed run permanently skips a window of data.',
    tags: ['ingest', 'patterns', 'pipelines'],
  },
  {
    id: 'fc-700-023',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'Idempotency',
    definition:
      'A load is idempotent when re-running it produces the same target state. Achieved with MERGE or delete-and-insert of the affected partition, rather than blind INSERT.',
    gotcha:
      'A retried pipeline with a plain INSERT doubles the data. Retries are a feature; non-idempotent writes make them a bug.',
    tags: ['ingest', 'patterns', 'reliability'],
  },
  {
    id: 'fc-700-024',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'MERGE / upsert',
    definition:
      'One statement matching source to target on a key: update matched rows, insert unmatched, optionally delete rows missing from the source. Available in Delta (Spark MERGE INTO / DeltaTable.merge) and in Fabric Warehouse T-SQL.',
    gotcha:
      'If the source can contain two rows for the same key, MERGE errors. Deduplicate to one row per key first.',
    tags: ['transform', 'delta', 'tsql'],
  },
  {
    id: 'fc-700-025',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'Copy activity vs Copy job',
    definition:
      'Copy activity is a single step inside a pipeline you orchestrate yourself. Copy job is a standalone item that handles full and incremental copy, with built-in change tracking and a guided setup, without you building the watermark logic.',
    tags: ['ingest', 'pipelines'],
  },
  {
    id: 'fc-700-026',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'Choosing a store',
    definition:
      'Lakehouse for files plus Spark and Delta tables. Warehouse for T-SQL writes, multi-table transactions and SQL-first teams. Eventhouse (KQL database) for high-volume time-series and log telemetry with sub-second queries.',
    gotcha:
      'The exam usually keys on the team skill set and the write path — not on data volume.',
    tags: ['architecture'],
  },
  {
    id: 'fc-700-027',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'Eventstream',
    definition:
      'No-code streaming ingestion: sources (Event Hubs, IoT Hub, Kafka, CDC, sample data, custom endpoint), an event-processing canvas (filter, manage fields, aggregate, join, union, expand), and destinations (eventhouse, lakehouse, derived stream, Activator, custom endpoint).',
    gotcha:
      'A lakehouse destination writes Delta in micro-batches, so it produces small files — plan table maintenance.',
    tags: ['rti', 'streaming', 'ingest'],
  },
  {
    id: 'fc-700-028',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'Windowing functions',
    definition:
      'Tumbling: fixed, non-overlapping. Hopping: fixed size, fixed hop, overlapping. Sliding: emits only when an event enters or leaves. Session: groups events separated by less than a timeout. Snapshot: groups events sharing the same timestamp.',
    gotcha:
      'Tumbling is the only one where every event belongs to exactly one window.',
    tags: ['rti', 'streaming'],
  },
  {
    id: 'fc-700-029',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'Watermark (streaming)',
    definition:
      'A threshold declaring how late an event may arrive and still be counted. In Spark structured streaming, withWatermark bounds the state store; events older than the watermark are dropped.',
    gotcha:
      'No watermark on a streaming aggregation means unbounded state — the job slows and eventually fails on memory.',
    tags: ['rti', 'streaming', 'spark'],
  },
  {
    id: 'fc-700-030',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'Structured streaming checkpoint',
    definition:
      'A durable location holding offsets and state so a restarted stream resumes exactly where it stopped, giving exactly-once semantics with an idempotent sink.',
    gotcha:
      'Two streams sharing a checkpoint location corrupt each other. One checkpoint per query, always.',
    tags: ['spark', 'streaming', 'reliability'],
  },
  {
    id: 'fc-700-031',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'KQL update policy',
    definition:
      'A rule on a target table that runs a KQL query over each ingested batch of a source table and appends the result — the eventhouse way to do transform-on-ingest into a curated table.',
    gotcha:
      'Update policies run on ingestion only. They do not backfill rows already in the source table.',
    tags: ['rti', 'kql', 'transform'],
  },
  {
    id: 'fc-700-032',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'KQL materialized view',
    definition:
      'A persisted, incrementally maintained aggregation over a source table (for example the latest row per key with arg_max, or a summarize by day) that returns instantly instead of scanning raw events.',
    tags: ['rti', 'kql', 'performance'],
  },
  {
    id: 'fc-700-033',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'OneLake availability for eventhouse',
    definition:
      'Turning it on writes KQL database tables to OneLake in Delta format as one logical copy, so Spark, the SQL endpoint and Direct Lake models can read the same telemetry without a second ingest.',
    gotcha:
      'The OneLake copy is read-only from outside the eventhouse.',
    tags: ['rti', 'onelake', 'kql'],
  },
  {
    id: 'fc-700-034',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'Handling duplicates',
    definition:
      'Deduplicate at the silver layer with a deterministic tiebreak: ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY loaded_at DESC) = 1, or dropDuplicates on a stable key in Spark.',
    gotcha:
      'dropDuplicates() with no columns compares every column, so two rows differing only by ingest timestamp both survive.',
    tags: ['transform', 'quality'],
  },
  {
    id: 'fc-700-035',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'Late-arriving dimensions',
    definition:
      'When a fact arrives before its dimension member, insert an inferred member (an unknown row keyed by the business key) so the fact keeps a valid surrogate key, then update it when the real attributes land.',
    gotcha:
      'The alternative — routing the fact to a reject table — loses data if nobody reprocesses it.',
    tags: ['transform', 'modelling', 'scd'],
  },
  {
    id: 'fc-700-036',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    term: 'Data Wrangler',
    definition:
      'A grid-based notebook tool for exploring and cleaning a pandas or Spark DataFrame that generates the equivalent PySpark code, which you then paste into the notebook.',
    tags: ['transform', 'spark', 'tools'],
  },

  // ---- Monitor and optimise
  {
    id: 'fc-700-037',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    term: 'Monitoring hub',
    definition:
      'One tenant-wide list of activity across items you have permission on — pipeline runs, dataflow refreshes, Spark applications, semantic model refreshes — with filtering by status, item type and time.',
    gotcha:
      'The Monitoring hub shows run history. Capacity consumption lives in the Capacity Metrics app.',
    tags: ['monitoring'],
  },
  {
    id: 'fc-700-038',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    term: 'Microsoft Fabric Capacity Metrics app',
    definition:
      'The app for CU consumption: the Compute page ranks items by CU-seconds, the ripple chart shows smoothed usage and overages, and the System events page shows throttling and pause/resume.',
    gotcha:
      'This is the only supported way to see which item caused throttling, and it is what an exam question means by "identify the noisy neighbour".',
    tags: ['monitoring', 'capacity'],
  },
  {
    id: 'fc-700-039',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    term: 'Query insights',
    definition:
      'Warehouse system views recording completed query history: queryinsights.exec_requests_history, plus frequently_run_queries, long_running_queries and exec_sessions_history.',
    gotcha:
      'For queries running right now, use the DMV sys.dm_exec_requests — query insights is historical.',
    tags: ['monitoring', 'warehouse', 'tsql'],
  },
  {
    id: 'fc-700-040',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    term: 'Spark application monitoring',
    definition:
      'Each notebook or Spark job definition run exposes a monitoring detail page with the job graph, driver and executor logs, resource usage over time and a link to the Spark UI for stage and task-level analysis.',
    gotcha:
      'Skew shows as one task in a stage taking far longer than the median — that is a repartition or salting problem, not a pool-size problem.',
    tags: ['monitoring', 'spark'],
  },
  {
    id: 'fc-700-041',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    term: 'Workspace monitoring',
    definition:
      'An opt-in workspace setting that provisions a read-only monitoring eventhouse and streams diagnostic logs and metrics from workspace items into it, so you can query operational telemetry with KQL.',
    tags: ['monitoring', 'kql'],
  },
  {
    id: 'fc-700-042',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    term: 'Warehouse statistics',
    definition:
      'The optimiser needs column statistics for good plans. Fabric Warehouse creates and maintains them automatically, and you can also CREATE STATISTICS or UPDATE STATISTICS manually for a column after a large load.',
    tags: ['performance', 'warehouse'],
  },
  {
    id: 'fc-700-043',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    term: 'Result set caching',
    definition:
      'Fabric Warehouse can serve a repeated identical query from a cached result set, returning in milliseconds and consuming almost no CUs. Caching is transparent and invalidated when underlying data changes.',
    tags: ['performance', 'warehouse'],
  },
  {
    id: 'fc-700-044',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    term: 'Eventhouse caching vs retention policy',
    definition:
      'The caching (hot cache) policy decides how much recent data is kept on SSD for fast queries. The retention policy decides how long data is kept at all before it is deleted.',
    gotcha:
      'Caching period longer than retention is meaningless — you cannot cache data that has already been dropped.',
    tags: ['performance', 'rti', 'kql'],
  },
  {
    id: 'fc-700-045',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    term: 'Pipeline retry and timeout',
    definition:
      'Each activity has a retry count, retry interval and timeout under General settings. Combine with failure paths (on-fail dependency) and Fail / Set variable activities to build controlled error handling.',
    gotcha:
      'Retries only help when the activity is idempotent — see the idempotency card.',
    tags: ['reliability', 'pipelines', 'orchestration'],
  },
  {
    id: 'fc-700-046',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    term: 'Spark autotune',
    definition:
      'A machine-learning feature that tunes per-query Spark configuration — shuffle partitions, broadcast join threshold, file size — from the history of previous runs of the same query.',
    tags: ['performance', 'spark'],
  },
  {
    id: 'fc-700-047',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    term: 'Partitioning trade-off',
    definition:
      'Partitioning a Delta table on a low-cardinality column that queries filter on enables partition pruning. Partitioning on a high-cardinality column (customer id, timestamp to the second) produces thousands of tiny files and makes everything slower.',
    gotcha:
      'Aim for partitions around 1 GB. Under roughly 1 TB, many Fabric tables are better off unpartitioned with OPTIMIZE and V-Order.',
    tags: ['performance', 'delta'],
  },
  {
    id: 'fc-700-048',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    term: 'Table maintenance',
    definition:
      'A lakehouse table context-menu action (and Spark equivalent) that runs OPTIMIZE, optionally applies V-Order, and runs VACUUM with a chosen retention — the routine fix for degrading Delta read performance.',
    tags: ['performance', 'delta', 'maintenance'],
  },
];

export const ALL_TAGS = Array.from(
  new Set(FLASHCARDS.flatMap((c) => c.tags))
).sort();
