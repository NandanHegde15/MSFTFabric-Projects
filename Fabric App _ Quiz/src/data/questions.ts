import type { ExamId } from './exams';

export type Difficulty = 'core' | 'tricky';

export interface Question {
  id: string;
  exam: ExamId;
  domainId: string;
  objectiveId: string;
  difficulty: Difficulty;
  stem: string;
  choices: string[];
  /** Indexes of the correct choices. Length > 1 implies a multi-select item. */
  answer: number[];
  explanation: string;
}

export const QUESTIONS: Question[] = [
  // ==================================================== DP-600 / Maintain
  {
    id: 'q-600-001',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    objectiveId: 'dp600-maintain-security',
    difficulty: 'core',
    stem: 'A colleague is added to a workspace with the Viewer role. They open the lakehouse SQL analytics endpoint and every SELECT fails with a permission error. What is the minimum change that lets them query the tables?',
    choices: [
      'Grant them the ReadData permission on the lakehouse item',
      'Change their workspace role to Contributor',
      'Grant them the ReadAll permission on the lakehouse item',
      'Add them to a OneLake data access role',
    ],
    answer: [0],
    explanation:
      'ReadData is the item permission that unlocks the SQL analytics endpoint. Contributor works but grants far more than needed, so it is not the minimum. ReadAll is for Spark and OneLake file access, and OneLake data access roles narrow file access rather than granting endpoint access.',
  },
  {
    id: 'q-600-002',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    objectiveId: 'dp600-maintain-security',
    difficulty: 'tricky',
    stem: 'A warehouse has a SQL row-level security policy. A Direct Lake semantic model is built on the SQL analytics endpoint of that warehouse. What happens when a restricted user opens a report?',
    choices: [
      'The model falls back to DirectQuery so the SQL security policy is applied',
      'Direct Lake reads the Delta files and the security policy is applied anyway',
      'The report fails with an authorisation error',
      'The user sees all rows, because Direct Lake bypasses SQL security',
    ],
    answer: [0],
    explanation:
      'SQL RLS cannot be enforced when VertiPaq reads Parquet files directly, so the engine falls back to DirectQuery over the endpoint, where the policy does apply. The report works, but at DirectQuery speed — a common cause of "why did my Direct Lake model get slow".',
  },
  {
    id: 'q-600-003',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    objectiveId: 'dp600-maintain-security',
    difficulty: 'core',
    stem: 'Which endorsement level can only be applied by users authorised by a Fabric administrator?',
    choices: ['Certified', 'Promoted', 'Master data', 'Featured'],
    answer: [0],
    explanation:
      'Any user with write permission can Promote an item. Certified is restricted to a list of users or groups configured in the tenant settings. "Featured" is not an endorsement level.',
  },
  {
    id: 'q-600-004',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    objectiveId: 'dp600-maintain-lifecycle',
    difficulty: 'core',
    stem: 'Three analysts must work on the same set of Fabric items at the same time, each on their own feature branch, without disturbing the shared development workspace. What should you configure?',
    choices: [
      'A separate workspace per analyst, each connected to its own branch of the repository',
      'One workspace connected to the repository, with each analyst committing to a different branch',
      'A deployment pipeline with one stage per analyst',
      'Git integration on the shared workspace plus deployment rules per analyst',
    ],
    answer: [0],
    explanation:
      'Fabric Git integration binds one workspace to exactly one branch. Isolated feature-branch development therefore means a workspace per developer. Deployment pipelines promote content between environments; they are not a branching mechanism.',
  },
  {
    id: 'q-600-005',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    objectiveId: 'dp600-maintain-lifecycle',
    difficulty: 'tricky',
    stem: 'A deployment pipeline promotes a semantic model from Test to Production, but the promoted model keeps pointing at the Test lakehouse. What should you configure?',
    choices: [
      'A deployment rule on the Production stage that repoints the data source',
      'A deployment rule on the Test stage that repoints the data source',
      'A parameter in the semantic model, changed manually after each deployment',
      'Git integration on the Production workspace',
    ],
    answer: [0],
    explanation:
      'Deployment rules are always defined on the target stage — the stage whose copy of the item needs different settings. A rule on Test would only affect content deployed into Test.',
  },
  {
    id: 'q-600-006',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    objectiveId: 'dp600-maintain-lifecycle',
    difficulty: 'core',
    stem: 'You want to edit a published semantic model with Tabular Editor and script the changes into source control. Which two things must be true? (Choose two.)',
    choices: [
      'The XMLA endpoint must be set to Read/Write on the capacity',
      'The workspace must be on Fabric or Premium capacity',
      'The semantic model must use Import storage mode',
      'The model must have the large semantic model storage format enabled',
    ],
    answer: [0, 1],
    explanation:
      'XMLA read/write requires a Fabric or Premium capacity workspace with the endpoint set to Read/Write. Storage mode is irrelevant, and large storage format is only required for very large models, not for XMLA editing.',
  },
  {
    id: 'q-600-007',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    objectiveId: 'dp600-maintain-security',
    difficulty: 'tricky',
    stem: 'A sensitivity label of "Highly Confidential" is applied to a lakehouse. Which statement is true?',
    choices: [
      'Semantic models and reports built downstream inherit the label automatically',
      'The label prevents users without the label from being added to the workspace',
      'The label is applied upstream to the source systems feeding the lakehouse',
      'The label replaces the need for row-level security',
    ],
    answer: [0],
    explanation:
      'Labels flow downstream through lineage and persist into exports. They are classification and protection metadata, not an access-control mechanism, and they never propagate upstream.',
  },

  // ==================================================== DP-600 / Prepare
  {
    id: 'q-600-008',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    objectiveId: 'dp600-prepare-get',
    difficulty: 'core',
    stem: 'A team needs Fabric queries to read data held in an Amazon S3 bucket. The data must never be duplicated and must always reflect the current contents of the bucket. What should you create?',
    choices: [
      'A OneLake shortcut to the S3 location',
      'A Copy job on a five-minute schedule',
      'A mirrored database',
      'A Dataflow Gen2 with an incremental refresh policy',
    ],
    answer: [0],
    explanation:
      'A shortcut is a pointer with no copy and no refresh, so reads always see current data. Copy jobs and dataflows both duplicate. Mirroring is for supported operational databases, not object storage.',
  },
  {
    id: 'q-600-009',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    objectiveId: 'dp600-prepare-get',
    difficulty: 'core',
    stem: 'Your team is fluent in T-SQL and must run INSERT, UPDATE and DELETE against curated tables, plus multi-table transactions. Which Fabric item should hold the curated layer?',
    choices: [
      'A warehouse',
      'A lakehouse, using the SQL analytics endpoint',
      'An eventhouse',
      'A semantic model in Direct Lake mode',
    ],
    answer: [0],
    explanation:
      'The lakehouse SQL analytics endpoint is read-only, so it cannot satisfy DML. Fabric Warehouse is the T-SQL engine with full DDL, DML and multi-table transactions.',
  },
  {
    id: 'q-600-010',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    objectiveId: 'dp600-prepare-get',
    difficulty: 'tricky',
    stem: 'Which source is NOT supported as a Fabric mirrored database?',
    choices: [
      'An on-premises file share of CSV files',
      'Azure SQL Database',
      'Azure Cosmos DB',
      'Snowflake',
    ],
    answer: [0],
    explanation:
      'Mirroring replicates operational databases with change feeds. Flat files on a share have no change feed to mirror — they would be ingested with a pipeline or reached with a shortcut through a gateway-accessible store.',
  },
  {
    id: 'q-600-011',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    objectiveId: 'dp600-prepare-transform',
    difficulty: 'core',
    stem: 'A Dataflow Gen2 runs successfully every morning, but the lakehouse table it is supposed to populate is always empty. What is the most likely cause?',
    choices: [
      'No data destination is configured on the dataflow query',
      'Staging is disabled on the dataflow',
      'The lakehouse SQL analytics endpoint has not refreshed',
      'The dataflow is missing an incremental refresh policy',
    ],
    answer: [0],
    explanation:
      'A Gen2 dataflow only persists output when a query has an explicit data destination. Without one it evaluates and discards. Staging affects how large transformations are executed, not whether results land.',
  },
  {
    id: 'q-600-012',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    objectiveId: 'dp600-prepare-transform',
    difficulty: 'tricky',
    stem: 'A dimension must keep full history of attribute changes so that historical facts continue to report against the attribute values that were current at the time. Which two design elements are required? (Choose two.)',
    choices: [
      'A surrogate key on the dimension, referenced by the fact table',
      'Effective start and end date columns with a current-record flag',
      'A NOT ENFORCED primary key on the business key column',
      'Overwriting the changed attribute during each load',
    ],
    answer: [0, 1],
    explanation:
      'That is a Type 2 SCD: a new row per change with validity dates and a current flag, and facts joined on the surrogate key rather than the business key. Overwriting is Type 1, which destroys history.',
  },
  {
    id: 'q-600-013',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    objectiveId: 'dp600-prepare-transform',
    difficulty: 'tricky',
    stem: 'A Fabric Warehouse table declares PRIMARY KEY (CustomerKey) NOT ENFORCED, but the load process has inserted duplicate CustomerKey values. What is the consequence?',
    choices: [
      'Queries can return incorrect results because the optimiser trusts the declared uniqueness',
      'The INSERT that created the duplicate would have failed',
      'Nothing — NOT ENFORCED constraints are ignored entirely',
      'The table becomes read-only until the duplicates are removed',
    ],
    answer: [0],
    explanation:
      'NOT ENFORCED means the engine never validates the constraint, but the optimiser still uses it when building plans. Declaring uniqueness that does not hold produces silently wrong results — the worst kind of failure.',
  },
  {
    id: 'q-600-014',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    objectiveId: 'dp600-prepare-transform',
    difficulty: 'core',
    stem: 'Which statement bulk-loads Parquet files from ADLS Gen2 into a Fabric Warehouse table with the highest throughput?',
    choices: ['COPY INTO', 'BULK INSERT', 'OPENROWSET', 'INSERT ... SELECT over a shortcut'],
    answer: [0],
    explanation:
      'COPY INTO is the supported high-throughput ingestion statement in Fabric Warehouse, handling PARQUET and CSV with wildcards and an ERRORFILE for rejected rows.',
  },
  {
    id: 'q-600-015',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    objectiveId: 'dp600-prepare-query',
    difficulty: 'core',
    stem: 'Within a single workspace, you need one query that joins a table in Warehouse A to a table in the SQL analytics endpoint of Lakehouse B. What do you do?',
    choices: [
      'Add both to the query editor and use three-part names — no data movement is needed',
      'Create a shortcut from Lakehouse B into Warehouse A first',
      'Copy the lakehouse table into the warehouse with a pipeline',
      'Build a composite semantic model over both',
    ],
    answer: [0],
    explanation:
      'Cross-database querying across warehouses and lakehouse SQL endpoints in the same workspace works natively with database.schema.table naming. The other options all add unnecessary movement or modelling.',
  },
  {
    id: 'q-600-016',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    objectiveId: 'dp600-prepare-transform',
    difficulty: 'tricky',
    stem: 'A bronze table is written by a streaming job every 30 seconds. Reports over it have become very slow, and the table holds hundreds of thousands of Parquet files. Which action addresses the root cause?',
    choices: [
      'Run OPTIMIZE on the table to compact small files, then schedule table maintenance',
      'Run VACUUM with a zero-hour retention',
      'Increase the size of the Spark pool',
      'Partition the table by event timestamp to the second',
    ],
    answer: [0],
    explanation:
      'This is the classic small-file problem, and bin-compaction with OPTIMIZE is the fix. VACUUM reclaims storage but does not compact. A bigger pool masks the symptom, and partitioning by second makes the file count dramatically worse.',
  },
  {
    id: 'q-600-017',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    objectiveId: 'dp600-prepare-transform',
    difficulty: 'core',
    stem: 'Which two statements about V-Order are correct? (Choose two.)',
    choices: [
      'It is applied when files are written and speeds up read performance',
      'The resulting files remain fully Parquet-compliant and readable by any engine',
      'It must be enabled for a Delta table to be readable by Direct Lake',
      'It reduces write time on ingestion-heavy tables',
    ],
    answer: [0, 1],
    explanation:
      'V-Order is a write-time optimisation producing standard Parquet that reads faster, especially for VertiPaq and Direct Lake. It costs write time rather than saving it, and Direct Lake reads non-V-Ordered Delta tables perfectly well — just less efficiently.',
  },
  {
    id: 'q-600-018',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    objectiveId: 'dp600-prepare-query',
    difficulty: 'core',
    stem: 'You must let a business analyst who does not write code build a filtered, grouped query over lakehouse tables and save it for reuse. Which tool fits best?',
    choices: [
      'The Visual query editor in the SQL analytics endpoint',
      'A Spark notebook using spark.sql',
      'The KQL queryset',
      'DAX Query View in Power BI Desktop',
    ],
    answer: [0],
    explanation:
      'The Visual query editor gives a drag-and-drop, Power Query-like surface over the SQL endpoint and the result can be saved as a view. Notebooks and KQL both require code, and DAX Query View targets semantic models.',
  },

  // ==================================================== DP-600 / Semantic models
  {
    id: 'q-600-019',
    exam: 'DP-600',
    domainId: 'dp600-models',
    objectiveId: 'dp600-models-design',
    difficulty: 'core',
    stem: 'A gold-layer Delta table in a lakehouse must serve a report with Import-like performance, with no scheduled refresh and no second copy of the data. Which storage mode should the semantic model use?',
    choices: ['Direct Lake', 'Import', 'DirectQuery', 'Dual'],
    answer: [0],
    explanation:
      'Direct Lake loads the Delta Parquet columns straight into VertiPaq on demand — Import-class speed with no refresh and no duplication. Import would duplicate and need refresh; DirectQuery would be slower.',
  },
  {
    id: 'q-600-020',
    exam: 'DP-600',
    domainId: 'dp600-models',
    objectiveId: 'dp600-models-design',
    difficulty: 'tricky',
    stem: 'You want a Direct Lake model to raise an error instead of quietly degrading when it cannot serve a query from OneLake. What do you configure?',
    choices: [
      'Set the DirectLakeBehavior property to DirectLakeOnly',
      'Set the DirectLakeBehavior property to Automatic',
      'Disable the SQL analytics endpoint on the lakehouse',
      'Enable large semantic model storage format',
    ],
    answer: [0],
    explanation:
      'DirectLakeOnly disables the DirectQuery fallback path, so anything unsupported fails loudly instead of running slowly. Automatic is the default that silently falls back.',
  },
  {
    id: 'q-600-021',
    exam: 'DP-600',
    domainId: 'dp600-models',
    objectiveId: 'dp600-models-design',
    difficulty: 'tricky',
    stem: 'In a composite model, a large fact table is DirectQuery and the shared date dimension is Import. Report performance is poor and relationship behaviour is inconsistent. What should you change?',
    choices: [
      'Set the date dimension to Dual storage mode',
      'Set the fact table to Import storage mode',
      'Set the date dimension to DirectQuery storage mode',
      'Remove the relationship and use TREATAS',
    ],
    answer: [0],
    explanation:
      'Dual lets the dimension act as Import when queried with Import tables and as DirectQuery when joined to the DirectQuery fact, avoiding a limited relationship. Converting the large fact to Import may be impossible, and making the dimension DirectQuery slows everything.',
  },
  {
    id: 'q-600-022',
    exam: 'DP-600',
    domainId: 'dp600-models',
    objectiveId: 'dp600-models-design',
    difficulty: 'core',
    stem: 'You have 12 measures and each needs year-to-date, prior year and year-over-year variants. What is the lowest-maintenance way to deliver 48 combinations?',
    choices: [
      'A calculation group with three calculation items using SELECTEDMEASURE()',
      'Thirty-six additional explicit measures written with CALCULATE',
      'A field parameter over the 12 base measures',
      'Three calculated columns on the date table',
    ],
    answer: [0],
    explanation:
      'A calculation group applies its items to any measure via SELECTEDMEASURE(), turning 36 near-identical measures into three definitions. Field parameters switch which measure is shown; they do not add time intelligence.',
  },
  {
    id: 'q-600-023',
    exam: 'DP-600',
    domainId: 'dp600-models',
    objectiveId: 'dp600-models-design',
    difficulty: 'core',
    stem: 'A role must hide the Salary column entirely — it should not appear in the field list for members of that role. What do you implement?',
    choices: [
      'Object-level security using an external tool such as Tabular Editor',
      'Row-level security with a DAX filter on the employee table',
      'Column-level security with a T-SQL DENY on the warehouse',
      'A dynamic data masking rule on the Salary column',
    ],
    answer: [0],
    explanation:
      'Hiding a column from the semantic model field list is object-level security, which is defined with external tools rather than the Power BI Desktop RLS dialog. RLS filters rows; T-SQL CLS and masking operate on the warehouse, not the model.',
  },
  {
    id: 'q-600-024',
    exam: 'DP-600',
    domainId: 'dp600-models',
    objectiveId: 'dp600-models-optimize',
    difficulty: 'tricky',
    stem: 'Incremental refresh is configured on a 900-million-row Import table, but each refresh still takes hours and the source is queried in full. What is the most likely cause?',
    choices: [
      'The RangeStart and RangeEnd filter does not fold to the source query',
      'The table is missing a relationship to the date dimension',
      'The large semantic model storage format is disabled',
      'Detect data changes is not configured',
    ],
    answer: [0],
    explanation:
      'Incremental refresh only limits work if the RangeStart/RangeEnd date filter folds into the native source query. If a preceding step breaks folding, every partition scans the whole table. Detect data changes is an optimisation on top, not the cause.',
  },
  {
    id: 'q-600-025',
    exam: 'DP-600',
    domainId: 'dp600-models',
    objectiveId: 'dp600-models-optimize',
    difficulty: 'core',
    stem: 'A report page is slow. You need to know which visual is responsible and see the DAX it generates. Which tool do you start with?',
    choices: [
      'Performance Analyzer in Power BI Desktop',
      'Best Practice Analyzer in Tabular Editor',
      'The Fabric Capacity Metrics app',
      'Query insights in the warehouse',
    ],
    answer: [0],
    explanation:
      'Performance Analyzer breaks a page down per visual into DAX query, visual display and other, and exposes the generated query. BPA reviews model design; the Metrics app reports capacity consumption; query insights covers warehouse T-SQL.',
  },
  {
    id: 'q-600-026',
    exam: 'DP-600',
    domainId: 'dp600-models',
    objectiveId: 'dp600-models-design',
    difficulty: 'tricky',
    stem: 'A measure is defined as SUMX(Sales, [Unit Price] * Sales[Quantity]) where [Unit Price] is a measure. Why can this be far slower than expected?',
    choices: [
      'Referencing a measure inside an iterator triggers context transition on every row',
      'SUMX cannot be used with measures and returns an error',
      'SUMX always materialises the entire table into memory',
      'Multiplication forces a DirectQuery fallback',
    ],
    answer: [0],
    explanation:
      'A measure reference is implicitly wrapped in CALCULATE, so each of the millions of row contexts becomes a filter context — the classic context-transition performance trap. Referencing the base column, or restructuring the calculation, avoids it.',
  },
  {
    id: 'q-600-027',
    exam: 'DP-600',
    domainId: 'dp600-models',
    objectiveId: 'dp600-models-design',
    difficulty: 'core',
    stem: 'A fact table has two date columns, OrderDate and ShipDate, both related to the date dimension, but only the OrderDate relationship is active. Which function lets a measure aggregate by ShipDate?',
    choices: [
      'USERELATIONSHIP inside CALCULATE',
      'TREATAS on the ShipDate column',
      'CROSSFILTER set to both',
      'RELATED on the date table',
    ],
    answer: [0],
    explanation:
      'USERELATIONSHIP activates an existing inactive relationship for the duration of a CALCULATE. TREATAS creates a virtual relationship where none exists; CROSSFILTER changes direction, not which relationship is active.',
  },
  {
    id: 'q-600-028',
    exam: 'DP-600',
    domainId: 'dp600-models',
    objectiveId: 'dp600-models-optimize',
    difficulty: 'tricky',
    stem: 'Which two conditions must hold for user-defined aggregations to actually be used? (Choose two.)',
    choices: [
      'The aggregation table is in Import storage mode',
      'The detail table is in DirectQuery or Dual storage mode',
      'The aggregation table is in DirectQuery storage mode',
      'The aggregation table has no relationships to dimensions',
    ],
    answer: [0, 1],
    explanation:
      'The point of aggregations is to answer coarse queries from memory and drill through to the source for finer grain, so the aggregation table must be Import and the detail table DirectQuery or Dual. The aggregation table needs relationships or GroupBy mappings to be matched at all.',
  },

  // ==================================================== DP-700 / Implement
  {
    id: 'q-700-001',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    objectiveId: 'dp700-implement-workspace',
    difficulty: 'core',
    stem: 'How many capacity units does an F64 capacity provide, and roughly how many CU-seconds is that per day?',
    choices: [
      '64 CUs, about 5,529,600 CU-seconds per day',
      '64 CUs, about 92,160 CU-seconds per day',
      '640 CUs, about 55,296,000 CU-seconds per day',
      '8 CUs, about 691,200 CU-seconds per day',
    ],
    answer: [0],
    explanation:
      'The F number is the CU count. 64 CUs x 86,400 seconds = 5,529,600 CU-seconds available per day. That figure is the denominator behind every utilisation percentage in the Capacity Metrics app.',
  },
  {
    id: 'q-700-002',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    objectiveId: 'dp700-implement-workspace',
    difficulty: 'tricky',
    stem: 'Over which periods does Fabric smooth consumption?',
    choices: [
      'Background operations over 24 hours; interactive operations over a minimum of 5 minutes',
      'Background operations over 5 minutes; interactive operations over 24 hours',
      'All operations over 30 seconds',
      'All operations over 10 minutes',
    ],
    answer: [0],
    explanation:
      'Background work such as Spark jobs, pipelines and refreshes is spread over 24 hours; interactive work such as report queries is spread over at least 5 minutes. This is why a large overnight job can still be throttling you the next morning.',
  },
  {
    id: 'q-700-003',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    objectiveId: 'dp700-implement-workspace',
    difficulty: 'tricky',
    stem: 'A capacity has accumulated 90 minutes of future smoothed consumption. Which throttling behaviour applies?',
    choices: [
      'Interactive requests are rejected; background jobs continue to run',
      'Interactive requests are delayed by about 20 seconds',
      'All requests, including background jobs, are rejected',
      'No throttling — overage protection still applies',
    ],
    answer: [0],
    explanation:
      'The stages are: under 10 minutes overage protection, 10 to 60 minutes interactive delay, 60 minutes to 24 hours interactive rejection, and beyond 24 hours background rejection. At 90 minutes you are in interactive rejection, so reports break but jobs keep running.',
  },
  {
    id: 'q-700-004',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    objectiveId: 'dp700-implement-workspace',
    difficulty: 'core',
    stem: 'Finance wants to downsize from F64 to F32 to cut cost. Several hundred report consumers hold free licences. What is the impact?',
    choices: [
      'Free-licence users lose access; every consumer now needs a Pro or PPU licence',
      'Nothing changes for consumers; only compute is reduced',
      'Free users keep read access but lose export capability',
      'The workspace becomes read-only',
    ],
    answer: [0],
    explanation:
      'Free-licence content consumption requires F64/P1 or above. Dropping below that threshold means every viewer needs a per-user licence — frequently a much larger cost than the capacity saving.',
  },
  {
    id: 'q-700-005',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    objectiveId: 'dp700-implement-workspace',
    difficulty: 'core',
    stem: 'Notebook sessions in a workspace take several minutes to start. Investigation shows every notebook is attached to a custom pool with a custom environment. What is the most effective change to shorten start-up?',
    choices: [
      'Use the starter pool where custom libraries are not required, and enable high concurrency mode',
      'Increase the node size of the custom pool',
      'Increase the maximum number of executors',
      'Enable autotune on the workspace',
    ],
    answer: [0],
    explanation:
      'Starter pools are pre-warmed, so sessions start in seconds; custom pools and custom environments force a cold start. High concurrency mode lets several notebooks share one already-running session. Bigger nodes do not make cold starts faster.',
  },
  {
    id: 'q-700-006',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    objectiveId: 'dp700-implement-security',
    difficulty: 'tricky',
    stem: 'Analysts must query a warehouse table but must never see full credit card numbers, while the finance service principal must see them. Which combination is appropriate?',
    choices: [
      'Dynamic data masking on the column, with UNMASK granted to the finance principal',
      'Row-level security with a filter predicate on the card column',
      'A OneLake data access role restricted to the finance folder',
      'Object-level security in the semantic model',
    ],
    answer: [0],
    explanation:
      'Masking changes what is returned per principal without duplicating the table, and UNMASK permission exempts privileged callers. RLS filters rows rather than obscuring a column value, and the other two operate at file or model level rather than on the warehouse column.',
  },
  {
    id: 'q-700-007',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    objectiveId: 'dp700-implement-security',
    difficulty: 'core',
    stem: 'Which two requirements apply to the predicate function used by a T-SQL row-level security policy? (Choose two.)',
    choices: [
      'It must be an inline table-valued function',
      'It must be created WITH SCHEMABINDING',
      'It must return a bit column named IsVisible',
      'It must be created in the dbo schema',
    ],
    answer: [0, 1],
    explanation:
      'CREATE SECURITY POLICY requires a schemabound inline table-valued function used as a FILTER PREDICATE. The function returns a row when access is allowed; the column name is arbitrary, and a dedicated security schema is good practice rather than a requirement.',
  },
  {
    id: 'q-700-008',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    objectiveId: 'dp700-implement-security',
    difficulty: 'tricky',
    stem: 'A pipeline must read from an ADLS Gen2 account whose firewall blocks public network access. Which approach lets Fabric reach it without opening the firewall to the internet?',
    choices: [
      'Enable a workspace identity and configure trusted workspace access with a resource instance rule',
      'Store the storage account key in the pipeline connection',
      'Create a OneLake shortcut with an account SAS token',
      'Move the storage account into the same region as the capacity',
    ],
    answer: [0],
    explanation:
      'Trusted workspace access uses the workspace identity plus a resource instance rule on the storage account, so traffic is authorised without a public network path. Keys and SAS tokens still traverse the blocked public endpoint, and region has nothing to do with firewall rules.',
  },
  {
    id: 'q-700-009',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    objectiveId: 'dp700-implement-orchestrate',
    difficulty: 'core',
    stem: 'A pipeline must start as soon as a file lands in a OneLake folder, with no polling delay. What do you configure?',
    choices: [
      'A OneLake event trigger, which is backed by Activator',
      'A schedule that runs every minute',
      'A tumbling window trigger on the Copy activity',
      'A Lookup activity in a Until loop that checks for the file',
    ],
    answer: [0],
    explanation:
      'Fabric event triggers subscribe to OneLake events through Activator and fire on file creation. Frequent schedules and polling loops burn CUs and still add latency.',
  },
  {
    id: 'q-700-010',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    objectiveId: 'dp700-implement-orchestrate',
    difficulty: 'tricky',
    stem: 'A ForEach activity iterates 200 table names and each iteration uses Set variable to build a log string. The resulting log is scrambled. Why?',
    choices: [
      'ForEach runs iterations in parallel by default, so Set variable is a race condition',
      'Set variable cannot be used inside ForEach',
      'Variables are scoped per iteration and reset each time',
      'The batch count must be set to at least 50 for variables to work',
    ],
    answer: [0],
    explanation:
      'Unless Sequential is checked, ForEach runs iterations concurrently, and pipeline variables are shared across the run. Either tick Sequential or append rows to a table instead of mutating a shared variable.',
  },
  {
    id: 'q-700-011',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    objectiveId: 'dp700-implement-lifecycle',
    difficulty: 'core',
    stem: 'A team wants warehouse schema managed declaratively as code, built into an artefact and published to each environment by a CI pipeline. What should they use?',
    choices: [
      'A SQL database project that builds a dacpac',
      'A deployment pipeline with deployment rules',
      'Git integration on the warehouse workspace only',
      'A notebook that runs CREATE TABLE IF NOT EXISTS statements',
    ],
    answer: [0],
    explanation:
      'Database projects capture warehouse objects as source, build a dacpac and publish a declarative diff — the state-based deployment model that fits a CI pipeline. Deployment pipelines are a portal-driven promotion mechanism, not a build artefact.',
  },

  // ==================================================== DP-700 / Ingest
  {
    id: 'q-700-012',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    objectiveId: 'dp700-ingest-patterns',
    difficulty: 'tricky',
    stem: 'An incremental load stores the maximum ModifiedDate it has processed and, on the next run, selects rows where ModifiedDate > watermark. Rows are occasionally missing from the target. What is the most likely cause?',
    choices: [
      'Rows sharing the same ModifiedDate as the stored watermark are skipped, and rows written during the read window are missed',
      'The watermark table is not indexed',
      'The Copy activity is not using a staging area',
      'ModifiedDate is stored as datetime2 rather than datetime',
    ],
    answer: [0],
    explanation:
      'A strict greater-than on a non-unique timestamp loses every row that shares the boundary value, and rows committed after the read started but stamped before it are missed too. Use >= with deduplication, or a proper change feed.',
  },
  {
    id: 'q-700-013',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    objectiveId: 'dp700-ingest-patterns',
    difficulty: 'core',
    stem: 'A pipeline updates its watermark control table immediately after reading the source, before the write to the lakehouse completes. Why is this a problem?',
    choices: [
      'If the write fails, the skipped window is never reprocessed and data is silently lost',
      'The control table becomes locked for the duration of the write',
      'The watermark cannot be read while the write is in progress',
      'It doubles the CU consumption of the pipeline',
    ],
    answer: [0],
    explanation:
      'The watermark must only advance once the data it represents is durably written. Otherwise a failed run leaves the watermark ahead of the data and the next run starts after the gap.',
  },
  {
    id: 'q-700-014',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    objectiveId: 'dp700-ingest-batch',
    difficulty: 'core',
    stem: 'A pipeline is configured with three retries. The load step is a plain INSERT of the batch into a Delta table. A transient failure causes a retry after the insert has already committed. What happens?',
    choices: [
      'The batch is inserted twice, duplicating data',
      'Delta detects the duplicate transaction and skips it',
      'The retry fails with a schema mismatch',
      'The table is rolled back to the previous version automatically',
    ],
    answer: [0],
    explanation:
      'Retries are only safe against idempotent writes. A blind INSERT is not idempotent — use MERGE on a business key, or delete-then-insert the affected partition, so re-running converges to the same state.',
  },
  {
    id: 'q-700-015',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    objectiveId: 'dp700-ingest-batch',
    difficulty: 'tricky',
    stem: 'A MERGE INTO fails with an error stating that a target row matched multiple source rows. What is the correct fix?',
    choices: [
      'Deduplicate the source to one row per merge key before the MERGE',
      'Add a NOT ENFORCED primary key to the target table',
      'Switch the merge condition to an inequality',
      'Run OPTIMIZE on the target table first',
    ],
    answer: [0],
    explanation:
      'MERGE is deterministic only when each target row matches at most one source row. Collapse the source with ROW_NUMBER or a window function picking the latest version per key.',
  },
  {
    id: 'q-700-016',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    objectiveId: 'dp700-ingest-batch',
    difficulty: 'core',
    stem: 'You must copy a large table incrementally from Azure SQL Database into a lakehouse, without hand-building watermark logic. Which Fabric item is the most direct fit?',
    choices: [
      'A Copy job',
      'A Copy activity inside a pipeline',
      'A Dataflow Gen2',
      'A Spark notebook using the JDBC connector',
    ],
    answer: [0],
    explanation:
      'Copy job is the standalone item designed for full and incremental copy with built-in change tracking and a guided setup. The other options all work but require you to implement incremental logic yourself.',
  },
  {
    id: 'q-700-017',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    objectiveId: 'dp700-ingest-stream',
    difficulty: 'core',
    stem: 'Telemetry must be aggregated into non-overlapping five-minute buckets so that each event is counted exactly once. Which windowing function do you use?',
    choices: ['Tumbling', 'Hopping', 'Sliding', 'Session'],
    answer: [0],
    explanation:
      'Tumbling windows are fixed-size and non-overlapping, so every event falls into exactly one window. Hopping and sliding windows overlap; session windows are defined by gaps in activity.',
  },
  {
    id: 'q-700-018',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    objectiveId: 'dp700-ingest-stream',
    difficulty: 'tricky',
    stem: 'A Spark structured streaming aggregation runs for days, gets progressively slower and eventually fails with memory pressure. Which change addresses the cause?',
    choices: [
      'Add withWatermark on the event-time column so old state can be evicted',
      'Increase the trigger interval',
      'Switch the output mode from append to complete',
      'Disable checkpointing to reduce overhead',
    ],
    answer: [0],
    explanation:
      'Without a watermark the engine must keep aggregation state forever in case a late event arrives. A watermark bounds how late an event may be, letting state be dropped. Complete output mode makes the problem worse, and removing checkpoints breaks recovery.',
  },
  {
    id: 'q-700-019',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    objectiveId: 'dp700-ingest-stream',
    difficulty: 'tricky',
    stem: 'An eventstream writes to a lakehouse destination. After a week, queries over the destination table are very slow. What is the most likely cause and remedy?',
    choices: [
      'Micro-batch writes produced many small Parquet files — schedule table maintenance with OPTIMIZE',
      'The eventstream is dropping events — increase the retention period',
      'The lakehouse SQL endpoint needs a manual metadata refresh',
      'The destination should be partitioned by event id',
    ],
    answer: [0],
    explanation:
      'Streaming destinations commit frequent small batches, so file counts grow quickly. Regular OPTIMIZE (table maintenance) compacts them. Partitioning by a high-cardinality id would make it far worse.',
  },
  {
    id: 'q-700-020',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    objectiveId: 'dp700-ingest-stream',
    difficulty: 'core',
    stem: 'Raw events land in a KQL table, but analysts need a curated table with parsed and typed columns, populated automatically as data arrives. What do you configure?',
    choices: [
      'An update policy on the curated table sourced from the raw table',
      'A materialized view over the raw table',
      'A scheduled notebook that reads and rewrites the table',
      'OneLake availability on the eventhouse',
    ],
    answer: [0],
    explanation:
      'An update policy runs a KQL transformation on every ingested batch and appends the result to the target table. A materialized view maintains an aggregation rather than a parsed copy, and it cannot reshape ingestion.',
  },
  {
    id: 'q-700-021',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    objectiveId: 'dp700-ingest-batch',
    difficulty: 'tricky',
    stem: 'Sales facts arrive referencing a store that does not yet exist in the store dimension. The business requires that no fact rows are dropped. What should the load do?',
    choices: [
      'Insert an inferred member row for the unknown store and update it when the real record arrives',
      'Route the fact rows to a reject table for manual reprocessing',
      'Set the store surrogate key to NULL on the fact row',
      'Delay the fact load until the dimension load completes',
    ],
    answer: [0],
    explanation:
      'The late-arriving dimension pattern inserts an inferred member keyed by the business key so the fact keeps a valid surrogate key, then back-fills attributes later. Rejecting or nulling loses data or breaks the star schema.',
  },
  {
    id: 'q-700-022',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    objectiveId: 'dp700-ingest-batch',
    difficulty: 'core',
    stem: 'Which two are true about a OneLake shortcut compared with a Copy activity? (Choose two.)',
    choices: [
      'A shortcut stores no second copy of the data',
      'A shortcut always reflects the current contents of the source',
      'A shortcut guarantees better query performance than a local copy',
      'A shortcut can transform the data as it is read',
    ],
    answer: [0, 1],
    explanation:
      'Shortcuts are virtualisation: no copy, always current. Performance depends on the remote store and can be worse than a local Delta copy, and shortcuts perform no transformation.',
  },

  // ==================================================== DP-700 / Monitor
  {
    id: 'q-700-023',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    objectiveId: 'dp700-monitor-items',
    difficulty: 'core',
    stem: 'Report users are reporting rejected queries. You need to identify which item consumed the capacity that caused the throttling. Where do you look?',
    choices: [
      'The Microsoft Fabric Capacity Metrics app',
      'The Monitoring hub',
      'The workspace activity log',
      'The Spark UI for the workspace',
    ],
    answer: [0],
    explanation:
      'The Capacity Metrics app is the supported tool for CU consumption, overage and throttling, and it ranks items by CU-seconds. The Monitoring hub shows run status, not capacity consumption.',
  },
  {
    id: 'q-700-024',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    objectiveId: 'dp700-monitor-errors',
    difficulty: 'core',
    stem: 'You need the history of completed T-SQL queries in a Fabric Warehouse, including duration and the submitting user. Which object do you query?',
    choices: [
      'queryinsights.exec_requests_history',
      'sys.dm_exec_requests',
      'queryinsights.frequently_run_queries only',
      'The Monitoring hub run list',
    ],
    answer: [0],
    explanation:
      'exec_requests_history is the query insights view holding completed request history. sys.dm_exec_requests is a DMV showing currently executing requests, which is the right answer for "what is running right now".',
  },
  {
    id: 'q-700-025',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    objectiveId: 'dp700-monitor-optimize',
    difficulty: 'tricky',
    stem: 'In a Spark stage, 199 tasks finish in seconds and one runs for 40 minutes. What is the problem?',
    choices: [
      'Data skew — one partition holds a disproportionate share of the rows',
      'The pool is undersized and needs more executors',
      'The table needs VACUUM',
      'Dynamic allocation is disabled',
    ],
    answer: [0],
    explanation:
      'A single long-running task in an otherwise fast stage is the signature of skew, typically caused by a hot join key or nulls. Add salting, repartition, or use a broadcast join. Adding executors does not help a single hot partition.',
  },
  {
    id: 'q-700-026',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    objectiveId: 'dp700-monitor-optimize',
    difficulty: 'tricky',
    stem: 'A 200 GB Delta table is partitioned by CustomerId, which has two million distinct values. Queries filter by OrderDate. What should you change?',
    choices: [
      'Remove the CustomerId partitioning and partition by a coarse date column, or leave it unpartitioned with OPTIMIZE and V-Order',
      'Add a second partition column for OrderDate',
      'Increase the Spark pool node size',
      'Enable result set caching on the lakehouse',
    ],
    answer: [0],
    explanation:
      'Partitioning on a high-cardinality column that queries do not filter on gives no pruning and produces millions of tiny files. Partition on the column queries actually filter, at a coarse grain, or not at all.',
  },
  {
    id: 'q-700-027',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    objectiveId: 'dp700-monitor-items',
    difficulty: 'core',
    stem: 'An operations team needs an email whenever a specific sensor stays above a threshold for ten consecutive minutes. Which Fabric item should you use?',
    choices: [
      'Activator',
      'A pipeline on a ten-minute schedule',
      'A KQL materialized view',
      'The Monitoring hub',
    ],
    answer: [0],
    explanation:
      'Activator evaluates conditions per object over time and triggers actions such as email, Teams or a Fabric item run — exactly the stateful "stays above for N minutes" pattern.',
  },
  {
    id: 'q-700-028',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    objectiveId: 'dp700-monitor-optimize',
    difficulty: 'tricky',
    stem: 'An eventhouse table has a retention policy of 7 days and a caching (hot cache) policy of 30 days. What is the effect?',
    choices: [
      'Only 7 days of data exists, so the extra caching period has no effect',
      'Data is retained for 30 days because caching extends retention',
      'Queries older than 7 days read from cold storage',
      'Ingestion fails because the policies conflict',
    ],
    answer: [0],
    explanation:
      'Retention decides how long data exists at all; caching decides how much of that data is held on SSD. A caching period longer than retention simply caches everything that still exists.',
  },
  {
    id: 'q-700-029',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    objectiveId: 'dp700-monitor-items',
    difficulty: 'core',
    stem: 'You want diagnostic logs and metrics from workspace items streamed somewhere you can query them with KQL. What do you enable?',
    choices: [
      'Workspace monitoring, which provisions a monitoring eventhouse',
      'The Capacity Metrics app',
      'A OneLake shortcut to the activity log',
      'Git integration for the workspace',
    ],
    answer: [0],
    explanation:
      'Workspace monitoring is the opt-in setting that creates a read-only monitoring eventhouse and streams item telemetry into it for KQL analysis.',
  },
  {
    id: 'q-700-030',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    objectiveId: 'dp700-monitor-optimize',
    difficulty: 'core',
    stem: 'After a very large bulk load, warehouse queries against the new data pick poor join orders. What is the most appropriate action?',
    choices: [
      'Update statistics on the columns used in joins and filters',
      'Run OPTIMIZE on the warehouse tables',
      'Rebuild the clustered columnstore index',
      'Increase the capacity SKU',
    ],
    answer: [0],
    explanation:
      'Plan quality depends on column statistics. Fabric maintains them automatically, but after a large load a manual UPDATE STATISTICS ensures the optimiser sees current cardinality. OPTIMIZE is a Delta lakehouse operation.',
  },
  {
    id: 'q-700-031',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    objectiveId: 'dp700-monitor-errors',
    difficulty: 'tricky',
    stem: 'A nightly pipeline fails intermittently on a Copy activity with a transient network error. Which combination is the most appropriate response?',
    choices: [
      'Configure retries on the activity and make the load idempotent with MERGE',
      'Configure retries on the activity and leave the plain INSERT in place',
      'Remove the retry so failures are always visible',
      'Increase the activity timeout to 24 hours',
    ],
    answer: [0],
    explanation:
      'Retries handle transient faults, but only safely when re-running produces the same result. Pair the retry with an idempotent write. A longer timeout does not address a network fault.',
  },
  {
    id: 'q-700-032',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    objectiveId: 'dp700-monitor-items',
    difficulty: 'core',
    stem: 'Which two are best answered from the Monitoring hub rather than the Capacity Metrics app? (Choose two.)',
    choices: [
      'Whether last night pipeline run succeeded',
      'How long a specific Spark application ran',
      'Which item consumed the most CU-seconds yesterday',
      'Whether the capacity entered interactive rejection',
    ],
    answer: [0, 1],
    explanation:
      'The Monitoring hub is the run-history surface: status, duration and drill-through per run. CU consumption and throttling events belong to the Capacity Metrics app.',
  },
];

export function questionsFor(exam: ExamId, domainIds?: string[]): Question[] {
  return QUESTIONS.filter(
    (q) =>
      q.exam === exam &&
      (!domainIds || domainIds.length === 0 || domainIds.includes(q.domainId))
  );
}
