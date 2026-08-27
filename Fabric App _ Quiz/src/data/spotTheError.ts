import type { ExamId } from './exams';

export interface SpotScenario {
  id: string;
  exam: ExamId;
  domainId: string;
  title: string;
  /** One or two sentences of context shown above the snippet. */
  brief: string;
  /** 'code' renders monospace with line numbers; 'claims' renders prose statements. */
  kind: 'code' | 'claims';
  language: string;
  lines: string[];
  /** 0-based index of the line that is wrong. Exactly one line is wrong. */
  faultyLine: number;
  /** Short label for the defect. */
  fault: string;
  explanation: string;
  /** What the line should say instead. */
  fix: string;
  /** Notes explaining why other tempting lines are actually fine. */
  decoys?: Record<number, string>;
}

export const SCENARIOS: SpotScenario[] = [
  {
    id: 'se-001',
    exam: 'DP-600',
    domainId: 'dp600-models',
    title: 'Margin measure that breaks on empty slices',
    brief:
      'This measure works on the summary page but shows errors on a matrix broken down by product.',
    kind: 'code',
    language: 'dax',
    lines: [
      'Profit Margin % =',
      'VAR TotalSales = SUM ( Sales[SalesAmount] )',
      'VAR TotalCost  = SUM ( Sales[Cost] )',
      'VAR Margin     = TotalSales - TotalCost',
      'RETURN',
      '    Margin / TotalSales',
    ],
    faultyLine: 5,
    fault: 'Unguarded division',
    explanation:
      'For any row of the matrix where no sales exist in the current filter context, TotalSales is BLANK and the division raises an error (or returns Infinity). DIVIDE handles the zero and blank denominator internally and returns the alternate result, and it is also optimised in the engine.',
    fix: '    DIVIDE ( Margin, TotalSales )',
    decoys: {
      1: 'SUM over a column in the current filter context is correct here.',
      3: 'Computing the margin in a VAR is fine and keeps the expression readable.',
    },
  },
  {
    id: 'se-002',
    exam: 'DP-600',
    domainId: 'dp600-models',
    title: 'Year-to-date that resets in odd places',
    brief:
      'The date table is marked as a date table and is related to Sales on OrderDate. This YTD measure still misbehaves at month boundaries and when filtered from the date table.',
    kind: 'code',
    language: 'dax',
    lines: [
      'Sales YTD =',
      'CALCULATE (',
      '    SUM ( Sales[SalesAmount] ),',
      "    DATESYTD ( Sales[OrderDate] )",
      ')',
    ],
    faultyLine: 3,
    fault: 'Time intelligence over the fact table date column',
    explanation:
      'Time intelligence functions must operate on the contiguous date column of a marked date table, not on a date column in the fact table. Using the fact column ignores dates with no sales, leaves gaps in the generated date range, and does not respond correctly to filters coming from the date dimension.',
    fix: "    DATESYTD ( 'Date'[Date] )",
    decoys: {
      2: 'Aggregating the fact column with SUM is exactly right.',
      1: 'CALCULATE is the correct function for modifying filter context.',
    },
  },
  {
    id: 'se-003',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    title: 'Row-level security policy that will not create',
    brief:
      'Running this script in a Fabric Warehouse fails on the CREATE SECURITY POLICY statement.',
    kind: 'code',
    language: 'sql',
    lines: [
      'CREATE FUNCTION Security.fn_SalesRegion (@Region AS nvarchar(50))',
      '    RETURNS TABLE',
      'AS',
      '    RETURN SELECT 1 AS Visible',
      "    WHERE @Region = USER_NAME() OR USER_NAME() = 'RegionalManager';",
      'GO',
      'CREATE SECURITY POLICY Security.SalesFilter',
      '    ADD FILTER PREDICATE Security.fn_SalesRegion(Region) ON dbo.Sales',
      '    WITH (STATE = ON);',
    ],
    faultyLine: 1,
    fault: 'Predicate function is not schemabound',
    explanation:
      'A security predicate must be an inline table-valued function created WITH SCHEMABINDING, so the objects it depends on cannot be altered out from under the policy. Without it, CREATE SECURITY POLICY refuses the function.',
    fix: '    RETURNS TABLE\n    WITH SCHEMABINDING',
    decoys: {
      3: 'Returning a single row when access is allowed is the correct predicate shape; the column name is arbitrary.',
      7: 'FILTER PREDICATE with the column passed positionally is correct syntax.',
    },
  },
  {
    id: 'se-004',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    title: 'Bulk load that rejects every row',
    brief:
      'The files in the container are Parquet exports from an upstream system. Every run fails with conversion errors.',
    kind: 'code',
    language: 'sql',
    lines: [
      'COPY INTO dbo.Sales',
      "FROM 'https://acct.dfs.core.windows.net/data/sales/*.parquet'",
      'WITH (',
      "    FILE_TYPE = 'CSV',",
      "    CREDENTIAL = (IDENTITY = 'Managed Identity'),",
      "    ERRORFILE = 'https://acct.dfs.core.windows.net/rejects/'",
      ');',
    ],
    faultyLine: 3,
    fault: 'FILE_TYPE does not match the source files',
    explanation:
      'The source is Parquet but COPY INTO is told to parse it as delimited text, so every row fails conversion. FILE_TYPE must match the physical format; the wildcard in the path is not what determines parsing.',
    fix: "    FILE_TYPE = 'PARQUET',",
    decoys: {
      4: 'Managed Identity is the recommended credential — far better than embedding a key.',
      5: 'An ERRORFILE location for rejected rows is good practice.',
    },
  },
  {
    id: 'se-005',
    exam: 'DP-600',
    domainId: 'dp600-prepare',
    title: 'The update that will never run',
    brief:
      'A developer opens the SQL analytics endpoint of a lakehouse and runs this to close out an SCD row.',
    kind: 'code',
    language: 'sql',
    lines: [
      '-- Connected to: MyLakehouse (SQL analytics endpoint)',
      'UPDATE dbo.DimCustomer',
      'SET   IsCurrent = 0,',
      '      EndDate   = CAST(GETDATE() AS date)',
      'WHERE CustomerKey = 42;',
    ],
    faultyLine: 1,
    fault: 'DML against a read-only endpoint',
    explanation:
      'The SQL analytics endpoint over a lakehouse is read-only. It supports SELECT, views, functions and security objects, but never INSERT, UPDATE or DELETE. Write the change with Spark (Delta MERGE or UPDATE) against the lakehouse table, or hold the dimension in a Fabric Warehouse where T-SQL DML is supported.',
    fix: 'Run the update as a Delta MERGE in a notebook, or move DimCustomer into a warehouse.',
    decoys: {
      2: 'Closing the row with a flag and an end date is correct Type 2 SCD behaviour.',
      4: 'Filtering on the surrogate key is right — it identifies one version of the member.',
    },
  },
  {
    id: 'se-006',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    title: 'Masking that masks nothing',
    brief:
      'The requirement is that analysts must not see customer email addresses. This script was deployed and the analysts still see them.',
    kind: 'code',
    language: 'sql',
    lines: [
      'ALTER TABLE dbo.Customer',
      'ALTER COLUMN Email nvarchar(200)',
      "ADD MASKED WITH (FUNCTION = 'email()');",
      'GO',
      'GRANT SELECT ON dbo.Customer TO AnalystRole;',
      'GRANT UNMASK TO AnalystRole;',
    ],
    faultyLine: 5,
    fault: 'UNMASK granted to the role being masked',
    explanation:
      'UNMASK exempts a principal from every masking rule in the database. Granting it to the analysts undoes the mask completely. Grant UNMASK only to the privileged principals that legitimately need raw values.',
    fix: 'GRANT UNMASK TO FinanceServicePrincipal;  -- not AnalystRole',
    decoys: {
      2: "The email() masking function is the right built-in for an address column.",
      4: 'Analysts do need SELECT — masking is what limits what they see, not the absence of SELECT.',
    },
  },
  {
    id: 'se-007',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    title: 'Streaming job that slowly dies',
    brief:
      'This structured streaming aggregation runs fine for a day, then gets steadily slower and eventually fails with out-of-memory errors on the executors.',
    kind: 'code',
    language: 'python',
    lines: [
      'df = spark.readStream.format("delta").load(bronze_path)',
      '',
      'agg = (df',
      '    .groupBy(window(col("event_time"), "5 minutes"), col("device_id"))',
      '    .agg(count("*").alias("events")))',
      '',
      '(agg.writeStream',
      '    .format("delta")',
      '    .outputMode("append")',
      '    .option("checkpointLocation", f"{silver_path}/_checkpoints/device_5min")',
      '    .start(silver_path))',
    ],
    faultyLine: 2,
    fault: 'Windowed aggregation with no watermark',
    explanation:
      'Without withWatermark on the event-time column, the engine must keep every window open forever in case a late event arrives, so the state store grows without bound. Adding a watermark declares how late an event may be and lets old state be evicted.',
    fix: 'agg = (df.withWatermark("event_time", "15 minutes")',
    decoys: {
      9: 'A query-specific checkpoint path is correct — sharing one path between queries is what causes corruption.',
      8: 'Append output mode is the right choice for a watermarked windowed aggregation.',
    },
  },
  {
    id: 'se-008',
    exam: 'DP-700',
    domainId: 'dp700-monitor',
    title: 'The write that created two million folders',
    brief:
      'A 200 GB gold table is written nightly. Reports filter almost exclusively on OrderDate. Query times have degraded badly since this write was introduced.',
    kind: 'code',
    language: 'python',
    lines: [
      '(df.write',
      '    .format("delta")',
      '    .mode("overwrite")',
      '    .partitionBy("customer_id")',
      '    .option("overwriteSchema", "true")',
      '    .save(gold_path))',
    ],
    faultyLine: 3,
    fault: 'Partitioned on a high-cardinality column nobody filters on',
    explanation:
      'customer_id has millions of distinct values, so this produces millions of tiny partitions and files. Queries filter on OrderDate, so they get no pruning benefit at all, only metadata overhead. Partition on the coarse column that queries actually filter, or leave the table unpartitioned and rely on OPTIMIZE and V-Order.',
    fix: '    .partitionBy("order_date_month")   # or drop partitionBy entirely',
    decoys: {
      2: 'Full overwrite is a legitimate load mode for a rebuilt gold table.',
      4: 'overwriteSchema is only needed when the schema changes, but it is not the performance problem.',
    },
  },
  {
    id: 'se-009',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    title: 'MERGE that fails intermittently',
    brief:
      'This upsert runs every 15 minutes. Most runs succeed, but some fail with "Cannot perform Merge as multiple source rows matched the same target row".',
    kind: 'code',
    language: 'python',
    lines: [
      'updates = spark.read.format("delta").load(staging_path)',
      '',
      '(DeltaTable.forPath(spark, target_path).alias("t")',
      '    .merge(updates.alias("s"), "t.customer_id = s.customer_id")',
      '    .whenMatchedUpdateAll()',
      '    .whenNotMatchedInsertAll()',
      '    .execute())',
    ],
    faultyLine: 0,
    fault: 'Source is not deduplicated to one row per merge key',
    explanation:
      'When a batch contains two changes for the same customer, MERGE cannot decide which one wins, so it aborts. Collapse the source to the latest row per key first — for example with row_number() over a partition by customer_id ordered by the change timestamp.',
    fix: 'updates = (spark.read.format("delta").load(staging_path)\n    .withColumn("rn", row_number().over(Window.partitionBy("customer_id").orderBy(col("modified_at").desc())))\n    .filter("rn = 1").drop("rn"))',
    decoys: {
      3: 'Matching on the business key is the intended merge condition.',
      4: 'whenMatchedUpdateAll is fine once the source has one row per key.',
    },
  },
  {
    id: 'se-010',
    exam: 'DP-600',
    domainId: 'dp600-models',
    title: 'Incremental refresh that refreshes everything',
    brief:
      'Incremental refresh is configured on this table with a 5-year archive and 10-day incremental window. Every refresh still takes hours and the DBA sees full table scans.',
    kind: 'code',
    language: 'powerquery',
    lines: [
      'let',
      '    Source   = Sql.Database("srv01", "SalesDW"),',
      '    Sales    = Source{[Schema="dbo", Item="FactSales"]}[Data],',
      '    Buffered = Table.Buffer(Sales),',
      '    Filtered = Table.SelectRows(Buffered, each [OrderDate] >= RangeStart and [OrderDate] < RangeEnd)',
      'in',
      '    Filtered',
    ],
    faultyLine: 3,
    fault: 'Query folding is broken before the range filter',
    explanation:
      'Table.Buffer materialises the whole table in memory, which stops query folding. The RangeStart/RangeEnd filter is then applied by the mashup engine after every row has already been pulled from SQL Server, so each partition scans the entire fact table. Remove the buffer so the filter folds into the native query.',
    fix: '    // remove Table.Buffer — apply the filter directly to Sales',
    decoys: {
      4: 'The half-open range (>= RangeStart, < RangeEnd) is exactly the required pattern — it avoids double-counting boundary rows.',
      1: 'A native SQL Server connection folds well; the source is not the problem.',
    },
  },
  {
    id: 'se-011',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    title: 'Update policy attached to the wrong table',
    brief:
      'Raw telemetry lands in RawEvents. ParsedEvents should be populated automatically with typed columns. ParsedEvents stays empty and RawEvents starts to look strange.',
    kind: 'code',
    language: 'kql',
    lines: [
      '.alter table RawEvents policy update',
      '@\'[{',
      '    "IsEnabled": true,',
      '    "Source": "ParsedEvents",',
      '    "Query": "RawEvents | extend deviceId = tostring(payload.device)",',
      '    "IsTransactional": true',
      '}]\'',
    ],
    faultyLine: 0,
    fault: 'Policy defined on the source table instead of the target',
    explanation:
      'An update policy belongs to the table that receives the transformed rows. It names the table it watches in Source. Here the policy sits on RawEvents and watches ParsedEvents, which is exactly backwards, so nothing ever lands in ParsedEvents.',
    fix: '.alter table ParsedEvents policy update   // with "Source": "RawEvents"',
    decoys: {
      5: 'IsTransactional: true is a reasonable choice — it makes ingestion fail if the transform fails, rather than silently dropping rows.',
      4: 'The transformation query itself is valid KQL.',
    },
  },
  {
    id: 'se-012',
    exam: 'DP-700',
    domainId: 'dp700-ingest',
    title: 'Incremental pipeline that loses a night of data',
    brief:
      'This pipeline runs nightly. On the rare night the notebook step fails, the missing rows never appear, even after the next successful run.',
    kind: 'code',
    language: 'text',
    lines: [
      "1. Lookup    — read LastLoaded from control.Watermark",
      "2. Script    — UPDATE control.Watermark SET LastLoaded = '@{utcNow()}'",
      "3. Copy      — WHERE ModifiedDate >= '@{activity('Lookup').output.firstRow.LastLoaded}'",
      '4. Notebook  — MERGE staging.Sales into dbo.Sales',
    ],
    faultyLine: 1,
    fault: 'Watermark advanced before the load is durable',
    explanation:
      'The watermark is moved forward at step 2, before the copy and the merge have committed. If step 3 or 4 fails, the watermark already points past the unloaded window, so the next run starts after the gap and those rows are lost forever. Advance the watermark only after the final write succeeds.',
    fix: 'Move the watermark UPDATE to the last step, on the success path of the notebook activity.',
    decoys: {
      2: 'Using >= rather than > is the safe choice — it re-reads boundary rows, which the MERGE then deduplicates.',
      3: 'MERGE makes the load idempotent, which is exactly what allows the re-read at step 3 to be safe.',
    },
  },
  {
    id: 'se-013',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    title: 'Load log that comes out scrambled',
    brief:
      'This ForEach loads 200 tables. All of them load correctly, but the audit string it builds is missing entries and lists some tables twice.',
    kind: 'code',
    language: 'text',
    lines: [
      'ForEach  "Load each table"',
      "    Items:       @activity('Get tables').output.value",
      '    Sequential:  false',
      '    Batch count: 20',
      '    Activities:',
      '      - Copy activity   source: @item().name',
      "      - Set variable    LoadLog = @concat(variables('LoadLog'), item().name, ';')",
    ],
    faultyLine: 6,
    fault: 'Shared pipeline variable mutated from parallel iterations',
    explanation:
      'Pipeline variables are scoped to the run, not the iteration. Twenty iterations reading and writing LoadLog concurrently lose each other updates — a classic read-modify-write race. Write each result as a row to a log table instead (or, if the log really must be a variable, set Sequential to true and accept the slower run).',
    fix: '      - Script activity  INSERT INTO audit.LoadLog (TableName, LoadedAt) VALUES (@item().name, @utcnow())',
    decoys: {
      2: 'Parallel execution is the point of this loop and is not itself a defect.',
      5: 'Passing @item().name to the Copy source is the correct way to parameterise the iteration.',
    },
  },
  {
    id: 'se-014',
    exam: 'DP-600',
    domainId: 'dp600-models',
    title: 'Direct Lake briefing — one claim is wrong',
    brief:
      'A colleague summarises Direct Lake for the team. Four of these statements are correct. One is not.',
    kind: 'claims',
    language: 'text',
    lines: [
      'Direct Lake loads Delta Parquet columns from OneLake straight into the VertiPaq engine on demand.',
      'A Direct Lake semantic model needs a scheduled refresh to pick up new data in the Delta table.',
      'If a query cannot be served from OneLake, the model can fall back to DirectQuery over the SQL analytics endpoint.',
      'SQL row-level security defined on the endpoint causes the model to fall back to DirectQuery.',
      'Setting DirectLakeBehavior to DirectLakeOnly makes unsupported queries fail instead of falling back.',
    ],
    faultyLine: 1,
    fault: 'Direct Lake does not use scheduled refresh',
    explanation:
      'There is no data copy to refresh. Direct Lake reads the current Delta files, so new data is visible without a refresh operation — that is the whole point of the storage mode. What does happen is column paging: the first query after a change reloads affected columns into memory and is slower.',
    fix: 'Direct Lake requires no scheduled refresh; new Delta data is picked up automatically.',
  },
  {
    id: 'se-015',
    exam: 'DP-700',
    domainId: 'dp700-implement',
    title: 'Capacity briefing — one claim is wrong',
    brief:
      'A capacity admin writes up how throttling works before a go-live. Spot the incorrect statement.',
    kind: 'claims',
    language: 'text',
    lines: [
      'An F64 capacity provides 64 capacity units.',
      'Background operations are smoothed over 24 hours.',
      'Interactive operations are smoothed over a minimum of five minutes.',
      'As soon as the capacity enters interactive delay, background jobs are rejected too.',
      'Pausing a capacity immediately settles any outstanding smoothed consumption.',
    ],
    faultyLine: 3,
    fault: 'Background jobs survive the interactive throttling stages',
    explanation:
      'The stages escalate: under 10 minutes of future smoothed consumption nothing is throttled; 10 to 60 minutes delays interactive requests by around 20 seconds; 60 minutes to 24 hours rejects interactive requests; only beyond 24 hours are background jobs rejected. So reports break well before pipelines do.',
    fix: 'Background jobs are only rejected in the final stage, beyond 24 hours of carry-forward.',
  },
  {
    id: 'se-016',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    title: 'Access briefing — one claim is wrong',
    brief:
      'A workspace admin documents who can do what. One statement will cause a support ticket.',
    kind: 'claims',
    language: 'text',
    lines: [
      'Contributor can create, edit and delete items in the workspace.',
      'Member can do everything Contributor can, and can also add other members.',
      'Viewer can open and read items but cannot create them.',
      'A user with the Viewer role automatically gets data access through the lakehouse SQL analytics endpoint.',
      'Only the Admin role can delete the workspace.',
    ],
    faultyLine: 3,
    fault: 'Viewer does not imply data access',
    explanation:
      'Viewer grants visibility of the item, not access to the data behind it. Querying a lakehouse through its SQL analytics endpoint requires the ReadData permission on the item; reading the underlying files with Spark requires ReadAll. This is the single most common Fabric permissions surprise.',
    fix: 'Grant ReadData on the lakehouse item in addition to the Viewer workspace role.',
  },
  {
    id: 'se-017',
    exam: 'DP-600',
    domainId: 'dp600-maintain',
    title: 'Lifecycle briefing — one claim is wrong',
    brief:
      'Notes from a CI/CD design session. One line does not survive contact with reality.',
    kind: 'claims',
    language: 'text',
    lines: [
      'A workspace connects to exactly one branch of one repository.',
      'Deployment rules are configured on the target stage of the pipeline.',
      'A deployment pipeline can have up to ten stages.',
      'A deployment pipeline can promote items between workspaces in different tenants.',
      'Each developer needing an isolated feature branch needs their own workspace.',
    ],
    faultyLine: 3,
    fault: 'Deployment pipelines are tenant-bound',
    explanation:
      'All stages of a deployment pipeline are workspaces within the same tenant. Moving content across tenants means exporting item definitions, or using Git plus a separate deployment in the target tenant — not a pipeline stage.',
    fix: 'All stages of a deployment pipeline live in the same tenant.',
  },
];

export function scenariosFor(exam: ExamId, domainIds?: string[]): SpotScenario[] {
  return SCENARIOS.filter(
    (s) =>
      s.exam === exam &&
      (!domainIds || domainIds.length === 0 || domainIds.includes(s.domainId))
  );
}
