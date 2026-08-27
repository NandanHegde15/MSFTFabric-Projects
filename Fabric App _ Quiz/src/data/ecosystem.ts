import type { ExamId } from './exams';

export type NodeKind = 'root' | 'workload' | 'item' | 'feature';
export type NodeShape = 'sphere' | 'box' | 'cylinder' | 'octahedron' | 'torus';

export interface EcoNode {
  id: string;
  name: string;
  kind: NodeKind;
  /** Hex colour. Inherited from the nearest ancestor that sets one. */
  color?: string;
  shape?: NodeShape;
  summary: string;
  /** Short, exam-relevant facts shown in the detail panel. */
  facts: string[];
  exam?: ExamId[];
  /** Domain to send the learner to when they want to practise this. */
  domainId?: string;
  /** Flashcard ids covering this node. Validated by the test suite. */
  cards?: string[];
  children?: EcoNode[];
}

export const ECOSYSTEM: EcoNode = {
  id: 'fabric',
  name: 'Microsoft Fabric',
  kind: 'root',
  shape: 'sphere',
  color: '#6366f1',
  summary:
    'One SaaS analytics platform. Every workload is billed from a single capacity and reads and writes the same storage, so the boundaries you see are experiences over shared foundations rather than separate products.',
  facts: [
    'All workloads draw compute from one capacity, measured in capacity units.',
    'All workloads persist to OneLake as Delta Parquet.',
    'A workspace is the unit of access control and lifecycle; a domain groups workspaces.',
  ],
  children: [
    // ==================================================== OneLake
    {
      id: 'onelake',
      name: 'OneLake',
      kind: 'workload',
      color: '#0ea5e9',
      shape: 'cylinder',
      summary:
        'The single, tenant-wide data lake provisioned automatically with the tenant. Built on ADLS Gen2 and addressable through the same APIs, it is where every other workload actually stores its data.',
      facts: [
        'One per tenant — you cannot create a second or turn it off.',
        'Tables are stored as Delta Parquet, readable by every engine.',
        'Think of it as OneDrive for data: one namespace, many experiences.',
      ],
      exam: ['DP-600', 'DP-700'],
      domainId: 'dp600-prepare',
      cards: ['fc-600-011'],
      children: [
        {
          id: 'onelake-shortcut',
          name: 'Shortcuts',
          kind: 'item',
          summary:
            'A pointer to data that lives elsewhere. No copy, no refresh schedule, always current.',
          facts: [
            'Internal shortcuts target other OneLake locations.',
            'External shortcuts reach ADLS Gen2, Amazon S3, S3-compatible stores, Google Cloud Storage and Dataverse.',
            'Deleting a shortcut never deletes the source.',
          ],
          exam: ['DP-600', 'DP-700'],
          domainId: 'dp600-prepare',
          cards: ['fc-600-012'],
          children: [
            {
              id: 'onelake-shortcut-internal',
              name: 'Internal shortcut',
              kind: 'feature',
              summary:
                'Points at another OneLake path, letting two workspaces share one physical copy of a table.',
              facts: [
                'The classic fix for "the same dimension copied into four lakehouses".',
                'Permissions are evaluated against the target, not the shortcut.',
              ],
            },
            {
              id: 'onelake-shortcut-external',
              name: 'External shortcut',
              kind: 'feature',
              summary:
                'Points at storage outside Fabric, so queries read through to the remote store.',
              facts: [
                'Performance depends on the remote store and the network.',
                'Reaching a firewalled account needs a workspace identity and trusted workspace access.',
              ],
              cards: ['fc-700-019'],
            },
            {
              id: 'onelake-shortcut-transform',
              name: 'Shortcut transformations',
              kind: 'feature',
              summary:
                'Shortcuts read data as it is — they perform no transformation on the way through.',
              facts: [
                'If the shape must change, land the data with a pipeline, dataflow or notebook instead.',
              ],
            },
          ],
        },
        {
          id: 'onelake-delta',
          name: 'Delta Parquet',
          kind: 'item',
          summary:
            'The open table format underneath every Fabric table: Parquet data files plus a transaction log.',
          facts: [
            'The _delta_log gives ACID transactions, schema enforcement and time travel.',
            'Time travel only reaches as far back as VACUUM has kept files.',
          ],
          exam: ['DP-600', 'DP-700'],
          domainId: 'dp600-prepare',
          cards: ['fc-600-016'],
          children: [
            {
              id: 'onelake-vorder',
              name: 'V-Order',
              kind: 'feature',
              summary:
                'A write-time optimisation — sorting, row-group distribution, dictionary encoding — that makes files far cheaper for the VertiPaq engine to read.',
              facts: [
                'Output stays fully Parquet-compliant and readable by any engine.',
                'It costs write time, so it is often disabled for bronze and staging tables.',
              ],
              cards: ['fc-600-017'],
            },
            {
              id: 'onelake-maintenance',
              name: 'OPTIMIZE and VACUUM',
              kind: 'feature',
              summary:
                'OPTIMIZE compacts small files. VACUUM deletes files the log no longer references, outside the retention window.',
              facts: [
                'They solve different problems: read performance versus reclaiming storage.',
                'Default VACUUM retention is seven days.',
              ],
              cards: ['fc-600-018', 'fc-700-048'],
            },
            {
              id: 'onelake-smallfiles',
              name: 'Small file problem',
              kind: 'feature',
              summary:
                'Thousands of tiny Parquet files force the engine to read far more metadata than data.',
              facts: [
                'Caused by frequent micro-batches and over-partitioning.',
                'The fix is OPTIMIZE and coarser partitions, not a bigger Spark pool.',
              ],
              cards: ['fc-600-026', 'fc-700-047'],
            },
          ],
        },
        {
          id: 'onelake-security',
          name: 'OneLake data access roles',
          kind: 'item',
          summary:
            'Folder-level security on a lakehouse: a role lists the folders or tables its members can read.',
          facts: [
            'Path-based, so it applies to Spark and the OneLake APIs.',
            'Coarse by design — row filtering still needs SQL RLS or semantic model RLS.',
          ],
          exam: ['DP-600', 'DP-700'],
          domainId: 'dp700-implement',
          cards: ['fc-600-009', 'fc-600-010'],
        },
        {
          id: 'onelake-catalog',
          name: 'OneLake catalog',
          kind: 'item',
          summary:
            'The discovery surface across the tenant: browse, search and filter items you have access to, with endorsement and lineage surfaced.',
          facts: [
            'Endorsement (Promoted, Certified, Master data) is a trust signal only — it grants no permission.',
            'Domains filter the catalog along business lines.',
          ],
          exam: ['DP-600'],
          domainId: 'dp600-maintain',
          cards: ['fc-600-003', 'fc-700-009'],
        },
      ],
    },

    // ==================================================== Data Factory
    {
      id: 'df',
      name: 'Data Factory',
      kind: 'workload',
      color: '#2563eb',
      shape: 'box',
      summary:
        'The ingestion and orchestration experience: move data in, transform it, and schedule the whole thing.',
      facts: [
        'Pipelines orchestrate; dataflows transform; copy jobs and mirroring move data.',
        'Choose by team skill and the shape of the job, not by data volume alone.',
      ],
      exam: ['DP-600', 'DP-700'],
      domainId: 'dp700-ingest',
      cards: ['fc-700-013'],
      children: [
        {
          id: 'df-pipeline',
          name: 'Data pipelines',
          kind: 'item',
          summary:
            'Low-code control flow: a graph of activities with dependencies, parameters, schedules and event triggers.',
          facts: [
            'Activities have retry, retry interval and timeout under General settings.',
            'Retries are only safe when the write is idempotent.',
          ],
          exam: ['DP-600', 'DP-700'],
          domainId: 'dp700-implement',
          cards: ['fc-700-013', 'fc-700-045'],
          children: [
            {
              id: 'df-activities',
              name: 'Activities',
              kind: 'feature',
              summary:
                'The units of work: Copy, Notebook, Dataflow, Script, Stored procedure, Lookup, Get metadata, Web, plus control flow — ForEach, If, Switch, Until, Wait, Fail, Set variable, Invoke pipeline.',
              facts: [
                'ForEach runs iterations in parallel unless Sequential is ticked (default batch of 20).',
                'Mutating a pipeline variable inside a parallel ForEach is a race condition.',
              ],
              cards: ['fc-700-015'],
            },
            {
              id: 'df-connections',
              name: 'Connections and gateways',
              kind: 'feature',
              summary:
                'Reusable, credential-bearing definitions of a source or sink, shared across pipelines and dataflows.',
              facts: [
                'On-premises sources need a data gateway; VNet sources need a VNet gateway.',
                'Firewalled ADLS Gen2 is better reached with a workspace identity and trusted workspace access than with keys.',
              ],
              cards: ['fc-700-019'],
            },
            {
              id: 'df-parameters',
              name: 'Parameters and dynamic content',
              kind: 'feature',
              summary:
                'Expressions such as @pipeline(), @activity(), @variables() and @item() computed at run time.',
              facts: [
                'Parameters are set per run and passed down; variables are mutable within a run.',
                'Dynamic content is what makes one pipeline serve a metadata-driven table list.',
              ],
              cards: ['fc-700-015'],
            },
            {
              id: 'df-triggers',
              name: 'Schedules and triggers',
              kind: 'feature',
              summary:
                'A pipeline runs on a schedule or on an event. Fabric event triggers are backed by Activator.',
              facts: [
                'OneLake, Azure Blob Storage and Fabric workspace item events can all fire a run.',
                'File-arrival triggers are event-driven — do not model them as a one-minute schedule.',
              ],
              cards: ['fc-700-014'],
            },
            {
              id: 'df-monitoring',
              name: 'Run history',
              kind: 'feature',
              summary:
                'Every run, its activities and their inputs, outputs and errors, surfaced in the Monitoring hub.',
              facts: [
                'The Monitoring hub answers "did it run"; the Capacity Metrics app answers "what did it cost".',
              ],
              cards: ['fc-700-037'],
            },
          ],
        },
        {
          id: 'df-dataflow',
          name: 'Dataflow Gen2',
          kind: 'item',
          summary:
            'Power Query Online at Fabric scale: low-code M transformations with an explicit output destination.',
          facts: [
            'Without a configured data destination it computes results and persists nothing.',
            'Staging helps large sources; fast copy speeds up bulk ingestion.',
          ],
          exam: ['DP-600', 'DP-700'],
          domainId: 'dp600-prepare',
          cards: ['fc-600-020'],
          children: [
            {
              id: 'df-dataflow-m',
              name: 'Power Query (M)',
              kind: 'feature',
              summary:
                'The transformation language. Steps that fold are pushed down to the source as native queries.',
              facts: [
                'Table.Buffer breaks folding — everything after it runs in the mashup engine.',
                'Broken folding is the usual reason an incremental refresh still scans everything.',
              ],
              cards: ['fc-600-034'],
            },
            {
              id: 'df-dataflow-dest',
              name: 'Data destinations',
              kind: 'feature',
              summary:
                'Where the output lands: lakehouse, warehouse, KQL database or SQL database.',
              facts: [
                'Append versus replace is chosen per destination.',
                'No destination means no persisted output, however green the refresh looks.',
              ],
            },
            {
              id: 'df-dataflow-staging',
              name: 'Staging',
              kind: 'feature',
              summary:
                'Lands intermediate results in a staging lakehouse so heavy transformations execute at scale.',
              facts: [
                'Useful for large joins; unnecessary overhead for small, foldable queries.',
              ],
            },
          ],
        },
        {
          id: 'df-copyjob',
          name: 'Copy job',
          kind: 'item',
          summary:
            'A standalone item for full and incremental copy, with change tracking and a guided setup — no watermark logic to build yourself.',
          facts: [
            'Copy activity is a step you orchestrate; Copy job is a managed item.',
            'The right answer when a scenario says "incremental, without building the logic".',
          ],
          exam: ['DP-700'],
          domainId: 'dp700-ingest',
          cards: ['fc-700-025'],
        },
        {
          id: 'df-mirroring',
          name: 'Mirroring',
          kind: 'item',
          summary:
            'Near-real-time, low-cost replication of an operational database into OneLake as Delta tables.',
          facts: [
            'Sources include Azure SQL Database and Managed Instance, Cosmos DB, Azure Database for PostgreSQL and Snowflake.',
            'The mirrored copy is read-only in Fabric — write back to the source.',
            'Flat files have no change feed, so they cannot be mirrored.',
          ],
          exam: ['DP-600', 'DP-700'],
          domainId: 'dp600-prepare',
          cards: ['fc-600-013'],
          children: [
            {
              id: 'df-mirroring-landing',
              name: 'Landing zone',
              kind: 'feature',
              summary:
                'Changes arrive in OneLake and are materialised into Delta tables for you.',
              facts: [
                'A SQL analytics endpoint is generated over the mirrored tables automatically.',
              ],
            },
            {
              id: 'df-mirroring-vs',
              name: 'Mirroring vs shortcut vs copy',
              kind: 'feature',
              summary:
                'Mirroring replicates a database continuously; a shortcut virtualises object storage; a copy moves a snapshot on a schedule.',
              facts: [
                'Pick mirroring for operational databases, shortcuts for lakes, copy for everything else.',
              ],
              cards: ['fc-600-012', 'fc-700-025'],
            },
          ],
        },
        {
          id: 'df-airflow',
          name: 'Apache Airflow job',
          kind: 'item',
          summary:
            'Managed Airflow for teams that already own Python DAGs or need complex dependency graphs.',
          facts: [
            'Choose it over pipelines when the orchestration logic is genuinely code, not a canvas.',
          ],
          exam: ['DP-700'],
          domainId: 'dp700-implement',
          cards: ['fc-700-013'],
        },
      ],
    },

    // ==================================================== Data Engineering
    {
      id: 'de',
      name: 'Data Engineering',
      kind: 'workload',
      color: '#0d9488',
      shape: 'box',
      summary:
        'Spark and the lakehouse: files, Delta tables, notebooks and the compute that runs them.',
      facts: [
        'A lakehouse is files plus Delta tables with a read-only SQL analytics endpoint.',
        'Compute comes from starter pools or custom pools, configured through environments.',
      ],
      exam: ['DP-600', 'DP-700'],
      domainId: 'dp700-ingest',
      cards: ['fc-600-014'],
      children: [
        {
          id: 'de-lakehouse',
          name: 'Lakehouse',
          kind: 'item',
          summary:
            'Files plus Delta tables in OneLake, written with Spark or pipelines, queried with Spark or read-only T-SQL.',
          facts: [
            'The SQL analytics endpoint supports SELECT, views, functions and security objects — never INSERT, UPDATE or DELETE.',
            'If the team needs T-SQL writes, the answer is a warehouse.',
          ],
          exam: ['DP-600', 'DP-700'],
          domainId: 'dp600-prepare',
          cards: ['fc-600-014', 'fc-600-015'],
          children: [
            {
              id: 'de-lh-files',
              name: 'Files section',
              kind: 'feature',
              summary:
                'Unmanaged storage for anything: CSV, JSON, images, raw landings.',
              facts: ['The usual landing spot for bronze data before it becomes a table.'],
            },
            {
              id: 'de-lh-tables',
              name: 'Tables section',
              kind: 'feature',
              summary:
                'Managed Delta tables, discoverable by the SQL endpoint and by Direct Lake semantic models.',
              facts: [
                'Only Delta tables appear here; other formats stay in Files.',
              ],
              cards: ['fc-600-016'],
            },
            {
              id: 'de-lh-endpoint',
              name: 'SQL analytics endpoint',
              kind: 'feature',
              summary:
                'A read-only T-SQL surface generated automatically over the Delta tables.',
              facts: [
                'You can create views and a security policy on it even though you cannot modify data.',
                'Reading through it needs the ReadData item permission, which Viewer does not grant.',
              ],
              cards: ['fc-600-015', 'fc-600-002'],
            },
            {
              id: 'de-lh-maintenance',
              name: 'Table maintenance',
              kind: 'feature',
              summary:
                'A table action that runs OPTIMIZE, optionally applies V-Order, and runs VACUUM with a chosen retention.',
              facts: [
                'The routine fix for Delta read performance degrading over time.',
              ],
              cards: ['fc-700-048'],
            },
          ],
        },
        {
          id: 'de-notebook',
          name: 'Notebook',
          kind: 'item',
          summary:
            'Code-first Spark development in PySpark, Scala, Spark SQL or R, and a first-class pipeline activity.',
          facts: [
            'notebookutils drives orchestration when the logic is too dynamic for a canvas.',
            'Magics such as %%sql switch language per cell.',
          ],
          exam: ['DP-600', 'DP-700'],
          domainId: 'dp700-ingest',
          cards: ['fc-700-013'],
          children: [
            {
              id: 'de-nb-wrangler',
              name: 'Data Wrangler',
              kind: 'feature',
              summary:
                'A grid tool for exploring and cleaning a DataFrame that generates the equivalent PySpark code.',
              facts: ['You paste the generated code back into the notebook.'],
              cards: ['fc-700-036'],
            },
            {
              id: 'de-nb-streaming',
              name: 'Structured streaming',
              kind: 'feature',
              summary:
                'Spark reads a stream and writes it continuously, with a checkpoint holding offsets and state.',
              facts: [
                'One checkpoint location per query — sharing one corrupts both.',
                'A windowed aggregation without withWatermark grows state without bound.',
              ],
              cards: ['fc-700-029', 'fc-700-030'],
            },
            {
              id: 'de-nb-merge',
              name: 'MERGE and upsert',
              kind: 'feature',
              summary:
                'One statement that updates matched rows, inserts unmatched, and optionally deletes.',
              facts: [
                'Two source rows for one key aborts the merge — deduplicate first.',
                'MERGE is what makes a retried load idempotent.',
              ],
              cards: ['fc-700-024', 'fc-700-023'],
            },
          ],
        },
        {
          id: 'de-spark',
          name: 'Spark compute',
          kind: 'item',
          summary:
            'The pools and settings behind every notebook and Spark job definition.',
          facts: [
            'Starter pools are pre-warmed and start in seconds; custom pools cold-start.',
            'High concurrency mode shares one session, and its capacity units, across notebooks.',
          ],
          exam: ['DP-700'],
          domainId: 'dp700-implement',
          cards: ['fc-700-010', 'fc-700-012'],
          children: [
            {
              id: 'de-spark-env',
              name: 'Environment item',
              kind: 'feature',
              summary:
                'A reusable bundle of Spark compute settings, Spark properties and libraries.',
              facts: [
                'Library changes need the environment published before sessions pick them up.',
                'Attaching custom libraries forfeits the pre-warmed start.',
              ],
              cards: ['fc-700-011'],
            },
            {
              id: 'de-spark-tuning',
              name: 'Performance tuning',
              kind: 'feature',
              summary:
                'Autotune sets per-query configuration from history; skew and partitioning are the usual culprits.',
              facts: [
                'One task far slower than the rest of its stage is skew, not an undersized pool.',
                'Partition on the coarse column queries actually filter, or not at all.',
              ],
              cards: ['fc-700-046', 'fc-700-047'],
            },
          ],
        },
        {
          id: 'de-sjd',
          name: 'Spark job definition',
          kind: 'item',
          summary:
            'A packaged Spark application — a JAR or Python file with arguments — run on a schedule or from a pipeline.',
          facts: [
            'The right shape for productionised code that does not belong in a notebook.',
          ],
          exam: ['DP-700'],
          domainId: 'dp700-implement',
        },
      ],
    },

    // ==================================================== Data Warehouse
    {
      id: 'dw',
      name: 'Data Warehouse',
      kind: 'workload',
      color: '#7c3aed',
      shape: 'box',
      summary:
        'A full T-SQL engine over OneLake with DDL, DML and multi-table transactions.',
      facts: [
        'The answer whenever a scenario needs INSERT, UPDATE, DELETE or stored procedures.',
        'Data still lands in OneLake as Delta, so Spark and Direct Lake can read it.',
      ],
      exam: ['DP-600', 'DP-700'],
      domainId: 'dp600-prepare',
      cards: ['fc-600-014'],
      children: [
        {
          id: 'dw-tsql',
          name: 'T-SQL surface',
          kind: 'item',
          summary:
            'Tables, views, functions and stored procedures, with cross-database queries inside a workspace.',
          facts: [
            'Three-part naming reaches other warehouses and lakehouse endpoints in the same workspace.',
            'PRIMARY KEY, UNIQUE and FOREIGN KEY exist only as NOT ENFORCED metadata.',
          ],
          exam: ['DP-600'],
          domainId: 'dp600-prepare',
          cards: ['fc-600-023', 'fc-600-025'],
          children: [
            {
              id: 'dw-constraints',
              name: 'NOT ENFORCED constraints',
              kind: 'feature',
              summary:
                'The engine never validates them, but the optimiser still trusts them when building plans.',
              facts: [
                'Declaring uniqueness that does not hold produces silently wrong results.',
              ],
              cards: ['fc-600-023'],
            },
            {
              id: 'dw-copyinto',
              name: 'COPY INTO',
              kind: 'feature',
              summary:
                'The high-throughput bulk load statement for PARQUET or CSV from ADLS Gen2 or Blob storage.',
              facts: [
                'Supports wildcards, an ERRORFILE for rejected rows and several credential types.',
                'FILE_TYPE must match the physical format, whatever the file extension says.',
              ],
              cards: ['fc-600-024'],
            },
            {
              id: 'dw-statistics',
              name: 'Statistics',
              kind: 'feature',
              summary:
                'Column statistics drive plan quality. Fabric maintains them automatically, and you can update them manually.',
              facts: [
                'Worth an explicit UPDATE STATISTICS after a very large load.',
              ],
              cards: ['fc-700-042'],
            },
          ],
        },
        {
          id: 'dw-security',
          name: 'Warehouse security',
          kind: 'item',
          summary:
            'Row-level, column-level and dynamic data masking, layered on top of item permissions.',
          facts: [
            'RLS is a schemabound inline table-valued function bound by CREATE SECURITY POLICY.',
            'Masking is presentation-only — a determined user can still infer values with a WHERE clause.',
          ],
          exam: ['DP-700'],
          domainId: 'dp700-implement',
          cards: ['fc-700-017', 'fc-700-018'],
          children: [
            {
              id: 'dw-rls',
              name: 'Row-level security',
              kind: 'feature',
              summary:
                'A FILTER PREDICATE that returns a row when access is allowed.',
              facts: [
                'The function must be inline and created WITH SCHEMABINDING or the policy will not create.',
                'SQL RLS on an endpoint forces a Direct Lake model to fall back to DirectQuery.',
              ],
              cards: ['fc-700-018', 'fc-600-010'],
            },
            {
              id: 'dw-ddm',
              name: 'Dynamic data masking',
              kind: 'feature',
              summary:
                'Obscures column values in results for unprivileged users. Data on disk is unchanged.',
              facts: [
                'UNMASK exempts a principal from every mask in the database — grant it sparingly.',
              ],
              cards: ['fc-700-017'],
            },
          ],
        },
        {
          id: 'dw-perf',
          name: 'Performance',
          kind: 'item',
          summary:
            'Query insights for history, DMVs for what is running now, and transparent result set caching.',
          facts: [
            'queryinsights.exec_requests_history holds completed queries.',
            'sys.dm_exec_requests shows currently executing requests.',
          ],
          exam: ['DP-700'],
          domainId: 'dp700-monitor',
          cards: ['fc-700-039', 'fc-700-043'],
        },
        {
          id: 'dw-dbproject',
          name: 'Database projects',
          kind: 'item',
          summary:
            'Warehouse schema as source, built into a dacpac and published as a declarative diff.',
          facts: [
            'The state-based deployment model that fits a CI pipeline.',
            'Deployment pipelines are portal-driven promotion, not a build artefact.',
          ],
          exam: ['DP-700'],
          domainId: 'dp700-implement',
          cards: ['fc-700-016'],
        },
      ],
    },

    // ==================================================== Real-Time Intelligence
    {
      id: 'rti',
      name: 'Real-Time Intelligence',
      kind: 'workload',
      color: '#db2777',
      shape: 'octahedron',
      summary:
        'Streaming ingestion, time-series storage and event-driven action: eventstreams, eventhouses and Activator.',
      facts: [
        'Built for high-volume telemetry and logs with sub-second queries.',
        'KQL is the query language; eventhouse is the storage.',
      ],
      exam: ['DP-700'],
      domainId: 'dp700-ingest',
      cards: ['fc-700-027'],
      children: [
        {
          id: 'rti-eventstream',
          name: 'Eventstream',
          kind: 'item',
          summary:
            'No-code streaming ingestion: sources, an event-processing canvas, and destinations.',
          facts: [
            'Sources include Event Hubs, IoT Hub, Kafka, CDC feeds and custom endpoints.',
            'A lakehouse destination writes micro-batches, so plan table maintenance.',
          ],
          exam: ['DP-700'],
          domainId: 'dp700-ingest',
          cards: ['fc-700-027'],
          children: [
            {
              id: 'rti-es-processing',
              name: 'Event processing',
              kind: 'feature',
              summary:
                'Filter, manage fields, aggregate, join, union and expand, applied to the stream in flight.',
              facts: ['Derived streams let one processed stream feed several destinations.'],
            },
            {
              id: 'rti-es-windows',
              name: 'Windowing functions',
              kind: 'feature',
              summary:
                'Tumbling, hopping, sliding, session and snapshot windows group events over time.',
              facts: [
                'Tumbling is the only one where every event belongs to exactly one window.',
                'Hopping and sliding overlap; session windows are defined by gaps.',
              ],
              cards: ['fc-700-028'],
            },
            {
              id: 'rti-es-late',
              name: 'Late and out-of-order events',
              kind: 'feature',
              summary:
                'A watermark declares how late an event may arrive and still be counted.',
              facts: ['Without one, state must be kept forever in case something turns up.'],
              cards: ['fc-700-029'],
            },
          ],
        },
        {
          id: 'rti-eventhouse',
          name: 'Eventhouse',
          kind: 'item',
          summary:
            'The storage engine: one or more KQL databases tuned for time-series and log data.',
          facts: [
            'Retention decides how long data exists; caching decides how much sits on SSD.',
            'A caching period longer than retention is meaningless.',
          ],
          exam: ['DP-700'],
          domainId: 'dp700-monitor',
          cards: ['fc-700-044'],
          children: [
            {
              id: 'rti-eh-update',
              name: 'Update policy',
              kind: 'feature',
              summary:
                'A rule on the target table that runs a KQL query over each ingested batch of a source table and appends the result.',
              facts: [
                'It lives on the table that receives the rows, and names the source it watches.',
                'It runs on ingestion only — it never backfills existing rows.',
              ],
              cards: ['fc-700-031'],
            },
            {
              id: 'rti-eh-mv',
              name: 'Materialized view',
              kind: 'feature',
              summary:
                'A persisted, incrementally maintained aggregation over a source table.',
              facts: [
                'Classic uses: arg_max for the latest row per key, or a daily summarize.',
              ],
              cards: ['fc-700-032'],
            },
            {
              id: 'rti-eh-onelake',
              name: 'OneLake availability',
              kind: 'feature',
              summary:
                'Writes KQL tables to OneLake in Delta format as one logical copy.',
              facts: [
                'Spark, the SQL endpoint and Direct Lake models can then read the same telemetry.',
                'The OneLake copy is read-only from outside the eventhouse.',
              ],
              cards: ['fc-700-033'],
            },
          ],
        },
        {
          id: 'rti-activator',
          name: 'Activator',
          kind: 'item',
          summary:
            'No-code detection and action: watch a stream, a visual or a Fabric event, and trigger something.',
          facts: [
            'Stateful per object, so it expresses "stays above 60 degrees for 10 minutes".',
            'Actions include email, Teams, Power Automate and running a Fabric item.',
            'It is also what backs pipeline event triggers.',
          ],
          exam: ['DP-700'],
          domainId: 'dp700-monitor',
          cards: ['fc-700-020', 'fc-700-014'],
        },
        {
          id: 'rti-dashboard',
          name: 'Real-Time Dashboard',
          kind: 'item',
          summary:
            'KQL-backed tiles with auto-refresh, built for operational monitoring rather than business reporting.',
          facts: [
            'Querysets hold the KQL behind the tiles.',
          ],
          exam: ['DP-700'],
          domainId: 'dp700-monitor',
        },
      ],
    },

    // ==================================================== Power BI
    {
      id: 'pbi',
      name: 'Power BI',
      kind: 'workload',
      color: '#eab308',
      shape: 'cylinder',
      summary:
        'The semantic layer and reporting experience — where the modelling decisions on this exam actually get made.',
      facts: [
        'A semantic model sits between storage and every report built on it.',
        'Storage mode is the single biggest performance decision in the model.',
      ],
      exam: ['DP-600'],
      domainId: 'dp600-models',
      cards: ['fc-600-027'],
      children: [
        {
          id: 'pbi-model',
          name: 'Semantic model',
          kind: 'item',
          summary:
            'Tables, relationships, measures and security, served by the VertiPaq engine.',
          facts: [
            'Star schema is the shape both VertiPaq and DAX are designed for.',
            'Large semantic model storage format is needed beyond the default size limit.',
          ],
          exam: ['DP-600'],
          domainId: 'dp600-models',
          cards: ['fc-600-021', 'fc-600-033'],
          children: [
            {
              id: 'pbi-storage',
              name: 'Storage modes',
              kind: 'feature',
              summary:
                'Import caches in memory, DirectQuery leaves data at source, Dual behaves as either, Direct Lake reads Delta straight from OneLake.',
              facts: [
                'Dual stops a DirectQuery fact dragging Import dimensions into limited relationships.',
                'Direct Lake needs no refresh — there is no copy to refresh.',
              ],
              cards: ['fc-600-027', 'fc-600-029', 'fc-600-030'],
            },
            {
              id: 'pbi-directlake',
              name: 'Direct Lake fallback',
              kind: 'feature',
              summary:
                'When a query cannot be served from OneLake, the model silently falls back to DirectQuery over the SQL endpoint.',
              facts: [
                'Triggered by SQL views, SQL RLS, or capacity guardrails.',
                'DirectLakeBehavior: Automatic, DirectLakeOnly or DirectQueryOnly.',
                'Set DirectLakeOnly while testing so fallback fails loudly.',
              ],
              cards: ['fc-600-028'],
            },
            {
              id: 'pbi-dax',
              name: 'DAX',
              kind: 'feature',
              summary:
                'Measures evaluated in a filter context, modified by CALCULATE, with row context during iteration.',
              facts: [
                'A measure referenced inside an iterator triggers context transition on every row.',
                'DIVIDE handles a blank or zero denominator; the / operator does not.',
                'Time intelligence needs a marked date table, not a fact date column.',
              ],
              cards: ['fc-600-036', 'fc-600-037', 'fc-600-038'],
            },
            {
              id: 'pbi-calcgroups',
              name: 'Calculation groups',
              kind: 'feature',
              summary:
                'Calculation items using SELECTEDMEASURE() applied across any measure.',
              facts: [
                'Turns 36 near-identical time-intelligence measures into three definitions.',
                'Two groups touching one measure need explicit precedence.',
              ],
              cards: ['fc-600-031', 'fc-600-032'],
            },
            {
              id: 'pbi-modelsecurity',
              name: 'RLS and OLS',
              kind: 'feature',
              summary:
                'RLS filters rows with a DAX expression on a role. OLS hides whole tables or columns.',
              facts: [
                'OLS is defined with external tools such as Tabular Editor, not the Desktop dialog.',
                'A hidden column makes dependent visuals error rather than degrade.',
              ],
              cards: ['fc-600-035'],
            },
            {
              id: 'pbi-refresh',
              name: 'Incremental refresh',
              kind: 'feature',
              summary:
                'Partitions by date using the RangeStart and RangeEnd parameters and refreshes only recent partitions.',
              facts: [
                'The filter must fold to the source or every partition scans the whole table.',
                'Detect data changes and a real-time DirectQuery partition are optional extras.',
              ],
              cards: ['fc-600-034'],
            },
          ],
        },
        {
          id: 'pbi-report',
          name: 'Reports',
          kind: 'item',
          summary:
            'Interactive reports, paginated reports and dashboards built on a semantic model.',
          facts: [
            'Building a report from a shared model needs the Build permission.',
            'Performance Analyzer breaks a slow page down per visual and exposes its DAX.',
          ],
          exam: ['DP-600'],
          domainId: 'dp600-models',
          cards: ['fc-600-039', 'fc-600-002'],
        },
        {
          id: 'pbi-tools',
          name: 'External tools',
          kind: 'item',
          summary:
            'Tabular Editor, DAX Studio and the Best Practice Analyzer, connected through the XMLA endpoint.',
          facts: [
            'XMLA read/write is a capacity setting and is off by default.',
            'Publishing from Desktop over an XMLA-modified model can overwrite external changes.',
          ],
          exam: ['DP-600'],
          domainId: 'dp600-maintain',
          cards: ['fc-600-007', 'fc-600-039'],
        },
      ],
    },

    // ==================================================== Data Science
    {
      id: 'ds',
      name: 'Data Science',
      kind: 'workload',
      color: '#ea580c',
      shape: 'octahedron',
      summary:
        'Notebooks, experiments and models tracked with MLflow, reading the same OneLake tables as everything else.',
      facts: [
        'Experiments and models are first-class Fabric items.',
        'Predictions can be written straight back to a lakehouse table.',
      ],
      exam: ['DP-600'],
      domainId: 'dp600-prepare',
      children: [
        {
          id: 'ds-experiment',
          name: 'Experiment',
          kind: 'item',
          summary:
            'MLflow runs grouped together, with parameters, metrics and artefacts compared side by side.',
          facts: ['Runs are logged automatically from a notebook session.'],
        },
        {
          id: 'ds-model',
          name: 'ML model',
          kind: 'item',
          summary:
            'A registered, versioned model that can be applied in batch to a lakehouse table.',
          facts: ['Scoring output is just another Delta table, so Direct Lake can serve it.'],
        },
        {
          id: 'ds-aifunctions',
          name: 'AI functions and Copilot',
          kind: 'item',
          summary:
            'Language-model helpers inside notebooks, dataflows and the query editors.',
          facts: [
            'Copilot availability and free-viewer consumption both depend on capacity size — check the current SKU thresholds.',
          ],
          cards: ['fc-700-006'],
        },
      ],
    },

    // ==================================================== Platform
    {
      id: 'platform',
      name: 'Platform and governance',
      kind: 'workload',
      color: '#475569',
      shape: 'torus',
      summary:
        'The parts that are not a workload: capacity, workspaces, security, lifecycle and monitoring. Roughly a third of both exams lives here.',
      facts: [
        'Capacity is the billing and throttling boundary.',
        'Workspace is the access-control and lifecycle boundary.',
      ],
      exam: ['DP-600', 'DP-700'],
      domainId: 'dp700-implement',
      cards: ['fc-700-001'],
      children: [
        {
          id: 'plat-capacity',
          name: 'Capacity',
          kind: 'item',
          summary:
            'F SKUs from F2 to F2048, where the number is the capacity units. Every workload draws from the same pool.',
          facts: [
            'An F64 supplies 64 x 86,400 = 5,529,600 CU-seconds a day.',
            'F64 is the P1 equivalent and the threshold for free-licence viewing.',
            'The trial is a 60-day, F64-equivalent capacity.',
          ],
          exam: ['DP-700'],
          domainId: 'dp700-implement',
          cards: ['fc-700-001', 'fc-700-002', 'fc-700-006', 'fc-700-007'],
          children: [
            {
              id: 'plat-smoothing',
              name: 'Smoothing',
              kind: 'feature',
              summary:
                'Consumption is spread over time so short spikes do not throttle you.',
              facts: [
                'Background operations smooth over 24 hours.',
                'Interactive operations smooth over a minimum of five minutes.',
                'It is why a job that finished hours ago can be throttling you now.',
              ],
              cards: ['fc-700-003'],
            },
            {
              id: 'plat-throttling',
              name: 'Throttling stages',
              kind: 'feature',
              summary:
                'Driven by future smoothed consumption, the carry-forward.',
              facts: [
                'Under 10 minutes: overage protection, nothing throttled.',
                '10 to 60 minutes: interactive delay of about 20 seconds.',
                '60 minutes to 24 hours: interactive requests rejected.',
                'Beyond 24 hours: background jobs rejected too.',
              ],
              cards: ['fc-700-004'],
            },
            {
              id: 'plat-bursting',
              name: 'Bursting',
              kind: 'feature',
              summary:
                'A job may temporarily use more CUs than the SKU nominally provides so it finishes faster.',
              facts: ['Not free headroom — it is borrowing against the next 24 hours.'],
              cards: ['fc-700-005'],
            },
          ],
        },
        {
          id: 'plat-workspace',
          name: 'Workspaces and domains',
          kind: 'item',
          summary:
            'A workspace holds items and carries the roles. A domain groups workspaces along business lines.',
          facts: [
            'Roles, least to most privileged: Viewer, Contributor, Member, Admin.',
            'A workspace belongs to at most one domain; domains are governance, not security.',
          ],
          exam: ['DP-600', 'DP-700'],
          domainId: 'dp600-maintain',
          cards: ['fc-600-001', 'fc-700-009'],
          children: [
            {
              id: 'plat-roles',
              name: 'Workspace roles',
              kind: 'feature',
              summary:
                'Contributor creates and edits items; Member can also add members; Admin can delete the workspace.',
              facts: [
                'Viewer does not imply data access — that is the single most common Fabric permissions surprise.',
              ],
              cards: ['fc-600-001'],
            },
            {
              id: 'plat-itemperms',
              name: 'Item permissions',
              kind: 'feature',
              summary:
                'Read sees metadata, ReadData unlocks the SQL endpoint, ReadAll unlocks OneLake files, Build allows new content from a model.',
              facts: [
                'Sharing without ticking the extra boxes grants Read only, and every query then fails.',
              ],
              cards: ['fc-600-002'],
            },
            {
              id: 'plat-identity',
              name: 'Workspace identity',
              kind: 'feature',
              summary:
                'A managed service principal for the workspace, used by trusted workspace access.',
              facts: [
                'Lets Fabric reach a firewalled ADLS Gen2 account with no public network path.',
              ],
              cards: ['fc-700-019'],
            },
          ],
        },
        {
          id: 'plat-lifecycle',
          name: 'Lifecycle management',
          kind: 'item',
          summary:
            'Git integration for source control and deployment pipelines for promotion between environments.',
          facts: [
            'A workspace connects to exactly one branch of one repository.',
            'Deployment rules are configured on the target stage.',
            'All stages of a pipeline live in the same tenant.',
          ],
          exam: ['DP-600', 'DP-700'],
          domainId: 'dp600-maintain',
          cards: ['fc-600-005', 'fc-600-006'],
          children: [
            {
              id: 'plat-git',
              name: 'Git integration',
              kind: 'feature',
              summary:
                'Connects a workspace to Azure DevOps or GitHub, serialising supported items into folders.',
              facts: [
                'One workspace, one branch — feature-branch development needs a workspace per developer.',
              ],
              cards: ['fc-600-005'],
            },
            {
              id: 'plat-pipelines',
              name: 'Deployment pipelines',
              kind: 'feature',
              summary:
                'Promote content between stages, each backed by a workspace, with rules that repoint data sources.',
              facts: [
                'Up to ten stages.',
                'Rules only apply to content deployed through the pipeline.',
              ],
              cards: ['fc-600-006'],
            },
          ],
        },
        {
          id: 'plat-governance',
          name: 'Governance',
          kind: 'item',
          summary:
            'Sensitivity labels, endorsement and lineage — classification and trust rather than access control.',
          facts: [
            'Labels flow downstream through lineage and persist into exports.',
            'Certified endorsement is restricted to users the tenant admin authorises.',
          ],
          exam: ['DP-600'],
          domainId: 'dp600-maintain',
          cards: ['fc-600-003', 'fc-600-004', 'fc-600-008'],
        },
        {
          id: 'plat-monitoring',
          name: 'Monitoring',
          kind: 'item',
          summary:
            'The Monitoring hub for runs, the Capacity Metrics app for consumption, workspace monitoring for telemetry.',
          facts: [
            'The Monitoring hub answers "did it run"; the Metrics app answers "what did it cost" and "who caused the throttling".',
            'Workspace monitoring provisions a read-only eventhouse you query with KQL.',
          ],
          exam: ['DP-700'],
          domainId: 'dp700-monitor',
          cards: ['fc-700-037', 'fc-700-038', 'fc-700-041'],
        },
      ],
    },
  ],
};

/** Flattened index, with each node's ancestry, for lookup and breadcrumbs. */
export interface NodeIndexEntry {
  node: EcoNode;
  path: EcoNode[];
  color: string;
}

function buildIndex(): Map<string, NodeIndexEntry> {
  const index = new Map<string, NodeIndexEntry>();

  const walk = (node: EcoNode, path: EcoNode[], color: string) => {
    const resolved = node.color ?? color;
    index.set(node.id, { node, path: [...path, node], color: resolved });
    node.children?.forEach((child) => walk(child, [...path, node], resolved));
  };

  walk(ECOSYSTEM, [], ECOSYSTEM.color ?? '#6366f1');
  return index;
}

export const NODE_INDEX = buildIndex();

export function findNode(id: string): NodeIndexEntry | undefined {
  return NODE_INDEX.get(id);
}

export function countDescendants(node: EcoNode): number {
  if (!node.children) return 0;
  return node.children.reduce((t, c) => t + 1 + countDescendants(c), 0);
}
