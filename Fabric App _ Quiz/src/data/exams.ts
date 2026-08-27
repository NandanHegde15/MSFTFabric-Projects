export type ExamId = 'DP-600' | 'DP-700';

export interface Objective {
  id: string;
  title: string;
  /** Bullet points lifted from the exam skills outline. */
  points: string[];
}

export interface Domain {
  id: string;
  exam: ExamId;
  /** Short label used on charts and chips. */
  short: string;
  title: string;
  /** Official weighting band from the skills outline, in percent. */
  weight: [number, number];
  objectives: Objective[];
}

export interface Exam {
  id: ExamId;
  title: string;
  certification: string;
  blurb: string;
  skillsOutlineUrl: string;
}

export const EXAMS: Exam[] = [
  {
    id: 'DP-600',
    title: 'Implementing Analytics Solutions Using Microsoft Fabric',
    certification: 'Fabric Analytics Engineer Associate',
    blurb:
      'Lakehouse and warehouse modelling, semantic models, Direct Lake, DAX and the analytics development lifecycle.',
    skillsOutlineUrl:
      'https://learn.microsoft.com/credentials/certifications/resources/study-guides/dp-600',
  },
  {
    id: 'DP-700',
    title: 'Implementing Data Engineering Solutions Using Microsoft Fabric',
    certification: 'Fabric Data Engineer Associate',
    blurb:
      'Ingestion and transformation, streaming, orchestration, security and governance, monitoring and performance tuning.',
    skillsOutlineUrl:
      'https://learn.microsoft.com/credentials/certifications/resources/study-guides/dp-700',
  },
];

export const DOMAINS: Domain[] = [
  // ---------------------------------------------------------------- DP-600
  {
    id: 'dp600-maintain',
    exam: 'DP-600',
    short: 'Maintain',
    title: 'Maintain a data analytics solution',
    weight: [25, 30],
    objectives: [
      {
        id: 'dp600-maintain-security',
        title: 'Implement security and governance',
        points: [
          'Workspace-level and item-level access controls',
          'Row-level, column-level, object-level and file-level access control',
          'Sensitivity labels and endorsement (Promoted / Certified)',
          'Workspace and item-level settings, logging and auditing',
        ],
      },
      {
        id: 'dp600-maintain-lifecycle',
        title: 'Maintain the analytics development lifecycle',
        points: [
          'Version control with Git integration for a workspace',
          'Deployment pipelines and deployment rules',
          'Reusable assets: notebooks, semantic models, Power BI templates',
          'Impact analysis, XMLA endpoint, semantic model documentation',
        ],
      },
    ],
  },
  {
    id: 'dp600-prepare',
    exam: 'DP-600',
    short: 'Prepare',
    title: 'Prepare data',
    weight: [45, 50],
    objectives: [
      {
        id: 'dp600-prepare-get',
        title: 'Get data',
        points: [
          'Create a data connection and discover data in OneLake',
          'Ingest or access data via shortcuts, mirroring, pipelines, dataflows, notebooks and eventstreams',
          'Choose between a lakehouse, warehouse or eventhouse',
          'Copy data with the Copy activity and Copy job',
        ],
      },
      {
        id: 'dp600-prepare-transform',
        title: 'Transform data',
        points: [
          'Create views, functions and stored procedures',
          'Enrich data by adding new columns or tables',
          'Implement a star schema for a lakehouse or warehouse',
          'Denormalise, aggregate, merge, join, deduplicate and handle missing or late-arriving data',
          'Convert column data types and implement slowly changing dimensions',
        ],
      },
      {
        id: 'dp600-prepare-query',
        title: 'Query and analyse data',
        points: [
          'Select, filter and aggregate with the Visual Query editor',
          'Select, filter and aggregate with SQL, KQL and DAX',
        ],
      },
    ],
  },
  {
    id: 'dp600-models',
    exam: 'DP-600',
    short: 'Semantic models',
    title: 'Implement and manage semantic models',
    weight: [25, 30],
    objectives: [
      {
        id: 'dp600-models-design',
        title: 'Design and build semantic models',
        points: [
          'Choose a storage mode: Import, DirectQuery, Direct Lake or Dual',
          'Identify use cases for and implement a composite model',
          'Design and build relationships, calculation groups and dynamic format strings',
          'Implement row-level and object-level security in a semantic model',
          'Write calculations that use DAX variables and functions',
        ],
      },
      {
        id: 'dp600-models-optimize',
        title: 'Optimise enterprise-scale semantic models',
        points: [
          'Improve performance with Performance Analyzer, DAX Query View and Best Practice Analyzer',
          'Improve DAX performance',
          'Configure large semantic model storage format',
          'Implement incremental refresh',
        ],
      },
    ],
  },

  // ---------------------------------------------------------------- DP-700
  {
    id: 'dp700-implement',
    exam: 'DP-700',
    short: 'Implement',
    title: 'Implement and manage an analytics solution',
    weight: [30, 35],
    objectives: [
      {
        id: 'dp700-implement-workspace',
        title: 'Configure Microsoft Fabric workspace settings',
        points: [
          'Configure Spark workspace settings, environments and pools',
          'Configure domain workspace settings',
          'Configure OneLake workspace settings',
          'Configure data workflow (Apache Airflow job) workspace settings',
        ],
      },
      {
        id: 'dp700-implement-lifecycle',
        title: 'Implement lifecycle management',
        points: [
          'Configure version control and Git integration',
          'Implement database projects',
          'Create and configure deployment pipelines',
        ],
      },
      {
        id: 'dp700-implement-security',
        title: 'Configure security and governance',
        points: [
          'Implement workspace-level, item-level and OneLake data access roles',
          'Implement row-level, column-level and dynamic data masking security',
          'Apply sensitivity labels to items',
          'Endorse items and configure workspace and item-level logging',
        ],
      },
      {
        id: 'dp700-implement-orchestrate',
        title: 'Orchestrate processes',
        points: [
          'Choose between a pipeline, notebook and Apache Airflow job',
          'Design and implement schedules and event-based triggers',
          'Implement orchestration patterns with notebooks and pipelines, including parameters and dynamic expressions',
        ],
      },
    ],
  },
  {
    id: 'dp700-ingest',
    exam: 'DP-700',
    short: 'Ingest',
    title: 'Ingest and transform data',
    weight: [30, 35],
    objectives: [
      {
        id: 'dp700-ingest-patterns',
        title: 'Design and implement loading patterns',
        points: [
          'Design and implement full and incremental data loads',
          'Prepare data for loading into a dimensional model',
          'Design and implement a loading pattern for streaming data',
        ],
      },
      {
        id: 'dp700-ingest-batch',
        title: 'Ingest and transform batch data',
        points: [
          'Choose an appropriate data store and data load mode',
          'Transform data with T-SQL, PySpark, KQL and Dataflow Gen2',
          'Denormalise, group, aggregate and pivot data',
          'Handle duplicate, missing and late-arriving data',
          'Use shortcuts and mirroring as ingest-free access patterns',
        ],
      },
      {
        id: 'dp700-ingest-stream',
        title: 'Ingest and transform streaming data',
        points: [
          'Choose an appropriate streaming engine: eventstreams, Spark structured streaming or KQL',
          'Process data with eventstreams and windowing functions',
          'Handle late-arriving and out-of-order events',
        ],
      },
    ],
  },
  {
    id: 'dp700-monitor',
    exam: 'DP-700',
    short: 'Monitor',
    title: 'Monitor and optimise an analytics solution',
    weight: [30, 35],
    objectives: [
      {
        id: 'dp700-monitor-items',
        title: 'Monitor Fabric items',
        points: [
          'Monitor data ingestion, transformation and semantic model refresh',
          'Configure alerts with Activator',
          'Use the Monitoring hub and the Capacity Metrics app',
        ],
      },
      {
        id: 'dp700-monitor-errors',
        title: 'Identify and resolve errors',
        points: [
          'Identify and resolve pipeline, dataflow, notebook, eventhouse and eventstream errors',
          'Identify and resolve T-SQL errors',
        ],
      },
      {
        id: 'dp700-monitor-optimize',
        title: 'Optimise performance',
        points: [
          'Optimise lakehouse tables, pipelines, the warehouse and Spark',
          'Optimise eventstreams and eventhouses',
          'Identify and resolve data loading performance bottlenecks',
          'Optimise queries and small-file or shortcut layout',
        ],
      },
    ],
  },
];

export const EXAM_IDS: ExamId[] = ['DP-600', 'DP-700'];

export function domainsFor(exam: ExamId): Domain[] {
  return DOMAINS.filter((d) => d.exam === exam);
}

export function domainById(id: string): Domain | undefined {
  return DOMAINS.find((d) => d.id === id);
}

export function examById(id: ExamId): Exam {
  return EXAMS.find((e) => e.id === id)!;
}
