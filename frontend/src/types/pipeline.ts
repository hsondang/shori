export type NodeType = 'csv_source' | 'db_source' | 'transform' | 'export'
export type DbType = 'oracle' | 'postgres'
export type ConnectionScope = 'local' | 'global'
export type NodeStatus = 'idle' | 'connecting' | 'running' | 'success' | 'error' | 'cancelled'
export type NodeLabelMode = 'auto' | 'custom'
export type NodeEditorMode = 'closed' | 'create' | 'edit'

export interface PostgresConnectionConfig {
  host: string
  port: number
  database: string
  user: string
  password: string
}

export interface OracleConnectionConfig {
  host: string
  port: number
  service_name: string
  user: string
  password: string
}

export type OracleFetchMode = 'fetchall' | 'fetchmany'

export interface OracleFetchConfig {
  mode: OracleFetchMode
  arraysize: number
  prefetchrows: number
}

export type DatabaseConnectionConfig = PostgresConnectionConfig | OracleConnectionConfig

export interface CsvSourceConfig {
  file_path: string
  original_filename: string
  preprocessing?: CsvPreprocessingConfig
}

export interface CsvPreprocessingConfig {
  enabled: boolean
  runtime: 'python' | 'bash'
  script: string
}

export interface PostgresDatabaseSourceConfig {
  connection_mode?: 'local'
  db_type: 'postgres'
  connection: PostgresConnectionConfig
  query: string
}

export interface OracleDatabaseSourceConfig {
  connection_mode?: 'local'
  db_type: 'oracle'
  connection: OracleConnectionConfig
  query: string
  fetch_config?: OracleFetchConfig
}

export interface GlobalPostgresDatabaseSourceConfig {
  connection_mode: 'global'
  connection_source_id: string
  db_type: 'postgres'
  query: string
}

export interface GlobalOracleDatabaseSourceConfig {
  connection_mode: 'global'
  connection_source_id: string
  db_type: 'oracle'
  query: string
  fetch_config?: OracleFetchConfig
}

export type DatabaseSourceConfig =
  | PostgresDatabaseSourceConfig
  | OracleDatabaseSourceConfig
  | GlobalPostgresDatabaseSourceConfig
  | GlobalOracleDatabaseSourceConfig

export type SavedDatabaseConnection =
  | ({ id: string; name: string; db_type: 'postgres' } & PostgresConnectionConfig)
  | ({ id: string; name: string; db_type: 'oracle' } & OracleConnectionConfig)

export type SavedDatabaseConnectionInput =
  | ({ name: string; db_type: 'postgres' } & PostgresConnectionConfig)
  | ({ name: string; db_type: 'oracle' } & OracleConnectionConfig)

export type ScopedDatabaseConnection = SavedDatabaseConnection & { scope: ConnectionScope }

export interface TransformConfig {
  sql: string
}

export interface ExportConfig {
  format: string
}

export interface NodeEditorDraft {
  id: string
  type: NodeType
  position: { x: number; y: number }
  label: string
  autoLabel: string
  labelMode: NodeLabelMode
  tableName: string
  config: Record<string, unknown>
}

export interface NodeExecutionResult {
  node_id: string
  status: NodeStatus
  row_count?: number
  column_count?: number
  columns?: string[]
  error?: string
  execution_time_ms?: number
  started_at?: string
  finished_at?: string
}

export interface ExecutionRunStatus {
  execution_id: string
  kind: 'node' | 'pipeline'
  status: NodeStatus
  started_at: string
  finished_at?: string
  node_results: Record<string, NodeExecutionResult>
}

export interface PipelineDefinition {
  id: string
  name: string
  database_connections: SavedDatabaseConnection[]
  nodes: Array<{
    id: string
    type: NodeType
    table_name: string
    label: string
    auto_label?: string
    label_mode?: NodeLabelMode
    position: { x: number; y: number }
    config: Record<string, unknown>
  }>
  edges: Array<{
    id: string
    source: string
    target: string
  }>
}

export interface ProjectSummary {
  id: string
  name: string
  starred: boolean
  created_at: string
  updated_at: string
}

export interface TablePreviewData {
  kind: 'table'
  columns: string[]
  column_types: string[]
  rows: unknown[][]
  total_rows: number
  offset: number
  limit: number
}

export interface CsvTextPreviewData {
  kind: 'csv_text'
  csv_stage: 'raw' | 'preprocessed'
  rows: string[][]
  limit: number
  truncated: boolean
  artifact_ready: boolean
}

export type DataPreview = TablePreviewData | CsvTextPreviewData

export interface MaterializedPreviewTab {
  nodeId: string
  tableNameAtLoad: string
  data: TablePreviewData | null
  loading: boolean
  error: string | null
  isStale: boolean
}

export interface TransientPreviewState {
  nodeId: string | null
  data: CsvTextPreviewData | null
  loading: boolean
  error: string | null
}

export type ActivePreviewTarget =
  | { kind: 'tab'; nodeId: string }
  | { kind: 'transient'; nodeId: string }
